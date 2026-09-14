use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use rayon::prelude::*;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{
    FAST, Field, IndexRecordOption, STORED, STRING, Schema, TantivyDocument, TextFieldIndexing,
    TextOptions,
};
use tantivy::tokenizer::{
    LowerCaser, NgramTokenizer, RemoveLongFilter, SimpleTokenizer, TextAnalyzer,
};
use tantivy::{DocAddress, Index, IndexReader, ReloadPolicy, Term};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{UnixListener, UnixStream};

const DEFAULT_HOME: &str = "/home/juxtapo/Server_files/tome";
const DEFAULT_MATA_DIR: &str = "/home/juxtapo/Server_files/nudger/mata";
const SOCKET_NAME: &str = "tome.sock";
const SCHEMA_VERSION: u32 = 1;
const DEFAULT_LIMIT: usize = 20;
const DEFAULT_EXT_LIMIT: usize = 15;
const MAX_LIMIT: usize = 100;

#[derive(Parser, Debug)]
#[command(
    name = "tome",
    version,
    about = "Hot local Tantivy search daemon for Mata imports and filesystem indexing"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Import(ImportArgs),
    Index(IndexArgs),
    Daemon(DaemonArgs),
    Search(QueryArgs),
    Find(QueryArgs),
    Status(StatusArgs),
    Recent(RecentArgs),
    Ext(ExtArgs),
}

#[derive(Args, Debug)]
struct ImportArgs {
    #[arg(long, value_enum)]
    source: SourceKind,
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    replace: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct IndexArgs {
    #[arg(long, value_enum, default_value_t = SourceKind::Linux)]
    source: SourceKind,
    #[arg(long = "root")]
    roots: Vec<PathBuf>,
    #[arg(long, conflicts_with = "reindex")]
    incremental: bool,
    #[arg(long)]
    reindex: bool,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct DaemonArgs {
    #[arg(long)]
    socket: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
struct QueryArgs {
    query: String,
    #[arg(long, value_enum, default_value_t = SourceSelection::Linux)]
    source: SourceSelection,
    #[arg(short = 'e', long = "ext")]
    extension: Option<String>,
    #[arg(long, default_value_t = DEFAULT_LIMIT)]
    limit: usize,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    no_plocate: bool,
}

#[derive(Args, Debug, Clone)]
struct StatusArgs {
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct RecentArgs {
    #[arg(long, value_enum, default_value_t = SourceSelection::Linux)]
    source: SourceSelection,
    #[arg(long, default_value_t = DEFAULT_LIMIT)]
    limit: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct ExtArgs {
    #[arg(long, value_enum, default_value_t = SourceSelection::Linux)]
    source: SourceSelection,
    #[arg(long, default_value_t = DEFAULT_EXT_LIMIT)]
    limit: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum SourceKind {
    Linux,
    Windows,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum SourceSelection {
    Linux,
    Windows,
    Both,
}

#[derive(Debug, Clone)]
struct AppPaths {
    index_root: PathBuf,
    meta_root: PathBuf,
    runtime_root: PathBuf,
    socket_path: PathBuf,
}

#[derive(Debug, Clone)]
struct SchemaFields {
    path: Field,
    filename: Field,
    filename_ngram: Field,
    extension: Field,
    content_preview: Field,
    /// Optional: a source indexed before the header field exists (e.g. windows,
    /// reindexed rarely) has no header field. The daemon must still start and search
    /// such sources — header just contributes nothing there. Strict get_field would
    /// hard-fail the whole daemon on a mixed-schema deployment.
    header: Option<Field>,
    source: Field,
    size_bytes: Field,
    modified_at: Field,
    indexed_at: Field,
    file_hash: Field,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceMeta {
    source: SourceKind,
    document_count: u64,
    imported_at: f64,
    #[serde(default)]
    source_db_path: Option<String>,
    #[serde(default)]
    source_db_mtime: Option<f64>,
    #[serde(default)]
    roots: Vec<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    config_path: Option<String>,
    schema_version: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct MataConfig {
    #[serde(default)]
    index_roots: Vec<String>,
    #[serde(default = "default_content_preview_chars")]
    content_preview_chars: usize,
    #[serde(default = "default_max_file_size_mb")]
    max_file_size_mb: u64,
    #[serde(default)]
    skip_dirs: Vec<String>,
    #[serde(default)]
    skip_extensions: Vec<String>,
    #[serde(default)]
    text_extensions: Vec<String>,
}

#[derive(Debug, Clone)]
struct CrawlConfig {
    config_path: PathBuf,
    roots: Vec<PathBuf>,
    content_preview_chars: usize,
    max_file_size_bytes: u64,
    skip_dirs: HashSet<String>,
    skip_extensions: HashSet<String>,
    text_extensions: HashSet<String>,
}

#[derive(Debug, Clone)]
struct IndexedDoc {
    path: String,
    filename: String,
    extension: Option<String>,
    content_preview: Option<String>,
    header: Option<String>,
    size_bytes: Option<u64>,
    modified_at: Option<f64>,
    indexed_at: Option<f64>,
    file_hash: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct CrawlStats {
    total_files: u64,
    total_dirs: u64,
    skipped_files: u64,
    unchanged_files: u64,
    indexed_files: u64,
    added_files: u64,
    updated_files: u64,
    deleted_files: u64,
    errors: u64,
}

#[derive(Debug, Clone)]
struct CrawlOutcome {
    changed_docs: HashMap<String, IndexedDoc>,
    seen_paths: HashSet<String>,
    crawled_roots: Vec<PathBuf>,
    stats: CrawlStats,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum IndexMode {
    Initial,
    Incremental,
    Reindex,
}

#[derive(Debug, Clone)]
struct SearchHit {
    path: String,
    filename: String,
    extension: Option<String>,
    source: SourceKind,
    backend: &'static str,
    snippet: String,
    size_bytes: Option<u64>,
    modified_at: Option<f64>,
    score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchResult {
    path: String,
    filename: String,
    extension: Option<String>,
    source: SourceKind,
    backend: String,
    snippet: String,
    size_bytes: Option<u64>,
    modified_at: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchResponse {
    ok: bool,
    elapsed_ms: u128,
    total: usize,
    results: Vec<SearchResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusResponse {
    ok: bool,
    daemon_reachable: bool,
    socket_path: String,
    uptime_seconds: Option<u64>,
    loaded_sources: Vec<SourceStatus>,
    schema_version: u32,
    degraded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceStatus {
    source: SourceKind,
    index_path: String,
    meta_path: String,
    index_present: bool,
    document_count: Option<u64>,
    imported_at: Option<f64>,
    source_db_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SocketRequest {
    action: String,
    query: Option<String>,
    source: Option<SourceSelection>,
    ext: Option<String>,
    limit: Option<usize>,
    no_plocate: Option<bool>,
}

struct LoadedSource {
    kind: SourceKind,
    reader: IndexReader,
    fields: SchemaFields,
}

struct AppState {
    loaded_at: Instant,
    sources: HashMap<SourceKind, Arc<LoadedSource>>,
    paths: AppPaths,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = AppPaths::from_env();

    match cli.command {
        Commands::Import(args) => cmd_import(&paths, args),
        Commands::Index(args) => cmd_index(&paths, args),
        Commands::Daemon(args) => cmd_daemon(&paths, args).await,
        Commands::Search(args) => cmd_query(&paths, args, QueryMode::Search),
        Commands::Find(args) => cmd_query(&paths, args, QueryMode::Find),
        Commands::Status(args) => cmd_status(&paths, args),
        Commands::Recent(args) => cmd_recent(&paths, args),
        Commands::Ext(args) => cmd_ext(&paths, args),
    }
}

impl AppPaths {
    fn from_env() -> Self {
        let home = std::env::var_os("TOME_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_HOME));
        let index_root = home.join("index");
        let meta_root = home.join("meta");
        let runtime_root = home.join("runtime");
        let socket_path = runtime_root.join(SOCKET_NAME);
        Self {
            index_root,
            meta_root,
            runtime_root,
            socket_path,
        }
    }

    fn index_path(&self, source: SourceKind) -> PathBuf {
        self.index_root.join(source.as_str())
    }

    fn meta_path(&self, source: SourceKind) -> PathBuf {
        self.meta_root.join(format!("{}.json", source.as_str()))
    }
}

impl SourceKind {
    fn as_str(self) -> &'static str {
        match self {
            SourceKind::Linux => "linux",
            SourceKind::Windows => "windows",
        }
    }

    fn default_mata_config(self) -> &'static str {
        match self {
            SourceKind::Linux => "config_linux.json",
            SourceKind::Windows => "config.json",
        }
    }
}

impl SourceSelection {
    fn sources(self) -> &'static [SourceKind] {
        match self {
            SourceSelection::Linux => &[SourceKind::Linux],
            SourceSelection::Windows => &[SourceKind::Windows],
            SourceSelection::Both => &[SourceKind::Linux, SourceKind::Windows],
        }
    }
}

impl SearchHit {
    fn into_result(self) -> SearchResult {
        SearchResult {
            path: self.path,
            filename: self.filename,
            extension: self.extension,
            source: self.source,
            backend: self.backend.to_string(),
            snippet: self.snippet,
            size_bytes: self.size_bytes,
            modified_at: self.modified_at,
        }
    }
}

impl IndexMode {
    fn as_str(self) -> &'static str {
        match self {
            IndexMode::Initial => "initial",
            IndexMode::Incremental => "incremental",
            IndexMode::Reindex => "reindex",
        }
    }
}

impl LoadedSource {
    fn open(paths: &AppPaths, kind: SourceKind) -> Result<Option<Self>> {
        let index_path = paths.index_path(kind);
        if !index_path.exists() {
            return Ok(None);
        }

        let index = Index::open_in_dir(&index_path)
            .with_context(|| format!("open index {}", index_path.display()))?;
        register_tokenizers(&index);
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .context("build index reader")?;
        let fields = schema_fields(index.schema())?;

        Ok(Some(Self {
            kind,
            reader,
            fields,
        }))
    }
}

fn cmd_import(paths: &AppPaths, args: ImportArgs) -> Result<()> {
    fs::create_dir_all(&paths.index_root).context("create index root")?;
    fs::create_dir_all(&paths.meta_root).context("create meta root")?;

    let target_index = paths.index_path(args.source);
    if target_index.exists() && !args.replace {
        bail!(
            "index already exists for {} at {} (use --replace)",
            args.source.as_str(),
            target_index.display()
        );
    }

    let started = Instant::now();
    let db_mtime = file_mtime(&args.db)?;
    let temp_index = temp_path(&target_index);
    if temp_index.exists() {
        fs::remove_dir_all(&temp_index)
            .with_context(|| format!("clear {}", temp_index.display()))?;
    }
    fs::create_dir_all(&temp_index).with_context(|| format!("create {}", temp_index.display()))?;

    let schema = build_schema();
    let index = Index::create_in_dir(&temp_index, schema).with_context(|| {
        format!(
            "create temp index for {} at {}",
            args.source.as_str(),
            temp_index.display()
        )
    })?;
    register_tokenizers(&index);
    let fields = schema_fields(index.schema())?;

    let conn = Connection::open_with_flags(
        &args.db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open sqlite {}", args.db.display()))?;

    validate_source_schema(&conn)?;

    let mut writer = index.writer(200_000_000).context("create index writer")?;
    let mut stmt = conn.prepare(
        "SELECT path, filename, extension, size_bytes, modified_at, indexed_at, content_preview, file_hash FROM files ORDER BY id",
    )?;
    let mut rows = stmt.query([])?;
    let mut imported: u64 = 0;

    while let Some(row) = rows.next()? {
        let path: String = row.get(0)?;
        let filename: String = row.get(1)?;
        let extension: Option<String> = row.get(2)?;
        let size_bytes: Option<u64> = row.get(3)?;
        let modified_at: Option<f64> = row.get(4)?;
        let indexed_at: Option<f64> = row.get(5)?;
        let content_preview: Option<String> = row.get(6)?;
        let file_hash: Option<String> = row.get(7)?;

        let mut doc = TantivyDocument::default();
        doc.add_text(fields.path, &path);
        doc.add_text(fields.filename, &filename);
        doc.add_text(fields.filename_ngram, &filename);
        doc.add_text(fields.source, args.source.as_str());
        if let Some(ext) = extension.as_deref() {
            doc.add_text(fields.extension, ext);
        }
        if let Some(preview) = content_preview.as_deref() {
            doc.add_text(fields.content_preview, preview);
        }
        // header: None on the SQLite import path — mata's DB has no header column, and
        // recomputing it here would re-read every file from disk (the duplicate IO pass
        // the design forbids). Header is populated by the filesystem walk, which already
        // holds the preview bytes.
        if let Some(bytes) = size_bytes {
            doc.add_u64(fields.size_bytes, bytes);
        }
        if let Some(ts) = modified_at {
            doc.add_f64(fields.modified_at, ts);
        }
        if let Some(ts) = indexed_at {
            doc.add_f64(fields.indexed_at, ts);
        }
        if let Some(hash) = file_hash.as_deref() {
            doc.add_text(fields.file_hash, hash);
        }

        writer.add_document(doc)?;
        imported += 1;
    }

    writer.commit().context("commit tantivy index")?;
    drop(writer);

    if target_index.exists() {
        fs::remove_dir_all(&target_index)
            .with_context(|| format!("remove {}", target_index.display()))?;
    }
    fs::rename(&temp_index, &target_index).with_context(|| {
        format!(
            "move {} -> {}",
            temp_index.display(),
            target_index.display()
        )
    })?;
    normalize_tree_permissions(&target_index)?;

    let meta = SourceMeta {
        source: args.source,
        document_count: imported,
        imported_at: now_ts(),
        source_db_path: Some(args.db.display().to_string()),
        source_db_mtime: Some(db_mtime),
        roots: Vec::new(),
        mode: Some("import".to_string()),
        config_path: None,
        schema_version: SCHEMA_VERSION,
    };
    let meta_path = paths.meta_path(args.source);
    write_json_pretty(&meta_path, &meta)?;
    normalize_path_permissions(&meta_path)?;

    let payload = json!({
        "ok": true,
        "source": args.source,
        "db": args.db.display().to_string(),
        "document_count": imported,
        "elapsed_ms": started.elapsed().as_millis(),
        "index_path": target_index.display().to_string(),
    });

    if args.json {
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!(
            "imported {} docs from {} into {} in {} ms",
            imported,
            args.db.display(),
            target_index.display(),
            started.elapsed().as_millis()
        );
    }

    Ok(())
}

fn cmd_index(paths: &AppPaths, args: IndexArgs) -> Result<()> {
    fs::create_dir_all(&paths.index_root).context("create index root")?;
    fs::create_dir_all(&paths.meta_root).context("create meta root")?;

    let target_index = paths.index_path(args.source);
    let mode = if args.reindex {
        IndexMode::Reindex
    } else if args.incremental || target_index.exists() {
        IndexMode::Incremental
    } else {
        IndexMode::Initial
    };
    let crawl = resolve_crawl_config(&args)?;
    let started = Instant::now();

    let existing_hashes = if matches!(mode, IndexMode::Incremental) {
        load_existing_hashes(&target_index)?
    } else {
        HashMap::new()
    };
    let existing_doc_count = existing_hashes.len() as u64;

    let outcome = crawl_filesystem(&crawl, &existing_hashes, mode)?;
    if outcome.crawled_roots.is_empty() {
        bail!("no crawl roots were accessible");
    }

    let deleted_paths = if matches!(mode, IndexMode::Incremental) && outcome.stats.errors == 0 {
        existing_hashes
            .keys()
            .filter(|path| {
                is_under_roots(path, &outcome.crawled_roots) && !outcome.seen_paths.contains(*path)
            })
            .cloned()
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };

    let temp_index = temp_path(&target_index);
    if temp_index.exists() {
        fs::remove_dir_all(&temp_index)
            .with_context(|| format!("clear {}", temp_index.display()))?;
    }
    fs::create_dir_all(&temp_index).with_context(|| format!("create {}", temp_index.display()))?;

    let schema = build_schema();
    let index = Index::create_in_dir(&temp_index, schema)
        .with_context(|| format!("create temp index {}", temp_index.display()))?;
    register_tokenizers(&index);
    let fields = schema_fields(index.schema())?;
    let mut writer = index.writer(200_000_000).context("create index writer")?;

    let carried_count = if matches!(mode, IndexMode::Incremental) {
        copy_existing_docs(
            &target_index,
            &mut writer,
            &fields,
            args.source,
            &outcome.changed_docs,
            &deleted_paths,
        )?
    } else {
        0
    };

    for doc in outcome.changed_docs.values() {
        add_indexed_doc(&mut writer, &fields, args.source, doc)?;
    }

    writer.commit().context("commit tantivy index")?;
    drop(writer);

    if target_index.exists() {
        fs::remove_dir_all(&target_index)
            .with_context(|| format!("remove {}", target_index.display()))?;
    }
    fs::rename(&temp_index, &target_index).with_context(|| {
        format!(
            "move {} -> {}",
            temp_index.display(),
            target_index.display()
        )
    })?;
    normalize_tree_permissions(&target_index)?;

    let final_count = carried_count + outcome.changed_docs.len() as u64;
    let mut stats = outcome.stats;
    if matches!(mode, IndexMode::Incremental) && stats.errors == 0 {
        stats.deleted_files = deleted_paths.len() as u64;
    }

    let meta = SourceMeta {
        source: args.source,
        document_count: final_count,
        imported_at: now_ts(),
        source_db_path: None,
        source_db_mtime: None,
        roots: crawl
            .roots
            .iter()
            .map(|root| root.display().to_string())
            .collect(),
        mode: Some(mode.as_str().to_string()),
        config_path: Some(crawl.config_path.display().to_string()),
        schema_version: SCHEMA_VERSION,
    };
    let meta_path = paths.meta_path(args.source);
    write_json_pretty(&meta_path, &meta)?;
    normalize_path_permissions(&meta_path)?;

    let payload = json!({
        "ok": true,
        "source": args.source,
        "mode": mode.as_str(),
        "document_count": final_count,
        "existing_document_count": existing_doc_count,
        "indexed_files": stats.indexed_files,
        "added_files": stats.added_files,
        "updated_files": stats.updated_files,
        "unchanged_files": stats.unchanged_files,
        "deleted_files": stats.deleted_files,
        "skipped_files": stats.skipped_files,
        "errors": stats.errors,
        "roots": meta.roots,
        "deletions_skipped": matches!(mode, IndexMode::Incremental) && stats.errors > 0,
        "elapsed_ms": started.elapsed().as_millis(),
        "index_path": target_index.display().to_string(),
        "config_path": crawl.config_path.display().to_string(),
    });

    if args.json {
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!(
            "indexed {} docs for {} in {} ms ({}, +{}, ~{}, ={}, -{}, skipped {}, errors {})",
            final_count,
            args.source.as_str(),
            started.elapsed().as_millis(),
            mode.as_str(),
            stats.added_files,
            stats.updated_files,
            stats.unchanged_files,
            stats.deleted_files,
            stats.skipped_files,
            stats.errors,
        );
        if matches!(mode, IndexMode::Incremental) && stats.errors > 0 {
            println!("deletions were skipped because the crawl had errors");
        }
    }

    Ok(())
}

fn resolve_crawl_config(args: &IndexArgs) -> Result<CrawlConfig> {
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_MATA_DIR).join(args.source.default_mata_config()));
    let config = load_mata_config(&config_path)?;
    let roots = if args.roots.is_empty() {
        config
            .index_roots
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>()
    } else {
        args.roots.clone()
    };
    let roots = normalize_roots(&roots);
    if roots.is_empty() {
        bail!("no roots configured for indexing");
    }

    Ok(CrawlConfig {
        config_path,
        roots,
        content_preview_chars: config.content_preview_chars,
        max_file_size_bytes: config.max_file_size_mb.saturating_mul(1024 * 1024),
        skip_dirs: config
            .skip_dirs
            .into_iter()
            .map(|name| name.to_lowercase())
            .collect(),
        skip_extensions: config
            .skip_extensions
            .into_iter()
            .map(|ext| normalize_extension(&ext))
            .collect(),
        text_extensions: config
            .text_extensions
            .into_iter()
            .map(|ext| normalize_extension(&ext))
            .collect(),
    })
}

fn load_mata_config(path: &Path) -> Result<MataConfig> {
    let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&data).with_context(|| format!("parse {}", path.display()))
}

fn crawl_filesystem(
    config: &CrawlConfig,
    existing_hashes: &HashMap<String, String>,
    mode: IndexMode,
) -> Result<CrawlOutcome> {
    let mut changed_docs = HashMap::new();
    let mut seen_paths = HashSet::new();
    let mut crawled_roots = Vec::new();
    let mut stats = CrawlStats::default();

    for root in &config.roots {
        if !root.exists() {
            eprintln!("warning: root does not exist: {}", root.display());
            stats.errors += 1;
            continue;
        }
        if fs::read_dir(root).is_err() {
            eprintln!("warning: cannot read root: {}", root.display());
            stats.errors += 1;
            continue;
        }

        crawled_roots.push(root.clone());
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            stats.total_dirs += 1;
            let entries = match fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(_) => {
                    stats.errors += 1;
                    continue;
                }
            };

            for entry_result in entries {
                let entry = match entry_result {
                    Ok(entry) => entry,
                    Err(_) => {
                        stats.errors += 1;
                        continue;
                    }
                };
                let file_type = match entry.file_type() {
                    Ok(file_type) => file_type,
                    Err(_) => {
                        stats.errors += 1;
                        continue;
                    }
                };
                if file_type.is_symlink() {
                    continue;
                }

                let path = entry.path();
                if file_type.is_dir() {
                    let dir_name = path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default()
                        .to_lowercase();
                    if !config.skip_dirs.contains(&dir_name) {
                        stack.push(path);
                    }
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }

                stats.total_files += 1;
                match inspect_file(&path, config, existing_hashes, mode) {
                    Ok(FileAction::Skip) => {
                        stats.skipped_files += 1;
                    }
                    Ok(FileAction::Unchanged(path_key)) => {
                        stats.unchanged_files += 1;
                        seen_paths.insert(path_key);
                    }
                    Ok(FileAction::Index {
                        path_key,
                        doc,
                        existed,
                    }) => {
                        if existed {
                            stats.updated_files += 1;
                        } else {
                            stats.added_files += 1;
                        }
                        stats.indexed_files += 1;
                        seen_paths.insert(path_key.clone());
                        changed_docs.insert(path_key, doc);
                    }
                    Err(_) => {
                        stats.errors += 1;
                    }
                }
            }
        }
    }

    Ok(CrawlOutcome {
        changed_docs,
        seen_paths,
        crawled_roots,
        stats,
    })
}

fn load_existing_hashes(index_path: &Path) -> Result<HashMap<String, String>> {
    if !index_path.exists() {
        return Ok(HashMap::new());
    }

    let index = Index::open_in_dir(index_path)
        .with_context(|| format!("open index {}", index_path.display()))?;
    let fields = schema_fields(index.schema())?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .context("build index reader")?;
    let searcher = reader.searcher();
    let mut hashes = HashMap::new();

    for segment_reader in searcher.segment_readers() {
        let store = segment_reader
            .get_store_reader(10)
            .context("open segment store reader")?;
        for doc_result in store.iter::<TantivyDocument>(segment_reader.alive_bitset()) {
            let doc = doc_result.context("read stored document")?;
            if let Some(path) = first_text(&doc, fields.path) {
                let hash = first_text(&doc, fields.file_hash)
                    .map(|value| value.into_owned())
                    .unwrap_or_default();
                hashes.insert(path.into_owned(), hash);
            }
        }
    }

    Ok(hashes)
}

fn copy_existing_docs(
    index_path: &Path,
    writer: &mut tantivy::IndexWriter,
    target_fields: &SchemaFields,
    source: SourceKind,
    changed_docs: &HashMap<String, IndexedDoc>,
    deleted_paths: &HashSet<String>,
) -> Result<u64> {
    if !index_path.exists() {
        return Ok(0);
    }

    let index = Index::open_in_dir(index_path)
        .with_context(|| format!("open index {}", index_path.display()))?;
    let source_fields = schema_fields(index.schema())?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .context("build index reader")?;
    let searcher = reader.searcher();
    let mut kept = 0;

    for segment_reader in searcher.segment_readers() {
        let store = segment_reader
            .get_store_reader(10)
            .context("open segment store reader")?;
        for doc_result in store.iter::<TantivyDocument>(segment_reader.alive_bitset()) {
            let doc = doc_result.context("read stored document")?;
            let indexed = indexed_doc_from_tantivy(&doc, &source_fields)?;
            if changed_docs.contains_key(&indexed.path) || deleted_paths.contains(&indexed.path) {
                continue;
            }
            add_indexed_doc(writer, target_fields, source, &indexed)?;
            kept += 1;
        }
    }

    Ok(kept)
}

fn add_indexed_doc(
    writer: &mut tantivy::IndexWriter,
    fields: &SchemaFields,
    source: SourceKind,
    indexed: &IndexedDoc,
) -> Result<()> {
    let mut doc = TantivyDocument::default();
    doc.add_text(fields.path, &indexed.path);
    doc.add_text(fields.filename, &indexed.filename);
    doc.add_text(fields.filename_ngram, &indexed.filename);
    doc.add_text(fields.source, source.as_str());
    if let Some(ext) = indexed.extension.as_deref() {
        doc.add_text(fields.extension, ext);
    }
    if let Some(preview) = indexed.content_preview.as_deref() {
        doc.add_text(fields.content_preview, preview);
    }
    if let (Some(field), Some(header)) = (fields.header, indexed.header.as_deref()) {
        doc.add_text(field, header);
    }
    if let Some(bytes) = indexed.size_bytes {
        doc.add_u64(fields.size_bytes, bytes);
    }
    if let Some(ts) = indexed.modified_at {
        doc.add_f64(fields.modified_at, ts);
    }
    if let Some(ts) = indexed.indexed_at {
        doc.add_f64(fields.indexed_at, ts);
    }
    if let Some(hash) = indexed.file_hash.as_deref() {
        doc.add_text(fields.file_hash, hash);
    }
    writer.add_document(doc)?;
    Ok(())
}

fn indexed_doc_from_tantivy(doc: &TantivyDocument, fields: &SchemaFields) -> Result<IndexedDoc> {
    let path = first_text(doc, fields.path)
        .map(|value| value.into_owned())
        .filter(|value| !value.is_empty())
        .context("document missing path")?;
    let filename = first_text(doc, fields.filename)
        .map(|value| value.into_owned())
        .unwrap_or_else(|| {
            Path::new(&path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(&path)
                .to_string()
        });
    Ok(IndexedDoc {
        path,
        filename,
        extension: first_text(doc, fields.extension).map(|value| value.into_owned()),
        content_preview: first_text(doc, fields.content_preview).map(|value| value.into_owned()),
        header: fields
            .header
            .and_then(|field| first_text(doc, field).map(|value| value.into_owned())),
        size_bytes: first_u64(doc, fields.size_bytes),
        modified_at: first_f64(doc, fields.modified_at),
        indexed_at: first_f64(doc, fields.indexed_at),
        file_hash: first_text(doc, fields.file_hash).map(|value| value.into_owned()),
    })
}

enum FileAction {
    Skip,
    Unchanged(String),
    Index {
        path_key: String,
        doc: IndexedDoc,
        existed: bool,
    },
}

fn inspect_file(
    path: &Path,
    config: &CrawlConfig,
    existing_hashes: &HashMap<String, String>,
    mode: IndexMode,
) -> Result<FileAction> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if should_skip_file(path, &metadata, config) {
        return Ok(FileAction::Skip);
    }

    let path_key = path.display().to_string();
    let existed = existing_hashes.contains_key(&path_key);
    let modified_at = file_mtime(path)?;
    let file_hash = quick_file_hash(metadata.len(), modified_at);

    if matches!(mode, IndexMode::Incremental)
        && existing_hashes
            .get(&path_key)
            .is_some_and(|hash| hash == &file_hash)
    {
        return Ok(FileAction::Unchanged(path_key));
    }

    let content_preview = if is_text_file(path, config) {
        read_content_preview(path, config.content_preview_chars)
    } else {
        None
    };
    // header is distilled from the SAME bytes read_content_preview already pulled —
    // zero extra IO. The extension is looked up once and shared with the doc below.
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(normalize_extension);
    let header = content_preview
        .as_deref()
        .and_then(|preview| extract_header(preview, extension.as_deref()));
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&path_key)
        .to_string();

    Ok(FileAction::Index {
        path_key: path_key.clone(),
        doc: IndexedDoc {
            path: path_key,
            filename,
            extension,
            content_preview,
            header,
            size_bytes: Some(metadata.len()),
            modified_at: Some(modified_at),
            indexed_at: Some(now_ts()),
            file_hash: Some(file_hash),
        },
        existed,
    })
}

fn should_skip_file(path: &Path, metadata: &fs::Metadata, config: &CrawlConfig) -> bool {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(normalize_extension)
        .is_some_and(|ext| config.skip_extensions.contains(&ext))
    {
        return true;
    }
    metadata.len() > config.max_file_size_bytes
}

fn is_text_file(path: &Path, config: &CrawlConfig) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(normalize_extension)
        .is_some_and(|ext| config.text_extensions.contains(&ext))
}

fn read_content_preview(path: &Path, max_chars: usize) -> Option<String> {
    let byte_limit = (max_chars.saturating_mul(4)).max(max_chars);
    let file = fs::File::open(path).ok()?;
    let mut bytes = Vec::new();
    file.take(byte_limit as u64).read_to_end(&mut bytes).ok()?;
    let preview = String::from_utf8_lossy(&bytes);
    let clipped = preview.chars().take(max_chars).collect::<String>();
    if clipped.is_empty() {
        None
    } else {
        Some(clipped)
    }
}

// ─── header extraction ────────────────────────────────────────────────────────
// Distills a file's "meaning" from its top comment block / frontmatter. The hard
// part is NOT the comment grammar — it is noise. Measured corpus-wide, ~69% of
// naive top-blocks are license/autogen boilerplate, which at boost 3.0 would make
// thousands of Apache/MIT blocks strong mutual matches and swamp real results.
// So this is extractor + noise filter, and the filter is the point.

/// Longest run of "meaningful" header lines we keep. Median useful header is
/// ~149 chars / a handful of lines; a long unbroken block is itself a noise signal.
const HEADER_MAX_LINES: usize = 12;
/// Hard char cap, applied AFTER filtering (a cap alone can't tell license from prose).
const HEADER_MAX_CHARS: usize = 800;

fn extract_header(preview: &str, extension: Option<&str>) -> Option<String> {
    // normalize_extension keeps the leading dot (".py"); the language families below
    // are dotless. Strip it once here rather than change the codebase-wide convention.
    let ext = extension.unwrap_or("").trim_start_matches('.');
    let mut out: Vec<String> = Vec::new();
    let mut in_block = false; // inside /* ... */ or <!-- ... -->
    let mut in_frontmatter = false; // inside --- ... --- (yaml md frontmatter)
    let mut in_docstring = false; // inside a python/julia """...""" module docstring
    let mut seen_any = false;

    for raw in preview.lines() {
        if out.len() >= HEADER_MAX_LINES {
            break;
        }
        let line = raw.trim();

        // ── inside a triple-quoted docstring: capture until the closing fence ──
        if in_docstring {
            if let Some(end) = line.find("\"\"\"") {
                let body = line[..end].trim();
                if !body.is_empty() {
                    push_header_line(&mut out, body);
                }
                // a docstring closes the header region — code follows
                break;
            }
            if !line.is_empty() {
                push_header_line(&mut out, line);
            }
            continue;
        }

        // ── inside frontmatter: yaml key:value IS the content ─────────────────
        if in_frontmatter {
            if line == "---" || line == "..." {
                break; // end of frontmatter
            }
            let body = line.trim_start_matches('#').trim();
            if !body.is_empty() {
                push_header_line(&mut out, body);
            }
            continue;
        }

        // ── inside /* */ or <!-- --> block comment ────────────────────────────
        if in_block {
            let (body, closes) = if let Some(end) = line.find("*/") {
                (&line[..end], true)
            } else if let Some(end) = line.find("-->") {
                (&line[..end], true)
            } else {
                (line, false)
            };
            let body = body.trim_start_matches('*').trim();
            if !body.is_empty() {
                push_header_line(&mut out, body);
            }
            if closes {
                in_block = false;
            }
            continue;
        }

        // ── fence-skip: preprocessor / pragma / code fences are not meaning ───
        if is_fence_line(line, ext) {
            continue;
        }

        // ── structural skips: shebang, modelines, encoding cookies ────────────
        if !seen_any && (line.starts_with("#!") || line.starts_with("# -*-")) {
            continue;
        }
        if is_modeline(line) {
            continue;
        }

        // ── frontmatter open fence (md only, first meaningful line) ───────────
        if !seen_any && line == "---" && matches!(ext, "md" | "markdown" | "mdx") {
            in_frontmatter = true;
            seen_any = true;
            continue;
        }

        // ── docstring open (py/jl): a bare """ opens a module docstring ───────
        if matches!(ext, "py" | "jl") && line.starts_with("\"\"\"") {
            seen_any = true;
            let rest = &line[3..];
            if let Some(end) = rest.find("\"\"\"") {
                // single-line docstring: """..."""
                let body = rest[..end].trim();
                if !body.is_empty() {
                    push_header_line(&mut out, body);
                }
                break; // docstring done, code follows
            }
            let rest = rest.trim();
            if !rest.is_empty() {
                push_header_line(&mut out, rest);
            }
            in_docstring = true;
            continue;
        }

        // ── block comment open ────────────────────────────────────────────────
        if line.starts_with("/*") || line.starts_with("<!--") {
            seen_any = true;
            let (open, close) = if line.starts_with("/*") {
                ("/*", "*/")
            } else {
                ("<!--", "-->")
            };
            let rest = line.trim_start_matches(open);
            if let Some(end) = rest.find(close) {
                let body = rest[..end].trim_start_matches('*').trim();
                if !body.is_empty() {
                    push_header_line(&mut out, body);
                }
                continue; // one-line block comment; keep reading
            }
            let rest = rest.trim_start_matches('*').trim();
            if !rest.is_empty() {
                push_header_line(&mut out, rest);
            }
            in_block = true;
            continue;
        }

        // ── line comment by extension family ─────────────────────────────────
        match line_comment_body(line, extension) {
            Some(body) => {
                let body = body.trim();
                seen_any = true;
                if body.is_empty() {
                    // blank comment line: tolerate only mid-block
                    if out.is_empty() {
                        continue;
                    }
                    break;
                }
                push_header_line(&mut out, body);
            }
            None => {
                // not a comment. Before any header collected, skip LEADING structural
                // code that carries no meaning and routinely precedes the real comment:
                // blank lines, shell `set`/`export`, braces. Only MEANINGFUL code ends
                // the hunt — this is what lets `#pragma once\n\n// real comment` and
                // `set -uo pipefail\n# NOTE: ...` both yield their headers.
                if out.is_empty() && is_leading_structural_line(line, ext) {
                    continue;
                }
                if matches!(ext, "md" | "txt" | "markdown") && !line.is_empty() {
                    push_header_line(&mut out, line);
                }
                // meaningful code/prose ends the header region
                break;
            }
        }
    }

    let joined = out.join("\n");
    let filtered = filter_header_noise(&joined);
    if filtered.is_empty() {
        return None;
    }
    let capped: String = filtered.chars().take(HEADER_MAX_CHARS).collect();
    let capped = capped.trim();
    if capped.is_empty() {
        None
    } else {
        Some(capped.to_string())
    }
}

/// Preprocessor/pragma/code-fence lines that open many C-family files but carry
/// no meaning: `#pragma once`, `#if !defined(GUARD)`, `#define GUARD`, `#ifdef`,
/// `#include`, and bare md code fences (```). Skipped WITHOUT consuming the
/// header run — the real comment usually follows them.
fn is_fence_line(line: &str, ext: &str) -> bool {
    if line.starts_with("```") {
        return true;
    }
    if !matches!(ext, "c" | "h" | "cpp" | "hpp" | "cc" | "cxx" | "rs") {
        return false;
    }
    let b = line.as_bytes();
    if b.first() != Some(&b'#') {
        return false;
    }
    // #if / #ifdef / #ifndef / #define / #pragma / #include / #endif / #else
    let directives = [
        "#if", "#ifdef", "#ifndef", "#define", "#pragma", "#include", "#endif", "#else", "#elif",
        "#undef",
    ];
    directives.iter().any(|d| line.starts_with(d))
}

/// Strips the comment marker for line-comment languages, by extension family.
/// Returns None for lines that aren't comments (caller ends the header region).
fn line_comment_body<'a>(line: &'a str, extension: Option<&str>) -> Option<&'a str> {
    let hash_langs = [
        "py", "sh", "bash", "rb", "pl", "yaml", "yml", "toml", "r", "jl", "conf", "ini", "cfg",
        "md", "txt", "nix",
    ];
    let slash_langs = [
        "rs", "c", "h", "cpp", "hpp", "cc", "js", "jsx", "ts", "tsx", "go", "java", "kt", "swift",
        "cs", "php", "scala", "dart", "zig",
    ];
    let dash_langs = ["sql", "lua", "hs", "ada"];
    let semi_langs = ["lisp", "clj", "scm", "asm"];

    let ext = extension.unwrap_or("").trim_start_matches('.');
    if line.starts_with('#') && (hash_langs.contains(&ext) || ext.is_empty()) {
        return Some(line.trim_start_matches('#'));
    }
    if line.starts_with("//") && slash_langs.contains(&ext) {
        // `///` and `//!`-style doc comments: drop the extra marker char so the
        // header text doesn't start with a stray '/' or '!'
        let body = line.trim_start_matches("//");
        return Some(body.trim_start_matches('/').trim_start_matches('!'));
    }
    if line.starts_with("--") && dash_langs.contains(&ext) {
        return Some(line.trim_start_matches("--"));
    }
    if line.starts_with(';') && semi_langs.contains(&ext) {
        return Some(line.trim_start_matches(';'));
    }
    // md/txt have no comment marker — a bare prose first line is the header
    if matches!(ext, "md" | "txt" | "markdown") && !line.is_empty() {
        return Some(line);
    }
    None
}

fn is_modeline(line: &str) -> bool {
    (line.contains("vim:") || line.contains("vi:") || line.contains("ex:")) && line.len() < 80
        || line.contains("-*-")
}

/// Leading code that carries no meaning and is routinely skipped past to reach the
/// real top comment: blank lines, shell option/env setup, lone braces, `use`/`import`
/// that open many files. Only consulted BEFORE any header line is collected — once a
/// header starts, any of these ends it.
fn is_leading_structural_line(line: &str, ext: &str) -> bool {
    if line.is_empty() {
        return true;
    }
    let l = line.trim();
    if matches!(ext, "sh" | "bash" | "zsh" | "fish") {
        return l.starts_with("set ")
            || l.starts_with("set\t")
            || l.starts_with("export ")
            || l.starts_with("source ")
            || l.starts_with('.')
            || l.starts_with("shopt ")
            || l == "}"
            || l == "{"
            || l.starts_with('}');
    }
    if matches!(ext, "py") {
        // `from __future__`, `import` — but a docstring/comment usually comes first.
        // We do NOT skip imports here: a py file's meaning is its docstring/first comment,
        // and skipping imports would swallow files that only have a mid-file comment.
        return false;
    }
    if matches!(
        ext,
        "rs" | "go" | "java" | "kt" | "scala" | "ts" | "js" | "tsx" | "jsx"
    ) {
        // `use`/`import`/`package` declarations open these files; the module doc comment
        // (/// or /*!) usually follows them. Skipping lets the doc comment be found.
        return l.starts_with("use ")
            || l.starts_with("use\t")
            || l.starts_with("import ")
            || l.starts_with("package ")
            || l.starts_with('}') && l.ends_with('{'); // grouped use `}` close + open
    }
    // lone braces / parens for C-family
    l == "{" || l == "}" || l == "};"
}

fn push_header_line(out: &mut Vec<String>, body: &str) {
    let body = body.trim();
    if !body.is_empty() {
        out.push(body.to_string());
    }
}

/// The filter IS the deliverable. Drops license/autogen/trivial blocks. Returns
/// the surviving text (possibly trimmed of a licensey prefix), or empty if the
/// whole block is noise.
fn filter_header_noise(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    let lower = text.to_lowercase();

    // ── whole-block kills: unambiguous boilerplate ─────────────────────────────
    let whole_block_noise = [
        "do not edit",
        "@generated",
        "code generated by",
        "auto-generated",
        "autogenerated",
        "generated by",
        "this file was generated",
        "apache license",
        "mit license",
        "gnu general public license",
        "gnu lesser general public license",
        "bsd license",
        "mozilla public license",
        "spdx-license-identifier",
        "licensed under the apache license",
        "permission is hereby granted, free of charge",
        "redistribution and use in source and binary forms",
        "this software is provided 'as-is'",
        "this source code is licensed under",
    ];
    for pat in whole_block_noise {
        if lower.contains(pat) {
            return String::new();
        }
    }

    // ── line-level: drop copyright/license lines, keep a real description ──────
    let noise_line = |l: &str| {
        let ll = l.to_lowercase();
        ll.starts_with("copyright")
            || ll.starts_with("(c)")
            || ll.starts_with("©")
            || ll.contains("copyright (c)")
            || ll.starts_with("license")
            || ll.starts_with("licensed")
            || ll.starts_with("author:")
            || ll.starts_with("authors:")
            || ll.starts_with("maintainer:")
            || ll.starts_with("spdx-")
    };

    let kept: Vec<&str> = text
        .lines()
        .filter(|l| !noise_line(l))
        .filter(|l| !is_trivial_line(l))
        .collect();

    kept.join("\n").trim().to_string()
}

/// A line so short/structural it carries no meaning: braces, separators, lone
/// tokens like "}", "---", "#", single-word punctuation.
fn is_trivial_line(line: &str) -> bool {
    let l = line.trim();
    if l.is_empty() {
        return true;
    }
    if l.chars()
        .all(|c| c == '-' || c == '=' || c == '*' || c == '#' || c == '/')
    {
        return true; // separator run
    }
    l.len() <= 2 && l.chars().all(|c| !c.is_alphanumeric())
}

fn quick_file_hash(size_bytes: u64, modified_at: f64) -> String {
    format!("{:x}", md5::compute(format!("{size_bytes}:{modified_at}")))
}

fn normalize_roots(raw_roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for root in raw_roots {
        let normalized = fs::canonicalize(root).unwrap_or_else(|_| root.clone());
        if !roots.contains(&normalized) {
            roots.push(normalized);
        }
    }
    roots
}

fn is_under_roots(path: &str, roots: &[PathBuf]) -> bool {
    let candidate = Path::new(path);
    roots.iter().any(|root| candidate.starts_with(root))
}

fn default_content_preview_chars() -> usize {
    2000
}

fn default_max_file_size_mb() -> u64 {
    50
}

async fn cmd_daemon(paths: &AppPaths, args: DaemonArgs) -> Result<()> {
    fs::create_dir_all(&paths.runtime_root).context("create runtime root")?;
    let socket_path = args.socket.unwrap_or_else(|| paths.socket_path.clone());
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    remove_stale_socket(&socket_path)?;

    let state = Arc::new(AppState {
        loaded_at: Instant::now(),
        sources: load_sources(paths)?,
        paths: paths.clone(),
    });
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind socket {}", socket_path.display()))?;

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            incoming = listener.accept() => {
                let (stream, _) = incoming.context("accept socket connection")?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(err) = handle_client(stream, state).await {
                        let _ = err;
                    }
                });
            }
        }
    }

    if socket_path.exists() {
        let _ = fs::remove_file(&socket_path);
    }

    Ok(())
}

fn cmd_query(paths: &AppPaths, args: QueryArgs, mode: QueryMode) -> Result<()> {
    let started = Instant::now();
    let response = match request_via_socket(
        &paths.socket_path,
        SocketRequest {
            action: mode.as_action().to_string(),
            query: Some(args.query.clone()),
            source: Some(args.source),
            ext: args.extension.clone(),
            limit: Some(args.limit),
            no_plocate: Some(args.no_plocate),
        },
    ) {
        Ok(payload) => serde_json::from_value::<SearchResponse>(payload)
            .context("parse socket search response")?,
        Err(_) => run_query_local(paths, &args, mode, started)?,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        print_human_results(&args.query, &response);
    }

    Ok(())
}

fn cmd_status(paths: &AppPaths, args: StatusArgs) -> Result<()> {
    let response = match request_via_socket(
        &paths.socket_path,
        SocketRequest {
            action: "status".to_string(),
            query: None,
            source: None,
            ext: None,
            limit: None,
            no_plocate: None,
        },
    ) {
        Ok(payload) => serde_json::from_value::<StatusResponse>(payload)
            .context("parse socket status response")?,
        Err(_) => build_local_status(paths, false)?,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        print_human_status(&response);
    }

    Ok(())
}

fn cmd_recent(paths: &AppPaths, args: RecentArgs) -> Result<()> {
    let started = Instant::now();
    let sources = load_sources(paths)?;
    let selected: Vec<Arc<LoadedSource>> = args
        .source
        .sources()
        .iter()
        .filter_map(|source| sources.get(source).cloned())
        .collect();

    if selected.is_empty() {
        bail!("no indexes available for requested source");
    }

    let limit = args.limit.clamp(1, MAX_LIMIT);
    let mut hits: Vec<SearchHit> = Vec::new();
    for source in &selected {
        hits.extend(recent_one_source(source, limit)?);
    }
    hits.sort_by(|a, b| {
        b.modified_at
            .partial_cmp(&a.modified_at)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.path.cmp(&b.path))
    });
    hits.truncate(limit);

    let response = SearchResponse {
        ok: true,
        elapsed_ms: started.elapsed().as_millis(),
        total: hits.len(),
        results: hits.into_iter().map(SearchHit::into_result).collect(),
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        print_human_results("recent", &response);
    }

    Ok(())
}

fn recent_one_source(source: &LoadedSource, limit: usize) -> Result<Vec<SearchHit>> {
    let searcher = source.reader.searcher();
    let collector =
        TopDocs::with_limit(limit).order_by_fast_field::<f64>("modified_at", tantivy::Order::Desc);
    let docs = searcher
        .search(&AllQuery, &collector)
        .context("collect recent documents")?;

    docs.into_iter()
        .map(|(modified_at, address)| doc_to_hit(source, &searcher, address, modified_at as f32))
        .collect()
}

fn cmd_ext(paths: &AppPaths, args: ExtArgs) -> Result<()> {
    let sources = load_sources(paths)?;
    let selected: Vec<Arc<LoadedSource>> = args
        .source
        .sources()
        .iter()
        .filter_map(|source| sources.get(source).cloned())
        .collect();

    if selected.is_empty() {
        bail!("no indexes available for requested source");
    }

    let limit = args.limit.clamp(1, MAX_LIMIT);
    let mut counts: HashMap<String, u64> = HashMap::new();
    for source in &selected {
        accumulate_extension_counts(source, &mut counts)?;
    }

    let mut ranked: Vec<(String, u64)> = counts.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked.truncate(limit);

    if args.json {
        let payload: Vec<_> = ranked
            .iter()
            .map(|(extension, count)| json!({ "extension": extension, "count": count }))
            .collect();
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!("{:<8} {:>6}", "Ext", "Count");
        println!("{:<8} {:>6}", "---", "---");
        for (extension, count) in &ranked {
            println!("{extension:<8} {count:>6}");
        }
    }

    Ok(())
}

// Summed `doc_freq` may include not-yet-merged deletes; acceptable for stats.
fn accumulate_extension_counts(
    source: &LoadedSource,
    counts: &mut HashMap<String, u64>,
) -> Result<()> {
    let searcher = source.reader.searcher();
    for segment_reader in searcher.segment_readers() {
        let inverted = segment_reader
            .inverted_index(source.fields.extension)
            .context("open extension inverted index")?;
        let mut stream = inverted
            .terms()
            .stream()
            .context("stream extension terms")?;
        while stream.advance() {
            let extension = String::from_utf8_lossy(stream.key()).into_owned();
            let doc_freq = u64::from(stream.value().doc_freq);
            *counts.entry(extension).or_insert(0) += doc_freq;
        }
    }
    Ok(())
}

#[derive(Copy, Clone)]
enum QueryMode {
    Search,
    Find,
}

impl QueryMode {
    fn as_action(self) -> &'static str {
        match self {
            QueryMode::Search => "search",
            QueryMode::Find => "find",
        }
    }
}

fn run_query_local(
    paths: &AppPaths,
    args: &QueryArgs,
    mode: QueryMode,
    started: Instant,
) -> Result<SearchResponse> {
    let sources = load_sources(paths)?;
    let hits = search_sources(&sources, args, mode)?;
    Ok(SearchResponse {
        ok: true,
        elapsed_ms: started.elapsed().as_millis(),
        total: hits.len(),
        results: hits.into_iter().map(SearchHit::into_result).collect(),
    })
}

fn search_sources(
    sources: &HashMap<SourceKind, Arc<LoadedSource>>,
    args: &QueryArgs,
    mode: QueryMode,
) -> Result<Vec<SearchHit>> {
    let limit = args.limit.clamp(1, MAX_LIMIT);
    let selected: Vec<Arc<LoadedSource>> = args
        .source
        .sources()
        .iter()
        .filter_map(|source| sources.get(source).cloned())
        .collect();

    if selected.is_empty() {
        bail!("no indexes available for requested source");
    }

    let mut hits: Vec<SearchHit> = selected
        .par_iter()
        .map(|source| search_one_source(source, args, mode, limit.saturating_mul(2)))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();

    if !args.no_plocate && matches!(args.source, SourceSelection::Linux | SourceSelection::Both) {
        hits.extend(plocate_hits(&args.query, limit.saturating_mul(2)));
    }

    Ok(merge_hits(hits, limit))
}

fn search_one_source(
    source: &LoadedSource,
    args: &QueryArgs,
    mode: QueryMode,
    limit: usize,
) -> Result<Vec<SearchHit>> {
    let searcher = source.reader.searcher();
    let query = build_query(source, args, mode)?;
    let docs = searcher
        .search(&query, &TopDocs::with_limit(limit))
        .context("execute tantivy search")?;

    docs.into_iter()
        .map(|(score, address)| doc_to_hit(source, &searcher, address, score))
        .collect()
}

fn build_query(source: &LoadedSource, args: &QueryArgs, mode: QueryMode) -> Result<Box<dyn Query>> {
    let text_query: Box<dyn Query> = match mode {
        QueryMode::Search => build_text_search_query(source, &args.query)?,
        QueryMode::Find => build_filename_query(source, &args.query)?,
    };

    let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, text_query)];
    if let Some(ext) = args.extension.as_deref() {
        let normalized = normalize_extension(ext);
        let term = Term::from_field_text(source.fields.extension, &normalized);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if clauses.len() == 1 {
        return Ok(clauses.remove(0).1);
    }

    Ok(Box::new(BooleanQuery::new(clauses)))
}

fn build_text_search_query(source: &LoadedSource, raw: &str) -> Result<Box<dyn Query>> {
    if raw.trim().is_empty() {
        bail!("empty query");
    }
    let index = source.reader.searcher().index().clone();
    // header only exists on sources indexed after the field was added; include it
    // (and its boost) only where present so mixed-schema sources still search.
    let mut fields = vec![
        source.fields.filename,
        source.fields.path,
        source.fields.content_preview,
    ];
    if let Some(header) = source.fields.header {
        fields.push(header);
    }
    let mut parser = QueryParser::for_index(&index, fields);
    parser.set_field_boost(source.fields.filename, 4.0);
    if let Some(header) = source.fields.header {
        parser.set_field_boost(header, 3.0);
    }
    parser.set_field_boost(source.fields.path, 2.5);
    parser.set_field_boost(source.fields.content_preview, 1.0);
    let (query, _errors) = parser.parse_query_lenient(raw);
    Ok(query)
}

fn build_filename_query(source: &LoadedSource, raw: &str) -> Result<Box<dyn Query>> {
    let query = raw.trim().to_lowercase();
    if query.is_empty() {
        bail!("empty query");
    }

    let grams = grams_for_query(&query);
    if grams.is_empty() {
        return Ok(Box::new(AllQuery));
    }

    let clauses = grams
        .into_iter()
        .map(|gram| {
            let term = Term::from_field_text(source.fields.filename_ngram, &gram);
            (
                Occur::Must,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
            )
        })
        .collect();

    Ok(Box::new(BooleanQuery::new(clauses)))
}

fn doc_to_hit(
    source: &LoadedSource,
    searcher: &tantivy::Searcher,
    address: DocAddress,
    score: f32,
) -> Result<SearchHit> {
    let doc: TantivyDocument = searcher.doc(address).context("load tantivy document")?;
    let path = first_text(&doc, source.fields.path)
        .unwrap_or_default()
        .into_owned();
    let filename = first_text(&doc, source.fields.filename)
        .unwrap_or(Cow::Borrowed(""))
        .into_owned();
    let extension = first_text(&doc, source.fields.extension).map(|value| value.into_owned());
    let snippet = first_text(&doc, source.fields.content_preview)
        .unwrap_or(Cow::Borrowed(""))
        .into_owned();
    let size_bytes = first_u64(&doc, source.fields.size_bytes);
    let modified_at = first_f64(&doc, source.fields.modified_at);

    Ok(SearchHit {
        path,
        filename,
        extension,
        source: source.kind,
        backend: "tantivy",
        snippet,
        size_bytes,
        modified_at,
        score,
    })
}

fn merge_hits(mut hits: Vec<SearchHit>, limit: usize) -> Vec<SearchHit> {
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.path.cmp(&b.path))
    });

    let mut seen = HashSet::new();
    let mut merged = Vec::new();
    for hit in hits {
        if seen.insert(hit.path.clone()) {
            merged.push(hit);
            if merged.len() >= limit {
                break;
            }
        }
    }
    merged
}

fn plocate_hits(query: &str, limit: usize) -> Vec<SearchHit> {
    let output = std::process::Command::new("plocate")
        .arg("-l")
        .arg(limit.to_string())
        .arg(query)
        .output();

    let Ok(output) = output else {
        return Vec::new();
    };

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let path = line.trim();
            if path.is_empty() {
                return None;
            }

            let filename = Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(path)
                .to_string();

            Some(SearchHit {
                path: path.to_string(),
                filename,
                extension: Path::new(path)
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| format!(".{ext}")),
                source: SourceKind::Linux,
                backend: "plocate",
                snippet: String::new(),
                size_bytes: None,
                modified_at: None,
                score: -10.0,
            })
        })
        .collect()
}

async fn handle_client(stream: UnixStream, state: Arc<AppState>) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = AsyncBufReader::new(read_half);
    let mut line = String::new();
    let count = reader
        .read_line(&mut line)
        .await
        .context("read request line")?;
    if count == 0 {
        return Ok(());
    }

    let request: SocketRequest = serde_json::from_str(line.trim()).context("parse request json")?;
    let response = handle_request(request, &state)?;
    let mut payload = serde_json::to_vec(&response)?;
    payload.push(b'\n');
    write_half
        .write_all(&payload)
        .await
        .context("write response")?;
    Ok(())
}

fn handle_request(request: SocketRequest, state: &AppState) -> Result<serde_json::Value> {
    match request.action.as_str() {
        "search" | "find" => {
            let args = QueryArgs {
                query: request.query.unwrap_or_default(),
                source: request.source.unwrap_or(SourceSelection::Linux),
                extension: request.ext,
                limit: request.limit.unwrap_or(DEFAULT_LIMIT),
                json: true,
                no_plocate: request.no_plocate.unwrap_or(false),
            };
            let mode = if request.action == "search" {
                QueryMode::Search
            } else {
                QueryMode::Find
            };
            let started = Instant::now();
            let hits = search_sources(&state.sources, &args, mode)?;
            Ok(serde_json::to_value(SearchResponse {
                ok: true,
                elapsed_ms: started.elapsed().as_millis(),
                total: hits.len(),
                results: hits.into_iter().map(SearchHit::into_result).collect(),
            })?)
        }
        "status" => Ok(serde_json::to_value(build_daemon_status(state)?)?),
        other => bail!("unsupported action {other}"),
    }
}

fn build_daemon_status(state: &AppState) -> Result<StatusResponse> {
    let mut loaded_sources = Vec::new();
    for source in [SourceKind::Linux, SourceKind::Windows] {
        let meta_path = state.paths.meta_path(source);
        let meta = read_meta(&meta_path).ok();
        loaded_sources.push(SourceStatus {
            source,
            index_path: state.paths.index_path(source).display().to_string(),
            meta_path: meta_path.display().to_string(),
            index_present: state.paths.index_path(source).exists(),
            document_count: meta.as_ref().map(|it| it.document_count),
            imported_at: meta.as_ref().map(|it| it.imported_at),
            source_db_path: meta.and_then(|it| it.source_db_path),
        });
    }

    Ok(StatusResponse {
        ok: true,
        daemon_reachable: true,
        socket_path: state.paths.socket_path.display().to_string(),
        uptime_seconds: Some(state.loaded_at.elapsed().as_secs()),
        loaded_sources,
        schema_version: SCHEMA_VERSION,
        degraded: state.sources.len() < 2,
    })
}

fn build_local_status(paths: &AppPaths, daemon_reachable: bool) -> Result<StatusResponse> {
    let mut loaded_sources = Vec::new();
    for source in [SourceKind::Linux, SourceKind::Windows] {
        let meta_path = paths.meta_path(source);
        let meta = read_meta(&meta_path).ok();
        loaded_sources.push(SourceStatus {
            source,
            index_path: paths.index_path(source).display().to_string(),
            meta_path: meta_path.display().to_string(),
            index_present: paths.index_path(source).exists(),
            document_count: meta.as_ref().map(|it| it.document_count),
            imported_at: meta.as_ref().map(|it| it.imported_at),
            source_db_path: meta.and_then(|it| it.source_db_path),
        });
    }
    Ok(StatusResponse {
        ok: true,
        daemon_reachable,
        socket_path: paths.socket_path.display().to_string(),
        uptime_seconds: None,
        loaded_sources,
        schema_version: SCHEMA_VERSION,
        degraded: true,
    })
}

fn request_via_socket(socket_path: &Path, request: SocketRequest) -> Result<serde_json::Value> {
    if !socket_path.exists() {
        bail!("socket missing");
    }

    let mut stream = StdUnixStream::connect(socket_path)
        .with_context(|| format!("connect socket {}", socket_path.display()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .context("set read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .context("set write timeout")?;

    let mut payload = serde_json::to_vec(&request)?;
    payload.push(b'\n');
    stream.write_all(&payload).context("write socket request")?;
    stream.flush().context("flush socket request")?;

    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    reader
        .read_line(&mut line)
        .context("read socket response")?;
    if line.trim().is_empty() {
        bail!("empty socket response");
    }
    serde_json::from_str(line.trim()).context("decode socket response")
}

fn load_sources(paths: &AppPaths) -> Result<HashMap<SourceKind, Arc<LoadedSource>>> {
    let mut sources = HashMap::new();
    for kind in [SourceKind::Linux, SourceKind::Windows] {
        if let Some(loaded) = LoadedSource::open(paths, kind)? {
            sources.insert(kind, Arc::new(loaded));
        }
    }
    Ok(sources)
}

fn remove_stale_socket(socket_path: &Path) -> Result<()> {
    if !socket_path.exists() {
        return Ok(());
    }

    match StdUnixStream::connect(socket_path) {
        Ok(_) => bail!("socket already active at {}", socket_path.display()),
        Err(_) => {
            fs::remove_file(socket_path)
                .with_context(|| format!("remove stale socket {}", socket_path.display()))?;
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("sigterm handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn build_schema() -> Schema {
    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer("tome_text")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let ngram_indexing = TextFieldIndexing::default()
        .set_tokenizer("filename_ngram")
        .set_index_option(IndexRecordOption::Basic);

    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing)
        .set_stored();
    let ngram_options = TextOptions::default().set_indexing_options(ngram_indexing);

    let mut builder = Schema::builder();
    builder.add_text_field("path", text_options.clone());
    builder.add_text_field("filename", text_options.clone());
    builder.add_text_field("filename_ngram", ngram_options);
    builder.add_text_field("extension", STRING | STORED);
    builder.add_text_field("content_preview", text_options.clone());
    builder.add_text_field("header", text_options);
    builder.add_text_field("source", STRING | STORED);
    builder.add_u64_field("size_bytes", STORED | FAST);
    builder.add_f64_field("modified_at", STORED | FAST);
    builder.add_f64_field("indexed_at", STORED);
    builder.add_text_field("file_hash", STORED);
    builder.build()
}

fn register_tokenizers(index: &Index) {
    index.tokenizers().register(
        "tome_text",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(80))
            .filter(LowerCaser)
            .build(),
    );
    index.tokenizers().register(
        "filename_ngram",
        TextAnalyzer::builder(
            NgramTokenizer::new(2, 4, false).expect("valid ngram tokenizer configuration"),
        )
        .filter(LowerCaser)
        .build(),
    );
}

fn schema_fields(schema: Schema) -> Result<SchemaFields> {
    Ok(SchemaFields {
        path: schema.get_field("path").context("schema field path")?,
        filename: schema
            .get_field("filename")
            .context("schema field filename")?,
        filename_ngram: schema
            .get_field("filename_ngram")
            .context("schema field filename_ngram")?,
        extension: schema
            .get_field("extension")
            .context("schema field extension")?,
        content_preview: schema
            .get_field("content_preview")
            .context("schema field content_preview")?,
        header: schema.get_field("header").ok(),
        source: schema.get_field("source").context("schema field source")?,
        size_bytes: schema
            .get_field("size_bytes")
            .context("schema field size_bytes")?,
        modified_at: schema
            .get_field("modified_at")
            .context("schema field modified_at")?,
        indexed_at: schema
            .get_field("indexed_at")
            .context("schema field indexed_at")?,
        file_hash: schema
            .get_field("file_hash")
            .context("schema field file_hash")?,
    })
}

fn validate_source_schema(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(files)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let columns: HashSet<String> = rows
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .collect();
    for required in [
        "path",
        "filename",
        "extension",
        "size_bytes",
        "modified_at",
        "indexed_at",
        "content_preview",
        "file_hash",
    ] {
        if !columns.contains(required) {
            bail!("mata schema missing required column {required}");
        }
    }
    Ok(())
}

fn normalize_extension(extension: &str) -> String {
    let lowered = extension.to_lowercase();
    if lowered.starts_with('.') {
        lowered
    } else {
        format!(".{lowered}")
    }
}

fn grams_for_query(query: &str) -> Vec<String> {
    let chars: Vec<char> = query.chars().collect();
    let n = if chars.len() >= 3 { 3 } else { 2 };
    if chars.len() < n {
        return vec![query.to_string()];
    }
    let mut grams = Vec::new();
    let mut seen = HashSet::new();
    for window in chars.windows(n) {
        let gram: String = window.iter().collect();
        if seen.insert(gram.clone()) {
            grams.push(gram);
        }
    }
    grams
}

fn first_text<'a>(doc: &'a TantivyDocument, field: Field) -> Option<Cow<'a, str>> {
    doc.get_first(field)
        .and_then(|value| match OwnedValueRef::from(value) {
            OwnedValueRef::Str(text) => Some(text),
            _ => None,
        })
}

fn first_u64(doc: &TantivyDocument, field: Field) -> Option<u64> {
    doc.get_first(field)
        .and_then(|value| match OwnedValueRef::from(value) {
            OwnedValueRef::U64(value) => Some(value),
            _ => None,
        })
}

fn first_f64(doc: &TantivyDocument, field: Field) -> Option<f64> {
    doc.get_first(field)
        .and_then(|value| match OwnedValueRef::from(value) {
            OwnedValueRef::F64(value) => Some(value),
            OwnedValueRef::Date | OwnedValueRef::Str(_) | OwnedValueRef::U64(_) => None,
        })
}

enum OwnedValueRef<'a> {
    Str(Cow<'a, str>),
    U64(u64),
    F64(f64),
    Date,
}

impl<'a> From<&'a tantivy::schema::OwnedValue> for OwnedValueRef<'a> {
    fn from(value: &'a tantivy::schema::OwnedValue) -> Self {
        match value {
            tantivy::schema::OwnedValue::Str(text) => OwnedValueRef::Str(Cow::Borrowed(text)),
            tantivy::schema::OwnedValue::U64(value) => OwnedValueRef::U64(*value),
            tantivy::schema::OwnedValue::F64(value) => OwnedValueRef::F64(*value),
            _ => OwnedValueRef::Date,
        }
    }
}

fn file_mtime(path: &Path) -> Result<f64> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let modified = metadata.modified().context("mtime")?;
    Ok(modified
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow!("mtime before unix epoch"))?
        .as_secs_f64())
}

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn temp_path(target: &Path) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .and_then(|it| it.to_str())
        .unwrap_or("index");
    parent.join(format!(".{name}.tmp-{stamp}"))
}

fn write_json_pretty(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn normalize_tree_permissions(root: &Path) -> Result<()> {
    normalize_path_permissions(root)?;
    if root.is_dir() {
        for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
            let entry = entry?;
            normalize_tree_permissions(&entry.path())?;
        }
    }
    Ok(())
}

fn normalize_path_permissions(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let current = metadata.permissions().mode();
    let desired = if metadata.is_dir() {
        current | 0o770
    } else {
        current | 0o660
    };
    if desired != current {
        let mut perms = metadata.permissions();
        perms.set_mode(desired);
        fs::set_permissions(path, perms).with_context(|| format!("chmod {}", path.display()))?;
    }
    Ok(())
}

fn read_meta(path: &Path) -> Result<SourceMeta> {
    let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&data).with_context(|| format!("parse {}", path.display()))
}

fn print_human_results(query: &str, response: &SearchResponse) {
    println!(
        "tome [{query}] — {} results in {} ms",
        response.total, response.elapsed_ms
    );
    for result in &response.results {
        let source = result.source.as_str();
        let backend = &result.backend;
        if result.snippet.is_empty() {
            println!("  [{source}/{backend}] {}", result.path);
        } else {
            println!(
                "  [{source}/{backend}] {}  ·  {}",
                result.path,
                one_line(&result.snippet, 96)
            );
        }
    }
}

fn print_human_status(response: &StatusResponse) {
    println!(
        "tome status — daemon={} degraded={} socket={}",
        response.daemon_reachable, response.degraded, response.socket_path
    );
    for source in &response.loaded_sources {
        println!(
            "  {} index_present={} docs={}",
            source.source.as_str(),
            source.index_present,
            source
                .document_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
}

fn one_line(text: &str, limit: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= limit {
        return normalized;
    }
    normalized.chars().take(limit).collect::<String>() + "..."
}

#[cfg(test)]
mod header_tests {
    use super::*;

    fn h(preview: &str, ext: &str) -> Option<String> {
        extract_header(preview, Some(ext))
    }

    #[test]
    fn rust_hash_comment_header() {
        let src =
            "// Hot local Tantivy search daemon for Mata imports\n// second line\nuse anyhow;\n";
        let got = h(src, "rs").expect("rust // header");
        assert!(
            got.contains("Hot local Tantivy search daemon"),
            "got: {got}"
        );
        assert!(!got.contains("use anyhow"), "code leaked: {got}");
    }

    #[test]
    fn python_docstring_captured_not_missed() {
        // THE trap: docstring opens, then code, then MORE comments.
        let src = "\"\"\"\ntranslate.py — Anthropic <-> OpenAI translation for kim-proxy v3.\n\"\"\"\nimport json\n# later comment\n";
        let got = h(src, "py").expect("docstring header");
        assert!(
            got.contains("Anthropic <-> OpenAI translation"),
            "got: {got}"
        );
        assert!(!got.contains("import json"), "code leaked: {got}");
    }

    #[test]
    fn c_generated_banner_killed_after_fence_skip() {
        // fence-skip must reach the @generated comment, filter must kill it
        let src = "#if !defined(TORCH_STABLE_ONLY) && !defined(TORCH_TARGET_VERSION)\n#pragma once\n\n// @generated by torchgen/gen.py from Function.h\n\n#include <ATen/Context.h>\n";
        assert_eq!(h(src, "h"), None, "generated C header must yield no header");
    }

    #[test]
    fn c_fence_then_real_comment() {
        let src = "#pragma once\n\n// Fast ring buffer for audio frames\n// lock-free, single producer\nstruct Ring {};\n";
        let got = h(src, "h").expect("real C header");
        assert!(got.contains("Fast ring buffer"), "got: {got}");
        assert!(!got.contains("#pragma"), "fence leaked: {got}");
    }

    #[test]
    fn apache_license_block_killed() {
        let src = "// Copyright (c) 2020 Example Corp\n//\n// Licensed under the Apache License, Version 2.0\n// distributed under the License is distributed on an \"AS IS\" BASIS\nfn main() {}\n";
        assert_eq!(h(src, "rs"), None, "pure license block must yield None");
    }

    #[test]
    fn copyright_prefix_dropped_real_desc_kept() {
        let src = "# Copyright (c) 2024 house\n# Warden — sweep services, restart the dead.\n# Second descriptive line.\nset -e\n";
        let got = h(src, "sh").expect("desc survives copyright strip");
        assert!(got.contains("Warden — sweep services"), "got: {got}");
        assert!(
            !got.to_lowercase().contains("copyright"),
            "copyright leaked: {got}"
        );
    }

    #[test]
    fn bash_shebang_then_comment() {
        let src = "#!/bin/bash\nset -uo pipefail\n# NOTE: no `set -e` — a failing check must not kill the sweep.\nexport PATH=x\n";
        let got = h(src, "sh").expect("bash header after shebang+code");
        assert!(got.contains("no `set -e`"), "got: {got}");
        assert!(!got.contains("export PATH"), "code leaked: {got}");
    }

    #[test]
    fn md_heading_is_header() {
        let src = "# LATEST.md — Kim's Session Handoff\n\nLast updated: 2026-07-30 stuff here.\n";
        let got = h(src, "md").expect("md heading");
        assert!(got.contains("Kim's Session Handoff"), "got: {got}");
        assert!(!got.starts_with('#'), "marker leaked: {got}");
    }

    #[test]
    fn md_frontmatter_description_captured() {
        let src = "---\nname: beacon\ndescription: Light the beacon Monitor\n---\n# Body\n";
        let got = h(src, "md").expect("frontmatter header");
        assert!(got.contains("Light the beacon Monitor"), "got: {got}");
        assert!(!got.contains("Body"), "body leaked past fence: {got}");
    }

    #[test]
    fn go_generated_killed() {
        let src = "// Code generated by protoc-gen-go. DO NOT EDIT.\n\npackage foo\n";
        assert_eq!(h(src, "go"), None, "go generated banner must yield None");
    }

    #[test]
    fn json_no_header_no_panic() {
        let src = "{\n  \"name\": \"x\",\n  \"value\": 1\n}\n";
        assert_eq!(h(src, "json"), None, "json has no comments, no header");
    }

    #[test]
    fn license_long_block_capped_or_killed() {
        // a long license preamble that dodges line filters still dies on phrase
        let src = "# Permission is hereby granted, free of charge, to any person\n# obtaining a copy of this software and associated documentation\n# files (the \"Software\"), to deal in the Software without restriction.\n";
        assert_eq!(h(src, "py"), None, "MIT preamble must die");
    }

    #[test]
    fn empty_after_filter_yields_none_not_empty() {
        let src = "# copyright 2024\n# license: mit\nfn x() {}\n";
        assert_eq!(
            h(src, "rs"),
            None,
            "all-noise must be None, not empty string"
        );
    }

    #[test]
    fn shebang_only_no_header() {
        let src = "#!/usr/bin/env python3\nimport os\nimport sys\n";
        assert_eq!(h(src, "py"), None, "lone shebang yields no header");
    }
}

#[cfg(test)]
mod header_doc_tests {
    use super::*;

    #[test]
    fn rust_use_then_doc_comment() {
        // the actual main.rs shape: use block, then /// doc comment on Cli
        let src = "use anyhow::{Context, Result};\nuse clap::Parser;\nuse std::fs;\n\n/// Hot local Tantivy search daemon for Mata imports\n/// and filesystem indexing\nstruct Cli;\n";
        let got = extract_header(src, Some("rs")).expect("rust doc comment after use");
        assert!(
            got.contains("Hot local Tantivy search daemon"),
            "got: {got}"
        );
        assert!(!got.contains("use anyhow"), "use leaked: {got}");
        assert!(!got.contains("struct Cli"), "code leaked: {got}");
    }

    #[test]
    fn rust_inner_doc_comment() {
        let src = "use std::fs;\n\n//! Module-level docs with bang\n//! second line\nmod foo;\n";
        let got = extract_header(src, Some("rs")).expect("rust //! doc");
        assert!(got.contains("Module-level docs"), "got: {got}");
    }

    #[test]
    fn rust_leading_slash_stripped() {
        let src = "use x;\n/// Hot daemon\nstruct C;\n";
        let got = extract_header(src, Some("rs")).expect("doc");
        assert!(!got.starts_with('/'), "leading slash leaked: {got}");
        assert!(got.starts_with("Hot"), "got: {got}");
    }
}

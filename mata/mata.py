#!/usr/bin/env python3
"""
Mata File Indexer 👁️
A lightweight SQLite+FTS5 file indexer with FastAPI server and MCP integration.
Author: Claude (Opus)
"""

import sqlite3
import hashlib
import json
import time
import threading
import signal
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import argparse

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn


# ============================================================================
# Configuration & Data Structures
# ============================================================================

@dataclass
class IndexStats:
    """Thread-safe index statistics"""
    total_files: int = 0
    total_dirs: int = 0
    files_per_sec: float = 0.0
    last_update: float = 0.0
    is_indexing: bool = False
    start_time: float = 0.0
    
    def __init__(self):
        self.lock = threading.Lock()
        self.total_files = 0
        self.total_dirs = 0
        self.files_per_sec = 0.0
        self.last_update = time.time()
        self.is_indexing = False
        self.start_time = 0.0


# Global state
app = FastAPI(title="Mata File Indexer", version="1.0.0")
config: Dict = {}
db_path: str = ""
stats = IndexStats()
shutdown_event = threading.Event()


# ============================================================================
# Database Layer
# ============================================================================

def init_db(db_path: str) -> None:
    """Initialize SQLite database with FTS5 index"""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL for concurrent reads
    
    # Create main files table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE NOT NULL,
            filename TEXT NOT NULL,
            extension TEXT,
            size_bytes INTEGER,
            modified_at REAL,
            indexed_at REAL,
            content_preview TEXT,
            file_hash TEXT
        )
    """)
    
    # Create FTS5 virtual table
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
            filename,
            path,
            content_preview,
            content='files',
            content_rowid='id'
        )
    """)
    
    # Triggers to keep FTS in sync
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
            INSERT INTO files_fts(rowid, filename, path, content_preview)
            VALUES (new.id, new.filename, new.path, new.content_preview);
        END
    """)
    
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
            INSERT INTO files_fts(files_fts, rowid, filename, path, content_preview)
            VALUES ('delete', old.id, old.filename, old.path, old.content_preview);
        END
    """)
    
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
            INSERT INTO files_fts(files_fts, rowid, filename, path, content_preview)
            VALUES ('delete', old.id, old.filename, old.path, old.content_preview);
            INSERT INTO files_fts(rowid, filename, path, content_preview)
            VALUES (new.id, new.filename, new.path, new.content_preview);
        END
    """)
    
    # Create index on path for faster lookups
    conn.execute("CREATE INDEX IF NOT EXISTS idx_path ON files(path)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_modified ON files(modified_at)")
    
    conn.commit()
    conn.close()


def get_file_hash(path: Path) -> str:
    """Quick hash of file (size + mtime)"""
    try:
        stat = path.stat()
        return hashlib.md5(f"{stat.st_size}:{stat.st_mtime}".encode()).hexdigest()
    except:
        return ""


def read_content_preview(path: Path, max_chars: int) -> Optional[str]:
    """Read first N characters from text file"""
    try:
        # Try UTF-8 first
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            return f.read(max_chars)
    except:
        return None


def batch_insert_files(db_path: str, files: List[Tuple]) -> None:
    """Batch insert files into database"""
    conn = sqlite3.connect(db_path)
    try:
        conn.executemany("""
            INSERT OR REPLACE INTO files 
            (path, filename, extension, size_bytes, modified_at, indexed_at, content_preview, file_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, files)
        conn.commit()
    finally:
        conn.close()


def get_indexed_files(db_path: str) -> Dict[str, str]:
    """Get all indexed files with their hashes"""
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT path, file_hash FROM files")
    result = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    return result


# ============================================================================
# Crawler Engine
# ============================================================================

def should_skip_dir(dir_name: str) -> bool:
    """Check if directory should be skipped"""
    return dir_name in config['skip_dirs']


def should_skip_file(path: Path) -> bool:
    """Check if file should be skipped"""
    # Check extension
    if path.suffix.lower() in config['skip_extensions']:
        return True
    
    # Check size
    try:
        size_mb = path.stat().st_size / (1024 * 1024)
        if size_mb > config['max_file_size_mb']:
            return True
    except:
        return True
    
    return False


def is_text_file(path: Path) -> bool:
    """Check if file is a text file"""
    return path.suffix.lower() in config['text_extensions']


def process_file(path: Path) -> Optional[Tuple]:
    """Process a single file and return data tuple"""
    try:
        if should_skip_file(path):
            return None
        
        stat = path.stat()
        file_hash = get_file_hash(path)
        content_preview = None
        
        # Extract content for text files
        if is_text_file(path):
            content_preview = read_content_preview(path, config['content_preview_chars'])
        
        return (
            str(path),
            path.name,
            path.suffix.lower() or None,
            stat.st_size,
            stat.st_mtime,
            time.time(),
            content_preview,
            file_hash
        )
    except Exception as e:
        return None


def crawl_directory(root: Path, existing_files: Dict[str, str]) -> List[Tuple]:
    """Crawl directory and return list of file tuples"""
    files_to_index = []
    
    try:
        for entry in root.iterdir():
            if shutdown_event.is_set():
                break
            
            try:
                if entry.is_dir():
                    if not should_skip_dir(entry.name):
                        with stats.lock:
                            stats.total_dirs += 1
                        files_to_index.extend(crawl_directory(entry, existing_files))
                
                elif entry.is_file():
                    # Check if file needs re-indexing
                    file_path = str(entry)
                    current_hash = get_file_hash(entry)
                    
                    if file_path in existing_files and existing_files[file_path] == current_hash:
                        # File hasn't changed, skip
                        with stats.lock:
                            stats.total_files += 1
                        continue
                    
                    # Process new/changed file
                    file_data = process_file(entry)
                    if file_data:
                        files_to_index.append(file_data)
                        with stats.lock:
                            stats.total_files += 1
            
            except (PermissionError, OSError):
                continue
    
    except (PermissionError, OSError):
        pass
    
    return files_to_index


def index_files(incremental: bool = False) -> None:
    """Main indexing function"""
    with stats.lock:
        stats.is_indexing = True
        stats.start_time = time.time()
        stats.total_files = 0
        stats.total_dirs = 0
    
    print(f"{'Incremental' if incremental else 'Initial'} indexing started...")
    
    # Get existing files for incremental indexing
    existing_files = get_indexed_files(db_path) if incremental else {}
    
    batch = []
    last_log_time = time.time()
    
    for root_str in config['index_roots']:
        if shutdown_event.is_set():
            break
        
        root = Path(root_str)
        if not root.exists():
            print(f"Warning: Root path does not exist: {root}")
            continue
        
        print(f"Crawling: {root}")
        
        # Crawl and collect files
        for file_data in crawl_directory(root, existing_files):
            if shutdown_event.is_set():
                break
            
            batch.append(file_data)
            
            # Batch insert
            if len(batch) >= config['batch_size']:
                batch_insert_files(db_path, batch)
                batch.clear()
            
            # Progress logging
            current_time = time.time()
            if current_time - last_log_time >= 10:
                elapsed = current_time - stats.start_time
                with stats.lock:
                    stats.files_per_sec = stats.total_files / elapsed if elapsed > 0 else 0
                    print(f"Progress: {stats.total_files} files, {stats.total_dirs} dirs, "
                          f"{stats.files_per_sec:.1f} files/sec")
                last_log_time = current_time
    
    # Insert remaining files
    if batch:
        batch_insert_files(db_path, batch)
    
    with stats.lock:
        elapsed = time.time() - stats.start_time
        stats.files_per_sec = stats.total_files / elapsed if elapsed > 0 else 0
        stats.is_indexing = False
    
    print(f"Indexing complete! {stats.total_files} files indexed in {elapsed:.1f}s "
          f"({stats.files_per_sec:.1f} files/sec)")


def start_background_indexing(incremental: bool = False) -> None:
    """Start indexing in background thread"""
    thread = threading.Thread(target=index_files, args=(incremental,), daemon=True)
    thread.start()


# ============================================================================
# FastAPI Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    with stats.lock:
        return {
            "status": "indexing" if stats.is_indexing else "ready",
            "total_files": stats.total_files,
            "files_per_sec": round(stats.files_per_sec, 1)
        }


@app.get("/stats")
async def get_stats():
    """Get index statistics"""
    conn = sqlite3.connect(db_path)
    
    # Total files
    total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    
    # By extension
    by_ext = conn.execute("""
        SELECT extension, COUNT(*) as count
        FROM files
        WHERE extension IS NOT NULL
        GROUP BY extension
        ORDER BY count DESC
        LIMIT 20
    """).fetchall()
    
    # Last crawl time
    last_crawl = conn.execute("SELECT MAX(indexed_at) FROM files").fetchone()[0]
    
    conn.close()
    
    with stats.lock:
        return {
            "total_files": total,
            "is_indexing": stats.is_indexing,
            "files_per_sec": round(stats.files_per_sec, 1),
            "by_extension": [{"ext": ext, "count": count} for ext, count in by_ext],
            "last_crawl": datetime.fromtimestamp(last_crawl).isoformat() if last_crawl else None
        }


@app.get("/search")
async def search_files(q: str, limit: int = 20, ext: Optional[str] = None):
    """Full-text search using FTS5"""
    if limit > 50:
        limit = 50
    
    start_time = time.time()
    conn = sqlite3.connect(db_path)
    
    # Build query
    if ext:
        query = """
            SELECT f.path, f.filename, f.extension, f.size_bytes, f.modified_at,
                   snippet(files_fts, 2, '**', '**', '...', 64) as snippet
            FROM files_fts
            JOIN files f ON files_fts.rowid = f.id
            WHERE files_fts MATCH ? AND f.extension = ?
            ORDER BY rank
            LIMIT ?
        """
        cursor = conn.execute(query, (q, ext, limit))
    else:
        query = """
            SELECT f.path, f.filename, f.extension, f.size_bytes, f.modified_at,
                   snippet(files_fts, 2, '**', '**', '...', 64) as snippet
            FROM files_fts
            JOIN files f ON files_fts.rowid = f.id
            WHERE files_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """
        cursor = conn.execute(query, (q, limit))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            "path": row[0],
            "filename": row[1],
            "extension": row[2],
            "size": f"{row[3] / 1024:.1f}KB" if row[3] < 1024*1024 else f"{row[3] / (1024*1024):.1f}MB",
            "modified": datetime.fromtimestamp(row[4]).strftime("%Y-%m-%d %H:%M:%S"),
            "snippet": row[5] if row[5] else ""
        })
    
    total_matches = conn.execute("SELECT COUNT(*) FROM files_fts WHERE files_fts MATCH ?", (q,)).fetchone()[0]
    conn.close()
    
    query_time = (time.time() - start_time) * 1000
    
    return {
        "results": results,
        "total_matches": total_matches,
        "query_time_ms": round(query_time, 2),
        "limit": limit
    }


@app.get("/find")
async def find_files(name: str, ext: Optional[str] = None, limit: int = 20):
    """Find files by filename pattern"""
    conn = sqlite3.connect(db_path)
    
    if ext:
        query = """
            SELECT path, filename, extension, size_bytes, modified_at
            FROM files
            WHERE filename LIKE ? AND extension = ?
            LIMIT ?
        """
        cursor = conn.execute(query, (f"%{name}%", ext, limit))
    else:
        query = """
            SELECT path, filename, extension, size_bytes, modified_at
            FROM files
            WHERE filename LIKE ?
            LIMIT ?
        """
        cursor = conn.execute(query, (f"%{name}%", limit))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            "path": row[0],
            "filename": row[1],
            "extension": row[2],
            "size": f"{row[3] / 1024:.1f}KB" if row[3] < 1024*1024 else f"{row[3] / (1024*1024):.1f}MB",
            "modified": datetime.fromtimestamp(row[4]).strftime("%Y-%m-%d %H:%M:%S")
        })
    
    conn.close()
    
    return {"results": results, "total": len(results)}


@app.post("/reindex")
async def trigger_reindex(background_tasks: BackgroundTasks):
    """Trigger incremental re-index"""
    with stats.lock:
        if stats.is_indexing:
            raise HTTPException(status_code=409, detail="Indexing already in progress")
    
    background_tasks.add_task(index_files, True)
    return {"status": "reindex started"}


# ============================================================================
# MCP Integration
# ============================================================================

@app.post("/mcp")
async def mcp_endpoint(request: dict):
    """MCP JSON-RPC endpoint"""
    method = request.get("method")
    params = request.get("params", {})
    req_id = request.get("id", 1)
    
    try:
        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [
                        {
                            "name": "mata_search",
                            "description": "Search indexed files by content or filename",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "query": {"type": "string", "description": "Search query (keywords, filename, code snippet)"},
                                    "type": {"type": "string", "enum": ["content", "filename"], "description": "Search type (default: content)"},
                                    "extension": {"type": "string", "description": "Filter by extension (e.g. '.py', '.md')"},
                                    "limit": {"type": "integer", "description": "Max results (default 20, max 50)"}
                                },
                                "required": ["query"]
                            }
                        },
                        {
                            "name": "mata_status",
                            "description": "Get index health and statistics",
                            "inputSchema": {
                                "type": "object",
                                "properties": {}
                            }
                        }
                    ]
                }
            }
        
        elif method == "tools/call":
            tool_name = params.get("name")
            args = params.get("arguments", {})
            
            if tool_name == "mata_search":
                search_type = args.get("type", "content")
                query = args.get("query")
                ext = args.get("extension")
                limit = args.get("limit", 20)
                
                if search_type == "filename":
                    result = await find_files(query, ext, limit)
                else:
                    result = await search_files(query, limit, ext)
                
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}
                }
            
            elif tool_name == "mata_status":
                result = await get_stats()
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}
                }
            
            else:
                raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")
        
        else:
            raise HTTPException(status_code=400, detail=f"Unknown method: {method}")
    
    except Exception as e:
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32603, "message": str(e)}
        }


# ============================================================================
# Main Entry Point
# ============================================================================

def signal_handler(sig, frame):
    """Graceful shutdown handler"""
    print("\nShutdown signal received, stopping indexing...")
    shutdown_event.set()
    sys.exit(0)


def main():
    global config, db_path
    
    parser = argparse.ArgumentParser(description="Mata File Indexer 👁️")
    parser.add_argument("--config", default="config.json", help="Config file path")
    parser.add_argument("--reindex", action="store_true", help="Force full re-index")
    parser.add_argument("--port", type=int, help="Override server port")
    args = parser.parse_args()
    
    # Load config
    config_path = Path(__file__).parent / args.config
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path) as f:
        config = json.load(f)
    
    if args.port:
        config['server_port'] = args.port
    
    db_path = config['db_path']
    
    # Initialize database
    print(f"Initializing database: {db_path}")
    init_db(db_path)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check if initial indexing is needed
    conn = sqlite3.connect(db_path)
    file_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    conn.close()
    
    if file_count == 0 or args.reindex:
        print("Starting initial indexing in background...")
        start_background_indexing(incremental=False)
    else:
        print(f"Database contains {file_count} files. Use --reindex to force re-index.")
    
    # Start server
    print(f"Starting Mata server on {config['server_host']}:{config['server_port']}")
    uvicorn.run(
        app,
        host=config['server_host'],
        port=config['server_port'],
        log_level="info"
    )


if __name__ == "__main__":
    main()

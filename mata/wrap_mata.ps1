# wrap_mata.ps1 — Mata wrapper loop
# Run this in a WT pane. Mata lives here, dies here, revives here.

$dir = $PSScriptRoot
$host.UI.RawUI.WindowTitle = "Mata 👁️ :9874"

while ($true) {
    # 🚽 Clean the toilet seat first
    $squatter = Get-NetTCPConnection -LocalPort 9874 -State Listen -ErrorAction SilentlyContinue
    if ($squatter) {
        Write-Host "$(Get-Date -Format 'HH:mm:ss') | Port 9874 occupied (PID $($squatter.OwningProcess)) — evicting..." -ForegroundColor Yellow
        Stop-Process -Id $squatter.OwningProcess -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 500
    }

    Write-Host "$(Get-Date -Format 'HH:mm:ss') | Starting mata.py..." -ForegroundColor Cyan
    & python "$dir\mata.py"
    Write-Host "$(Get-Date -Format 'HH:mm:ss') | Mata died. Restarting in 3s..." -ForegroundColor Red
    Start-Sleep -Seconds 3
}

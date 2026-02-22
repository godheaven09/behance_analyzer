# =============================================================
# Behance Analyzer — Deploy to server (run from your Windows PC)
# Usage: powershell -File deploy.ps1
# =============================================================

$SERVER = "root@80.249.145.203"
$REMOTE_DIR = "/root/behance-analyzer"
$LOCAL_DIR = Split-Path -Parent $PSScriptRoot  # parent of deploy/

Write-Host "=== Deploying Behance Analyzer ===" -ForegroundColor Cyan

# 1. Create remote directory
Write-Host "[1/4] Creating remote directory..."
ssh $SERVER "mkdir -p $REMOTE_DIR/data $REMOTE_DIR/reports $REMOTE_DIR/deploy"

# 2. Upload project files
Write-Host "[2/4] Uploading files..."
$files = @("config.py", "db.py", "scraper.py", "analyzer.py", "run.py", "requirements.txt", "launch.py")
foreach ($f in $files) {
    $src = Join-Path $LOCAL_DIR $f
    if (Test-Path $src) {
        scp $src "${SERVER}:${REMOTE_DIR}/${f}"
        Write-Host "  Uploaded: $f" -ForegroundColor Green
    } else {
        Write-Host "  SKIP (not found): $f" -ForegroundColor Yellow
    }
}

# Upload setup script
scp (Join-Path $LOCAL_DIR "deploy\setup_server.sh") "${SERVER}:${REMOTE_DIR}/deploy/setup_server.sh"
Write-Host "  Uploaded: deploy/setup_server.sh" -ForegroundColor Green

# 3. Upload existing database if present
$dbPath = Join-Path $LOCAL_DIR "data\behance.db"
if (Test-Path $dbPath) {
    Write-Host "[3/4] Uploading existing database..."
    scp $dbPath "${SERVER}:${REMOTE_DIR}/data/behance.db"
    Write-Host "  Database uploaded!" -ForegroundColor Green
} else {
    Write-Host "[3/4] No local database to upload, skipping"
}

# 4. Run setup on server
Write-Host "[4/4] Running server setup..."
ssh $SERVER "cd $REMOTE_DIR && bash deploy/setup_server.sh"

Write-Host ""
Write-Host "=== Deployment Complete ===" -ForegroundColor Green
Write-Host "To run manually: ssh $SERVER 'cd $REMOTE_DIR && venv/bin/python run.py full'"
Write-Host "To check logs:   ssh $SERVER 'tail -50 $REMOTE_DIR/cron.log'"
Write-Host "To get reports:  scp ${SERVER}:${REMOTE_DIR}/reports/*.txt ."

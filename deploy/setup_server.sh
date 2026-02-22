#!/bin/bash
# =============================================================
# Behance Analyzer — Server setup script
# Run this ON THE SERVER after uploading files
# Usage: bash setup_server.sh
# =============================================================

set -e

APP_DIR="$HOME/behance-analyzer"
VENV_DIR="$APP_DIR/venv"

echo "=== Behance Analyzer Server Setup ==="

# 1. System dependencies
echo "[1/6] Installing system packages..."
sudo apt update -qq
sudo apt install -y -qq python3 python3-pip python3-venv \
    libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
    libnspr4 libnss3 libx11-xcb1 > /dev/null 2>&1

# 2. Create app directory
echo "[2/6] Setting up application directory..."
mkdir -p "$APP_DIR/data" "$APP_DIR/reports"

# 3. Python venv
echo "[3/6] Creating Python virtual environment..."
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

# 4. Install Python dependencies
echo "[4/6] Installing Python dependencies..."
pip install --upgrade pip -q
pip install playwright pandas scipy numpy -q

# 5. Install Playwright browsers
echo "[5/6] Installing Chromium for Playwright..."
playwright install chromium
playwright install-deps chromium 2>/dev/null || true

# 6. Setup cron
echo "[6/6] Setting up cron job (every 12 hours)..."

CRON_CMD="0 8,20 * * * cd $APP_DIR && $VENV_DIR/bin/python run.py full >> $APP_DIR/cron.log 2>&1"

# Check if cron already exists
(crontab -l 2>/dev/null | grep -v "behance-analyzer" ; echo "$CRON_CMD") | crontab -

echo ""
echo "=== Setup Complete ==="
echo "App directory: $APP_DIR"
echo "Virtual env:   $VENV_DIR"
echo "Database:      $APP_DIR/data/behance.db"
echo "Reports:       $APP_DIR/reports/"
echo "Cron log:      $APP_DIR/cron.log"
echo ""
echo "Cron schedule: 08:00 and 20:00 daily (server time)"
echo ""
echo "To run manually:"
echo "  cd $APP_DIR && $VENV_DIR/bin/python run.py full"
echo ""
echo "To check cron:"
echo "  crontab -l"

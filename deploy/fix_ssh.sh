#!/bin/bash
set -e
echo "=== Fixing SSH access ==="

# 1. Stop everything
echo "[1] Stopping services..."
systemctl stop sslh 2>/dev/null || true
systemctl stop nginx 2>/dev/null || true

# 2. Make sure nothing is on 443
echo "[2] Clearing port 443..."
fuser -k 443/tcp 2>/dev/null || true
sleep 1

# 3. Fix nginx to 8443
echo "[3] Moving nginx to 8443..."
for f in /etc/nginx/sites-enabled/*; do
    [ -f "$f" ] || continue
    sed -i 's/listen 443/listen 8443/g' "$f"
    sed -i 's/listen \[::\]:443/listen [::]:8443/g' "$f"
    # Also catch "listen 443 ssl" patterns that weren't caught
    sed -i 's/443 ssl/8443 ssl/g' "$f"
done
nginx -t && systemctl start nginx
echo "    nginx OK on 8443"

# 4. Figure out sslh config format
echo "[4] Configuring sslh..."
SSLH_VERSION=$(sslh --version 2>&1 | head -1 || echo "unknown")
echo "    sslh version: $SSLH_VERSION"

# Method A: /etc/default/sslh (older Ubuntu)
cat > /etc/default/sslh << 'EOF'
RUN=yes
DAEMON=/usr/sbin/sslh
DAEMON_OPTS="--user sslh --listen 0.0.0.0:443 --ssh 127.0.0.1:22 --tls 127.0.0.1:8443 --pidfile /var/run/sslh/sslh.pid"
EOF

# Method B: /etc/sslh.cfg (newer sslh)
cat > /etc/sslh.cfg << 'EOF'
foreground: false;
inetd: false;
numeric: false;
transparent: false;
timeout: 5;
user: "sslh";
pidfile: "/var/run/sslh/sslh.pid";

listen:
(
    { host: "0.0.0.0"; port: "443"; }
);

protocols:
(
    { name: "ssh"; service: "ssh"; host: "127.0.0.1"; port: "22"; },
    { name: "tls"; host: "127.0.0.1"; port: "8443"; }
);
EOF

# Make sure pidfile dir exists
mkdir -p /var/run/sslh
chown sslh:sslh /var/run/sslh 2>/dev/null || true

# Try to start sslh
systemctl restart sslh 2>/dev/null || true

# Check if sslh is running
if ! systemctl is-active --quiet sslh; then
    echo "    systemd sslh failed, trying manual start..."
    # Try running directly
    sslh --listen 0.0.0.0:443 --ssh 127.0.0.1:22 --tls 127.0.0.1:8443 --pidfile /var/run/sslh/sslh.pid --user sslh -f &
    sleep 2
fi

# 5. Verify
echo ""
echo "=== Verification ==="
echo "Port 443:"
ss -tlnp | grep ':443 ' || echo "  NOTHING on 443!"
echo "Port 8443:"
ss -tlnp | grep ':8443 ' || echo "  NOTHING on 8443!"
echo "Port 22:"
ss -tlnp | grep ':22 ' || echo "  NOTHING on 22!"
echo ""
echo "sslh status:"
systemctl is-active sslh 2>/dev/null || echo "  not running via systemd"
pgrep -a sslh || echo "  no sslh process found"

# 6. Test SSH locally
echo ""
echo "=== Local SSH test ==="
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes root@127.0.0.1 "echo LOCAL_SSH_OK" 2>&1 || echo "  local ssh test done"

echo ""
echo "=== Done ==="

#!/bin/bash
set -e

echo "=== Fixing SSH access via sslh ==="

# Stop nginx temporarily
systemctl stop nginx

# Make nginx listen on 8443 instead of 443
find /etc/nginx/sites-enabled/ -type f -exec sed -i 's/listen 443/listen 8443/g' {} \;
find /etc/nginx/sites-enabled/ -type f -exec sed -i 's/listen \[::\]:443/listen [::]:8443/g' {} \;

# Verify nginx config and start
nginx -t && systemctl start nginx
echo "Nginx moved to port 8443"

# Configure sslh
mkdir -p /var/run/sslh
cat > /etc/default/sslh << 'SSLHEOF'
RUN=yes
DAEMON=/usr/sbin/sslh
DAEMON_OPTS="--user sslh --listen 0.0.0.0:443 --ssh 127.0.0.1:22 --tls 127.0.0.1:8443 --pidfile /var/run/sslh/sslh.pid"
SSLHEOF

systemctl restart sslh
systemctl enable sslh
echo "sslh configured on port 443"

# Fix cron to use correct path
CRON_CMD="0 8,20 * * * cd /root/behance_analyzer && /root/behance_analyzer/venv/bin/python run.py full >> /root/behance_analyzer/cron.log 2>&1"
(crontab -l 2>/dev/null | grep -v "behance" ; echo "$CRON_CMD") | crontab -
echo "Cron fixed"

# Show status
echo ""
echo "=== Status ==="
ss -tlnp | grep -E '443|8443|22'
systemctl is-active sslh
echo ""
echo "SSH should now work on port 443"

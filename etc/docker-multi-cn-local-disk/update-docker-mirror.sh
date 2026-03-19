#!/bin/bash
# Update Docker Registry Mirror configuration with better mirrors
# This script updates existing configuration with more reliable mirrors

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Updating Docker Registry Mirror Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "This script needs to be run with sudo"
    echo "Usage: sudo $0"
    exit 1
fi

# Backup existing config if it exists
if [ -f /etc/docker/daemon.json ]; then
    echo "Backing up existing /etc/docker/daemon.json..."
    cp /etc/docker/daemon.json /etc/docker/daemon.json.backup.$(date +%Y%m%d_%H%M%S)
fi

# Update daemon.json with better mirrors
echo "Updating Docker registry mirrors with more reliable sources..."

cat > /etc/docker/daemon.json << 'EOF'
{
  "registry-mirrors": [
    "https://dockerproxy.com",
    "https://docker.m.daocloud.io",
    "https://docker.nju.edu.cn",
    "https://mirror.baidubce.com",
    "https://docker.mirrors.sjtug.sjtu.edu.cn",
    "https://hub-mirror.c.163.com"
  ]
}
EOF

echo "✅ Docker registry mirrors updated!"
echo ""
echo "New mirrors (in priority order):"
echo "  1. https://dockerproxy.com (Docker Proxy - recommended)"
echo "  2. https://docker.m.daocloud.io (DaoCloud)"
echo "  3. https://docker.nju.edu.cn (NJU)"
echo "  4. https://mirror.baidubce.com (Baidu)"
echo "  5. https://docker.mirrors.sjtug.sjtu.edu.cn (SJTU)"
echo "  6. https://hub-mirror.c.163.com (163)"
echo ""

# Restart Docker daemon
echo "Restarting Docker daemon..."
if systemctl is-active --quiet docker; then
    systemctl restart docker
    echo "✅ Docker daemon restarted"
else
    echo "⚠️  Docker daemon is not running. Please start it manually:"
    echo "   sudo systemctl start docker"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Testing configuration:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Verify configuration:"
echo "  docker info | grep -A 10 'Registry Mirrors'"
echo ""
echo "Test with:"
echo "  docker pull hello-world"
echo ""

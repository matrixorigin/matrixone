#!/bin/bash
# Setup Docker Registry Mirror for faster image pulls
# This script configures Docker daemon to use Chinese mirrors

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Docker Registry Mirror Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "This script needs to be run with sudo"
    echo "Usage: sudo $0"
    exit 1
fi

# Create docker config directory if it doesn't exist
mkdir -p /etc/docker

# Backup existing config if it exists
if [ -f /etc/docker/daemon.json ]; then
    echo "Backing up existing /etc/docker/daemon.json..."
    cp /etc/docker/daemon.json /etc/docker/daemon.json.backup.$(date +%Y%m%d_%H%M%S)
fi

# Create or update daemon.json
echo "Configuring Docker registry mirrors..."

# Common Chinese Docker mirrors (multiple options for better availability)
cat > /etc/docker/daemon.json << 'EOF'
{
  "registry-mirrors": [
    "https://docker.1panel.live",
    "https://hub3.nat.tf",
    "https://dockerproxy.com",
    "https://docker.m.daocloud.io",
    "https://docker.nju.edu.cn",
    "https://mirror.baidubce.com",
    "https://docker.mirrors.sjtug.sjtu.edu.cn",
    "https://hub-mirror.c.163.com"
  ]
}
EOF

echo "✅ Docker registry mirrors configured!"
echo ""
echo "Configured mirrors (in priority order):"
echo "  1. https://dockerproxy.com (Docker Proxy)"
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
    # edev-up-prometheus-localcho "✅ Docker daemon restarted"
else
    echo "⚠️  Docker daemon is not running. Please start it manually:"
    echo "   sudo systemctl start docker"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Verification:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "To verify the configuration, run:"
echo "  docker info | grep -A 10 'Registry Mirrors'"
echo ""
echo "Or test with:"
echo "  docker pull hello-world"
echo ""

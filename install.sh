#!/bin/bash
# Idempotent auto-installer for Ubuntu/Debian. Run as root: sudo ./install.sh
set -e
[ "$EUID" -eq 0 ] || { echo "Run as root (sudo ./install.sh)"; exit 1; }
cd "$(dirname "$(readlink -f "$0")")"
DIR="$(pwd)"
. /etc/os-release
case "$ID" in ubuntu|debian) ;; *) echo "Only Ubuntu/Debian supported"; exit 1;; esac
export DEBIAN_FRONTEND=noninteractive

echo "==> System packages"
apt-get update -y
apt-get install -y python3 python3-pip python3-venv git curl snapd lxcfs redis-server
apt-get install -y mysql-server || apt-get install -y mariadb-server
systemctl enable --now redis-server || true
systemctl enable --now mysql 2>/dev/null || systemctl enable --now mariadb

echo "==> Docker"
command -v docker >/dev/null || curl -fsSL https://get.docker.com | sh
systemctl enable --now docker

echo "==> LXD"
if ! command -v lxc >/dev/null; then
  systemctl enable --now snapd || true
  snap install lxd || true
  export PATH="$PATH:/snap/bin"
fi
if command -v lxc >/dev/null && [ -z "$(lxc storage list --format csv 2>/dev/null)" ]; then lxd init --auto || true; fi

[ -f .env ] || cp .env.example .env

getv() { grep -E "^$1=" .env | head -1 | cut -d= -f2-; }
setv() { python3 - "$1" "$2" <<'PY'
import sys,re
k,v=sys.argv[1],sys.argv[2]
s=open('.env').read()
line=f"{k}={v}"
if re.search(rf'^{k}=.*$',s,re.M): s=re.sub(rf'^{k}=.*$',lambda m:line,s,flags=re.M)
else: s+="\n"+line+"\n"
open('.env','w').write(s)
PY
}
ask() { # key prompt [secret]
  local cur; cur="$(getv "$1")"; local v
  if [ "$3" = "s" ]; then read -r -s -p "$2 [${cur:+set}]: " v; echo; else read -r -p "$2 [$cur]: " v; fi
  [ -n "$v" ] && setv "$1" "$v" || true
}

echo "==> Configuration (press Enter to keep current value)"
ask BOT_TOKEN "Enter BOT_TOKEN" s
ask OWNER_ID "Enter OWNER_ID (Discord user id)"
ask ADMIN_IDS "Enter ADMIN_IDS (comma separated)"
ask GUILD_ID "Enter GUILD_ID"
ask LOG_CHANNEL_ID "Enter LOG_CHANNEL_ID"
ask DB_PASS "Enter MySQL password for vpsbot" s
read -r -p "Enable LXC mode? (y/n): " L
[ "$L" = "y" ] && setv LXC_MODE on || setv LXC_MODE off
[ -z "$(getv LXC_HOST_IP)" ] && setv LXC_HOST_IP "$(curl -s https://ifconfig.me || hostname -I | awk '{print $1}')"
setv REDIS_URL "redis://localhost:6379/0"

echo "==> MySQL database + user"
DBPW="$(getv DB_PASS)"; [ -n "$DBPW" ] || { echo "DB_PASS is empty"; exit 1; }
mysql -e "CREATE DATABASE IF NOT EXISTS vpsbot CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'vpsbot'@'localhost' IDENTIFIED BY '$DBPW';
ALTER USER 'vpsbot'@'localhost' IDENTIFIED BY '$DBPW';
GRANT ALL PRIVILEGES ON vpsbot.* TO 'vpsbot'@'localhost'; FLUSH PRIVILEGES;"

echo "==> Python venv"
[ -d venv ] || python3 -m venv venv
./venv/bin/pip install -q --upgrade pip
./venv/bin/pip install -q -r requirements.txt

echo "==> systemd service"
cat > /etc/systemd/system/vpsbot.service <<UNIT
[Unit]
Description=Discord VPS Bot
After=network-online.target mysql.service docker.service
Wants=network-online.target

[Service]
WorkingDirectory=$DIR
Environment=PATH=/snap/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=$DIR/venv/bin/python $DIR/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable vpsbot
systemctl restart vpsbot
echo "✅ Bot is running! Check logs: journalctl -u vpsbot -f"

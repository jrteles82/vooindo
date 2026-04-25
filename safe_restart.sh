#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/skyscanner-bot"
SERVICE="skyscanner-bot.service"
CHECK_SCRIPT="$PROJECT_DIR/maintenance_check.sh"

cd "$PROJECT_DIR"

if [[ ! -f "$CHECK_SCRIPT" ]]; then
  echo "ERRO: script de checklist não encontrado: $CHECK_SCRIPT"
  exit 1
fi

bash "$CHECK_SCRIPT"

echo
echo "== Reiniciando serviço =="
sudo systemctl restart "$SERVICE"

echo
echo "== Status após restart =="
systemctl status "$SERVICE" --no-pager -l | sed -n '1,40p'

echo
echo "== Logs após restart =="
journalctl -u "$SERVICE" --since "2 minutes ago" --no-pager | tail -n 80

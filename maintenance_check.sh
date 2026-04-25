#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/skyscanner-bot"
SERVICE="skyscanner-bot.service"

cd "$PROJECT_DIR"

echo "== Projeto =="
pwd

echo
printf '== Branch ==\n'
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  branch="$(git branch --show-current || true)"
  if [[ -z "$branch" ]]; then
    echo "ERRO: repositório está em detached HEAD. Pare aqui antes de alterar ou reiniciar."
    git status --short --branch
    exit 1
  fi
  echo "$branch"
else
  echo "indisponível (deploy sem metadata git)"
fi

echo
printf '== HEAD ==\n'
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git rev-parse --short HEAD
else
  echo "indisponível"
fi

echo
printf '== Git status ==\n'
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git status --short
else
  echo "indisponível"
fi

echo
printf '== Service status ==\n'
systemctl status "$SERVICE" --no-pager -l | sed -n '1,40p'

echo
printf '== Últimos logs ==\n'
journalctl -u "$SERVICE" -n 40 --no-pager

echo
printf 'Checklist OK. Se for reiniciar, use:\n'
echo "sudo systemctl restart $SERVICE"

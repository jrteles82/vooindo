#!/bin/bash
# Renova sessão Google local e sincroniza perfis para o VPS.
# Rodar quando o bot avisar "⚠️ Sessão Google expirada".
#
# Configuração: edite as variáveis abaixo antes de usar no VPS.

set -e

VPS_USER="teles"
VPS_HOST="localhost"          # alterar para IP/host do VPS ao publicar
VPS_PORT="22"
BOT_PATH="/opt/skyscanner-bot"
LOCAL_BOT_PATH="$(cd "$(dirname "$0")" && pwd)"
NUM_PROFILES=4

RSYNC_EXCLUDES=(
  --exclude='Default/Cache'
  --exclude='Default/Code Cache'
  --exclude='Default/GPUCache'
  --exclude='Default/DawnWebGPUCache'
  --exclude='Default/DawnGraphiteCache'
)

echo "=== Sync de sessão Google para VPS ==="
echo "VPS: ${VPS_USER}@${VPS_HOST}:${VPS_PORT}"
echo "Bot path: ${BOT_PATH}"
echo

cd "$LOCAL_BOT_PATH"

if [ ! -d "google_session" ]; then
  echo "ERRO: diretório google_session/ não encontrado em $LOCAL_BOT_PATH"
  exit 1
fi

# Passo 1: replicar google_session -> google_session_1..N
echo "[1/3] Replicando google_session para $NUM_PROFILES perfis locais..."
for i in $(seq 1 $NUM_PROFILES); do
  rsync -a --delete "${RSYNC_EXCLUDES[@]}" google_session/ "google_session_${i}/"
  echo "  google_session_${i}/ ok"
done

# Passo 2: sincronizar perfis para VPS
echo "[2/3] Enviando perfis para o VPS..."
for i in $(seq 1 $NUM_PROFILES); do
  rsync -az --delete "${RSYNC_EXCLUDES[@]}" \
    -e "ssh -p ${VPS_PORT}" \
    "google_session_${i}/" \
    "${VPS_USER}@${VPS_HOST}:${BOT_PATH}/google_session_${i}/"
  echo "  google_session_${i}/ -> VPS ok"
done

# Passo 3: reiniciar workers no VPS para pegar os novos perfis
echo "[3/3] Reiniciando job_workers no VPS..."
ssh -p "${VPS_PORT}" "${VPS_USER}@${VPS_HOST}" \
  "pkill -f job_worker.py || true"
echo "  Workers reiniciados (run_all.py vai respawnar automaticamente)"

echo
echo "=== Concluído ==="

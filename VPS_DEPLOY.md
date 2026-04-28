# Deploy e Manutenção no VPS

## Configuração inicial

### 1. Editar variáveis em `sync_sessions_vps.sh`

```bash
VPS_USER="seu_usuario"
VPS_HOST="ip-do-vps"       # ex: 123.456.789.0
VPS_PORT="22"
BOT_PATH="/caminho/no/vps" # ex: /home/teles/bot
```

### 2. Configurar acesso SSH sem senha (recomendado)

```bash
ssh-copy-id -p 22 usuario@ip-do-vps
```

---

## Renovar sessão Google (quando bot avisar)

O bot envia `⚠️ Sessão Google expirada` quando o Chrome não consegue buscar voos.

### Passo a passo

**1. Renovar sessão localmente:**
```bash
cd /home/teles/dev/python/vooindo-bot
source .venv/bin/activate
python renew_google_session.py
# Chrome abre → faz login na conta Google → pressiona Enter no terminal
```

**2. Sincronizar para o VPS:**
```bash
./sync_sessions_vps.sh
```

O script faz automaticamente:
- Copia `google_session/` → `google_session_1..4` (um por worker)
- Envia todos os 4 perfis para o VPS via rsync
- Reinicia os `job_worker.py` no VPS (run_all.py respawna em seguida)

### Com que frequência

Sessão Google dura semanas/meses. Só renovar quando receber o alerta.

---

## Estrutura de perfis Chrome

| Diretório          | Usado por              |
|--------------------|------------------------|
| `google_session/`  | Fonte de renovação     |
| `google_session_1/`| job_worker 1           |
| `google_session_2/`| job_worker 2           |
| `google_session_3/`| job_worker 3 (se N=4)  |
| `google_session_4/`| job_worker 4 (se N=4)  |

Número de workers configurado em `run_all.py`:
```python
NUM_JOB_WORKERS = int(os.getenv('NUM_JOB_WORKERS', '2'))
```

---

## Variáveis de ambiente relevantes

| Variável                       | Descrição                                 |
|--------------------------------|-------------------------------------------|
| `NUM_JOB_WORKERS`              | Número de workers paralelos (padrão: 2)   |
| `GOOGLE_PERSISTENT_PROFILE_DIR`| Perfil Chrome do worker (setado pelo run_all) |
| `SCAN_INTERVAL_MINUTES`        | Intervalo do agendador em minutos         |
| `JOB_WORKER_POLL_SECONDS`      | Frequência de poll dos workers (padrão: 5s) |
| `DB_ENGINE`                    | `mysql`                                   |
| `MYSQL_URL`                    | URL de conexão MySQL                      |
| `TELEGRAM_BOT_TOKEN`           | Token do bot Telegram                     |
| `TELEGRAM_ADMIN_CHAT_ID`       | Chat ID para alertas de admin             |

---

## Iniciar/parar o bot

```bash
# Iniciar tudo
python run_all.py

# Parar tudo
pkill -f run_all.py

# Reiniciar só os workers (sem parar o bot)
pkill -f job_worker.py
# run_all.py respawna automaticamente em ~2s
```

# README de Manutenção de Configurações

Documento enxuto com o que vale no projeto hoje.

## Fontes de configuração

- `.env`: segredos e parâmetros de infraestrutura/execução.
- `MySQL`: regras dinâmicas de negócio, filas, usuários, pagamentos, rotas e histórico.
- código (`main.py`, `.py`, `run_all.py`): comportamento operacional controlado por variáveis de ambiente.

## Banco usado atualmente

O runtime oficial usa **MySQL**.

Variáveis principais:
- `DB_ENGINE=mysql`
- `MYSQL_URL=...`

`DB_PATH` e arquivos SQLite antigos não fazem parte do fluxo ativo.

## Variáveis importantes no `.env`

### Obrigatórias
- `DB_ENGINE`
- `MYSQL_URL`
- `TELEGRAM_BOT_TOKEN`
- `MP_ACCESS_TOKEN`

### Operacionais comuns
- `JOB_WORKER_POLL_SECONDS`
- `JOB_WORKER_CACHE_TTL_SECONDS`
- `NUM_JOB_WORKERS`
- `GOOGLE_FLIGHTS_EXECUTOR_ENABLED`
- `GOOGLE_FLIGHTS_EXECUTOR_PATH`
- `GOOGLE_FLIGHTS_EXECUTOR_TIMEOUT_MS`
- `GOOGLE_FLIGHTS_EXECUTOR_HEADLESS`
- `GOOGLE_FLIGHTS_EXECUTOR_SLOW_MO_MS`
- `GOOGLE_PERSISTENT_PROFILE_ENABLED`
- `GOOGLE_PERSISTENT_PROFILE_DIR`
- `GOOGLE_STORAGE_STATE_PATH`
- `GOOGLE_SETTLE_SECONDS`
- `GOOGLE_REQUEST_PAUSE_SECONDS`
- `SCAN_IMAGE_SCALE`
- `SCAN_IMAGE_TARGET_WIDTH`
- `PAYMENT_WEBHOOK_PORT`
- `_SECRET_KEY`

## Onde ficam os dados dinâmicos

No MySQL:
- `admins`
- `monetization_settings`
- `user_access`
- `bot_users`
- `bot_settings`
- `payments`
- `scan_jobs`
- `scan_cache`
- `users`
- `user_routes`
- `user_telegram`
- `user_runs`
- `app_settings`

## Operação

### Validar sintaxe
```bash
python3 -m py_compile main.py .py job_worker.py bot_scheduler.py
```

### Reiniciar serviço
```bash
sudo systemctl restart vooindo-bot.service
systemctl status vooindo-bot.service --no-pager --lines=20
```

### Logs
```bash
sudo journalctl -u vooindo-bot.service -f
```

## Observação sobre legado

Artefatos SQLite antigos podem existir apenas como histórico/migração. O runtime atual não deve depender deles.

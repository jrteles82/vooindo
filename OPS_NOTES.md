# OPS_NOTES.md

## Regras operacionais combinadas

- Quando o Sr. Junior disser **"no changes"**, não fazer alterações de código. Apenas analisar, acompanhar e reportar.

## Comandos úteis

### Reiniciar serviço

```bash
sudo systemctl restart vooindo.service
```

### Ver status do serviço

```bash
systemctl status vooindo.service --no-pager -l
```

### Filtrar alertas e problemas relevantes no journal

```bash
journalctl -u vooindo.service --no-pager -l | rg "ALERT_ADMIN|JOB_RECOVERY|PROCESS_EXIT|SCHED_FAIL|SCHED_DB_LIMIT"
```

## Lições recentes

### Fila / workers

- Usar **2 job workers** foi mais estável que 4.
- Reutilizar conexão de banco por muito tempo no worker pode deixar processo vivo sem consumir jobs `pending`.
- Abrir e fechar conexão a cada iteração do loop do worker ajudou a destravar captura de fila.
- `running_timeout_minutes = 5` ficou agressivo para scraping pesado com paralelismo; 15 minutos foi mais seguro.

### Sessão Google

- O perfil usado pelos workers precisa ser o mesmo perfil renovado pelo script de sessão.
- O perfil operacional consolidado ficou como **`google_session`**.
- Para renovar sessão:

```bash
cd /home/teles/dev/python/skyscanner-bot
source .venv/bin/activate
python renew_google_session.py
```

### Preço máximo

- Se o usuário não tiver `max_price` cadastrado, não aplicar filtro de preço.

### Alertas/logs

- Prefixos úteis para filtrar no log:
  - `[ALERT_ADMIN][JOB_WORKER]`
  - `[ALERT_ADMIN][RUN_ALL]`
  - `[ALERT_ADMIN][SCHEDULER]`
  - `[JOB_RECOVERY]`
  - `[PROCESS_EXIT]`
  - `[SCHED_FAIL]`
  - `[SCHED_DB_LIMIT]`

## Incidentes observados

- Restarts manuais no meio da rodada podem represar a fila e atrasar execução em massa.
- `Chat not found` indica usuário/chat inválido ou bloqueado e não deve continuar consumindo tempo de fila.
- Erro de **Sessão Google expirada** pode ser desalinhamento de perfil ou expiração real da autenticação.

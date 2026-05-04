# Firefox Login Solution

## Problema

O Chrome/Chromium headless com Playwright é bloqueado pelo Google na página `accounts.google.com/v3/signin/rejected` — mesmo com perfil persistente, stealth habilitado, app password válida. O Firefox com headless=False (via Xvfb) passa despercebido.

## Arquivos

| Arquivo | Função |
|---------|--------|
| `google_login_firefox_stdin.py` | Login via Firefox Playwright (headful) |
| `google_login_stdin.py` | Login via Chrome (legado, fallback) |
| `google_session_firefox/` | Perfil persistente do Firefox |
| `check_google_session.py` | Verificador de score 0-3 |

## Fluxo

1. Firefox faz login no Google (Xvfb :98, headless=False, stealth)
2. Firefox salva cookies no perfil persistente (`google_session_firefox/`)
3. Script lê cookies do SQLite do Firefox (`moz_cookies`)
4. Escreve direto no SQLite do Chrome (`google_session/Default/Cookies`)
5. Sincroniza para workers via `google_session_sync.py`
6. Ajusta permissões (`chown ubuntu:ubuntu`)

## Cookie Transfer — Detalhe Crítico

A transferência é feita via SQLite direto (não `add_cookies()` do Playwright) porque:

- `add_cookies()` não persiste cookies `__Host-*` corretamente no disco
- Chrome usa formato Windows epoch (microdesde 1601-01-01)
- Firefox usa Unix epoch (milissegundo desde 1970-01-01)

**Regra de ouro para `__Host-GAPS`:**
- NÃO adicionar leading dot no host (`accounts.google.com`, não `.accounts.google.com`)
- Prefixo `__Host-` exige Domain attribute vazio (RFC 6265)

```python
if name.startswith('__Host-'):
    host = host  # mantém como está, sem ponto
else:
    host = '.' + host  # adiciona ponto pra domain cookies
```

Cookies transferidos: NID, OTZ (×2), **__Host-GAPS**, __Secure-BUCKET.

## Auto-Renewal

Tanto `bot_scheduler.py` quanto `healthcheck.py` tentam nesta ordem:

1. `google_login_firefox_stdin.py` (Firefox)
2. `google_login_stdin.py` (Chrome, fallback)

Se nenhum funcionar, o admin é notificado via Telegram.

## Verificação

```bash
cd /opt/vooindo
.venv/bin/python3 check_google_session.py
# Esperado: Score: 3/3 ✅ Sessão Google válida
```

## App Password

```
rcwv jvmu yyyx okto
```

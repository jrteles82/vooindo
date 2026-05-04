# 🔐 Fluxo de Autenticação Google — Vooindo

## Visão Geral

O bot usa uma sessão persistente do Google Chrome para acessar o Google Flights. A autenticação é feita via Chrome real (não Playwright headless) para evitar detecção de automação.

## Como a Sessão é Armazenada

- **Diretório base:** `/opt/vooindo/google_session/`
- **Cookies:** `/opt/vooindo/google_session/Default/Cookies` (SQLite)
- **Storage state (Playwright):** `/opt/vooindo/google_session/storage_state.json`
- **Workers (cópias):** `google_session_pool_0..N` e `google_session_2..N`

## Cookies de Autenticação

O Google atual mudou o formato. Hoje o principal indicador de login é:

| Cookie | Domínio | O que indica |
|--------|---------|-------------|
| `__Host-GAPS` | `accounts.google.com` | Sessão autenticada (expira ~1 ano) |
| `SAPISID` | `.google.com` | Sessão antiga (pode não existir) |
| `APISID`, `HSID`, `SSID`, `SID` | `.google.com` | Sessão legado |

**Score:**
- `__Host-GAPS` presente → **3/3** (logado)
- >= 3 cookies legado → **3/3**
- >= 1 cookie legado → **1/3** (parcial)
- Nenhum → **0/3** (não logado)

## Scripts Envolvidos

### `google_login_subprocess.py`
**Usado pelo bot do Telegram** quando o admin clica "Renovar Sessão".

**Fluxo:**
1. Prepara ambiente (mata Xvfb zumbi, sobe novo)
2. Faz backup da sessão atual → `google_session_bkp/`
3. Remove `Cookies` e `Login Data` para forçar login fresco
4. Abre `google-chrome` (sem headless) no Xvfb (`:99`)
5. Usa `xdotool` para digitar email e senha automaticamente
6. Aguarda confirmação 2FA no celular do admin
7. Se detectar "Login confirmed", fecha Chrome
8. Lê cookies SQLite → calcula score
9. Se score >= 3, sincroniza sessão para todos os workers
10. Retorna `STATUS:AUTH_SCORE:N` pelo stdout

**Comunicação com o bot:** via stdout (protocolo `STATUS:*`) + arquivo `/opt/vooindo/logs/login_result.json`

### `force_google_login.py`
**Script manual** para login interativo via SSH.

**Diferença:** não usa xdotool — você digita senha e código 2FA manualmente no terminal.

### `google_login_stdin.py`
Alternativa com Playwright — **não usado atualmente** porque o fluxo do Google detecta automação.

### `google_session_sync.py`
Sincroniza a sessão base (`google_session/`) para todos os workers (`google_session_pool_*`, `google_session_2..7`).

## Infraestrutura

- **Xvfb:** display virtual `:99` para renderizar o Chrome
- **xdotool:** automatiza cliques e digitação na janela do Chrome
- **google-chrome:** Chromium estável, sem flags de automação (exceto `--no-sandbox`)

## Problemas Conhecidos

1. **Chrome zumbi:** às vezes o processo não fecha completamente e o SingletonLock impede nova abertura
   - **Auto-fix:** o script mata processos Chrome e Xvfb antes de começar
2. **check_auth_score desatualizado:** o Google mudou os cookies de SAPISID → `__Host-GAPS`
   - **Corrigido** em mai/2026
3. **Xvfb lock file:** `/tmp/.X99-lock` pode travar se o Xvfb crashar
   - **Limpeza manual:** `rm -f /tmp/.X99-lock /tmp/.X11-unix/X99`

---

# ✅ Verificação de Sessão Pré-Rodada

## O Quê

Verificar se a sessão Google está com score 3/3:
- **15 minutos antes de cada rodada agendada**
- **Em toda rodada manual** (acionada pelo admin)

## Como Fazer

### No código (check_session_before_round.py)

```python
import sqlite3
from pathlib import Path

COOKIES_DB = Path('/opt/vooindo/google_session/Default/Cookies')
REQUIRED_COOKIE = '__Host-GAPS'  # indicador de login Google

def check_session_score() -> int:
    """Retorna 0-3. 3 = autenticado."""
    if not COOKIES_DB.exists():
        return 0
    conn = sqlite3.connect(str(COOKIES_DB))
    cur = conn.cursor()
    cur.execute("SELECT name FROM cookies WHERE name = ?", (REQUIRED_COOKIE,))
    has_gaps = cur.fetchone() is not None
    cur.execute("SELECT name FROM cookies WHERE name IN ('SAPISID','APISID','HSID','SSID','SID','OSID')")
    legacy = len(cur.fetchall())
    conn.close()
    if has_gaps:
        return 3
    if legacy >= 3:
        return 3
    if legacy >= 1:
        return 1
    return 0
```

### Integração no scheduler

No arquivo que dispara as rodadas (`bot_scheduler.py` ou similar), **antes de iniciar**:

1. Chamar `check_session_score()`
2. Se score < 3:
   - Logar aviso
   - Enviar mensagem para o admin Telegram:
     ```
     ⚠️ Sessão Google com score {score}/3
     A rodada pode falhar. Renove a sessão:
     /renovar_sessao
     ```
3. Se score >= 3: prosseguir normalmente

### Para rodadas manuais

No handler do bot que inicia scan manual, adicionar o mesmo check antes de executar.

### Onde colocar a notificação

O admin Telegram notificado via `bot.send_message(admin_chat_id, texto)`.

### IDs de admin

```python
ADMIN_IDS = [1748352987]  # Teles
```

## Resumo do Fluxo

```
Pré-rodada (15min antes) ou Scan manual
        │
        ▼
check_session_score()
        │
    ├── score >= 3 ✅ → segue rodada
    │
    └── score < 3 ❌ → notifica admin:
                        "⚠️ Sessão Google score {N}/3.
                         Renove: /renovar_sessao"
```

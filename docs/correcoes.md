# 📋 Correções Aplicadas — Vooindo

## 04/05/2026

---

### 1. 🔐 Sessão Google — check_auth_score desatualizado

**Problema:** O login Google dava score 0/3 mesmo após confirmação no celular.

**Causa:** O Google mudou o cookie de autenticação de `SAPISID`/`APISID`/`HSID`/`SSID`/`SID`/`OSID` para `__Host-GAPS`. O `check_auth_score()` só procurava pelos cookies antigos.

**Correção:** Adicionado `__Host-GAPS` à lista de cookies reconhecidos. Se presente, retorna score 3/3.

**Arquivo:** `google_login_subprocess.py` — função `check_auth_score()`

```python
# Antes:
cur.execute("... WHERE name IN ('SAPISID','APISID','HSID','SSID','SID','OSID')")

# Depois:
cur.execute("... WHERE name IN ('SAPISID','APISID','HSID','SSID','SID','OSID','__Host-GAPS')")
if '__Host-GAPS' in names:
    return 3
```

---

### 2. ✅ Verificação de Sessão Pré-Rodada

**Problema:** Rodadas podiam começar com sessão Google inválida (score < 3), resultando em erros ou timeouts.

**Solução:** Criado `check_google_session.py` + integração no scheduler e scans manuais.

**Arquivos criados:**
- `check_google_session.py` — Script que lê cookies SQLite e retorna score 0-3.
  - `--notify`: envia alerta no Telegram se score < 3

**Arquivos modificados:**
- `bot_scheduler.py`:
  - `sleep_until_next_slot()` aceita `check_session=True` — verifica sessão 15 min antes do slot
  - Chamado no primeiro ciclo do scheduler
- `bot.py`:
  - `agora()` (scan manual) verifica sessão antes de criar job

---

### 3. 📄 Documentação do Fluxo de Autenticação

**Arquivo criado:** `docs/google_auth_flow.md`

Contém:
- Visão geral do fluxo de login Google
- Cookies e scores
- Scripts envolvidos (`google_login_subprocess.py`, `force_google_login.py`, `google_login_stdin.py`, `google_session_sync.py`)
- Infraestrutura (Xvfb, xdotool, Chrome)
- Problemas conhecidos e auto-fixes
- Fluxo de verificação pré-rodada

---

### 4. 🗑️ Deduplicação de Resultados de Voo

**Problema:** Usuários recebiam links duplicados para o mesmo trecho/data (ex: PVH→FOR em 04/06 aparecia 2x).

**Causa:** `_merge_rows_for_combined_result_view()` era um no-op (só retornava rows), e o Google retorna variações de booking_url para o mesmo voo.

**Correção:** `_merge_rows_for_combined_result_view()` agora agrupa e deduplica por:
- Origem + Destino + Data ida + Data volta + Cia aérea + Número do voo

Entre duplicatas, mantém o **menor preço**.

**Arquivo:** `main.py` — função `_merge_rows_for_combined_result_view()`

```python
# Antes:
def _merge_rows_for_combined_result_view(rows):
    return rows

# Depois:
def _merge_rows_for_combined_result_view(rows):
    seen = {}
    for row in rows:
        key = (origin, destination, outbound_date, inbound_date, airline, flight_number)
        if key in seen:
            # mantém menor preço
        else:
            seen[key] = row
    return list(seen.values())
```

---

### 5. 🧹 Arquivos Criados/Modificados (Resumo)

| Arquivo | Ação | Descrição |
|---------|------|-----------|
| `check_google_session.py` | ✨ Novo | Verifica score da sessão Google |
| `docs/google_auth_flow.md` | ✨ Novo | Documentação do fluxo de autenticação |
| `docs/correcoes.md` | ✨ Novo | Este arquivo |
| `bot_scheduler.py` | 🔧 Modificado | Verificação de sessão 15 min antes da rodada |
| `bot.py` | 🔧 Modificado | Verificação em scans manuais + import `Path` |
| `google_login_subprocess.py` | 🔧 Modificado | `check_auth_score()` reconhece `__Host-GAPS` |
| `main.py` | 🔧 Modificado | `_merge_rows_for_combined_result_view()` deduplica |

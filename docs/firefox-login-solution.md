# Firefox Login Solution — Descontinuado

## TL;DR

Firefox Playwright **também** é barrado no `signin/rejected`. A conta `vooindo.bot@gmail.com` está na lista negra do Google e não aceita automação (Playwright Chrome ou Firefox).

O `__Host-GAPS` obtido da página de erro é um cookie de *challenge*, não de sessão real. Falso positivo detectado após verificação com `check_session_health()`.

**Única solução funcional:** renovação manual com Chrome headful real (`/renovar_sessao` via bot, ou login manual no navegador com Xvfb).

## Histórico do Problema

| Tentativa | Resultado |
|-----------|-----------|
| Chrome Playwright headless | `signin/rejected` |
| Firefox Playwright headless=False + Xvfb | `signin/rejected` (mesma página) |
| Chrome Playwright headful + Xvfb + perfil persistente | Funcionou na última renovação manual |

O Google está bloqueando **automação programática**, não o navegador em si. A automação manual (humano interagindo com Chrome headful) funciona porque o Google não detecta o comportamento como robótico.

## Diagnóstico

- `check_session_health()` → score 0-1/3 (login prompt visível)
- `check_google_session.py` SQLite → 3/3 (falso positivo — cookie no DB mas não reconhecido)
- `check_google_session.py` com Playwright → 1/3 (real)

## Scripts

| Arquivo | Status | Função |
|---------|--------|--------|
| `google_login_firefox_stdin.py` | ❌ Bloqueado | Tentativa Firefox, detecta rejected e falha |
| `google_login_stdin.py` | ❌ Bloqueado | Tentativa Chrome, detecta rejected e falha |
| `google_login_firefox.py` | ❌ Bloqueado | Versão interativa do Firefox |
| `check_google_session.py` | ✅ Corrigido | Verificação dupla (SQLite + Playwright real) |

## Auto-Renewal

O cascade em `bot_scheduler.py` e `healthcheck.py` ainda tenta Firefox → Chrome no auto-renewal, mas ambos falham com `signin/rejected`. A notificação de sessão inválida é enviada ao admin via Telegram.

## Próximos passos possíveis

1. **Login manual periódico** (`/renovar_sessao`) — funciona, tolerância de ~12h antes da sessão expirar
2. **Undetected Chromedriver** — biblioteca que patcha o Chrome pra evitar detecção
3. **Selenium com perfil real** — alternativa ao Playwright
4. **Curl + cookies copiados manualmente** — baixa tecnologia, confiável

## App Password

```
Vooindo#8212
```

# Vooindo API — Endpoints para App

Base URL: `https://api.vooindo.com/v1`
Auth: Bearer Token (JWT obtido via login)

---

## 🔐 Autenticação

### POST /auth/login
Login via Telegram WebApp initData ou email/senha.
```json
// Request
{ "telegram_init_data": "query_id=...&hash=..." }
// ou
{ "email": "user@email.com", "password": "..." }

// Response 200
{ "token": "jwt...", "user": { "id": 2, "name": "Jr Teles", "role": "user" } }
```

### POST /auth/verify
Verifica se token é válido.
```json
// Response 200
{ "valid": true, "user": { "id": 2, "name": "Jr Teles", "role": "user" } }
```

---

## 👤 Usuário (User)

### GET /user/profile
Perfil do usuário logado.
```json
// Response 200
{
  "id": 2,
  "name": "Jr Teles",
  "username": "@telesjr",
  "plan": "free",
  "confirmed": true,
  "blocked": false,
  "created_at": "2026-04-15T10:30:00"
}
```

### GET /user/routes
Rotas ativas do usuário.
```json
// Response 200
{
  "routes": [
    {
      "id": 56,
      "origin": "AEP",
      "destination": "PVH",
      "outbound_date": "2026-06-15",
      "inbound_date": null,
      "active": true
    },
    {
      "id": 99,
      "origin": "PVH",
      "destination": "MIA",
      "outbound_date": "2026-11-01",
      "inbound_date": null,
      "active": true
    }
  ]
}
```

### POST /user/routes
Adicionar nova rota.
```json
// Request
{
  "origin": "PVH",
  "destination": "FOR",
  "outbound_date": "2026-08-15",
  "inbound_date": null
}

// Response 201
{ "id": 121, "status": "created" }
```

### DELETE /user/routes/:id
Remover rota.
```json
// Response 200
{ "status": "deleted" }
```

### PUT /user/routes/:id
Ativar/desativar rota.
```json
// Request
{ "active": false }

// Response 200
{ "id": 56, "active": false }
```

### GET /user/settings
Configurações do usuário.
```json
// Response 200
{
  "enable_google_flights": true,
  "max_price": null,
  "airline_filters": ["LATAM", "Gol", "Azul"],
  "alerts_enabled": true,
  "scan_interval_minutes": 60
}
```

### PUT /user/settings
Atualizar configurações.
```json
// Request
{
  "max_price": 2000.0,
  "enable_google_flights": true,
  "airline_filters": ["LATAM", "Gol"],
  "alerts_enabled": true
}

// Response 200
{ "status": "updated" }
```

---

## 🔍 Consulta de Voos

### POST /scan
Executar scan de todas as rotas do usuário (equivalente ao `/agora`).
```json
// Request (opcional)
{ "airline_filters": ["LATAM"] }

// Response 200
{
  "status": "ok",
  "summary": "ok: 2/2 exibidos",
  "results": [
    {
      "origin": "AEP",
      "destination": "PVH",
      "outbound_date": "2026-06-15",
      "price": 1480.00,
      "currency": "BRL",
      "airline": "LATAM",
      "airline_url": "https://www.google.com/travel/flights/booking?tfs=...",
      "price_band": "normal",
      "price_insight": "Os preços estão normais",
      "best_airline_price": 1480.00,
      "best_agency_price": null,
      "vendor": "LATAM",
      "notes": "..."
    }
  ]
}
```

### POST /scan/manual
Consulta manual de voo único (equivalente ao `/manual`).
```json
// Request
{
  "origin": "PVH",
  "destination": "GRU",
  "outbound_date": "2026-08-15",
  "inbound_date": null
}

// Response 200
{
  "origin": "PVH",
  "destination": "GRU",
  "outbound_date": "2026-08-15",
  "results": [ { "price": 890.00, "airline": "Gol", ... } ]
}
```

### GET /scan/status/:run_id
Status de um scan em andamento.
```json
// Response 200
{ "run_id": 42, "status": "running", "progress": "2/4 routes" }
// ou
{ "run_id": 42, "status": "completed", "summary": "ok: 4/4 exibidos" }
```

### GET /scan/history
Histórico de scans do usuário.
```json
// Response 200
{
  "history": [
    {
      "run_id": 42,
      "started_at": "2026-05-06T11:00:03",
      "finished_at": "2026-05-06T11:01:45",
      "status": "completed",
      "summary": "ok: 4/4",
      "new_lowest_price": false
    }
  ]
}
```

### GET /scan/history/:run_id/results
Resultados detalhados de um scan específico.
```json
// Response 200
{
  "run_id": 42,
  "started_at": "2026-05-06T11:00:03",
  "results": [ { "origin": "PVH", "destination": "FOR", "price": 1329.00, ... } ]
}
```

---

## 🔔 Alertas & Notificações

### PUT /user/settings/alerts
Alternar alertas de preço.
```json
// Request
{ "alerts_enabled": false }

// Response 200
{ "alerts_enabled": false }
```

### GET /user/notifications
Últimas notificações.
```json
// Response 200
{
  "notifications": [
    {
      "id": 1001,
      "type": "price_drop",
      "message": "PVH → FOR caiu para R$ 1.329",
      "created_at": "2026-05-06T11:00:00",
      "read": false
    }
  ]
}
```

### PUT /user/notifications/:id/read
Marcar notificação como lida.

---

## 🏷️ Planos & Pagamento

### GET /user/plan
Detalhes do plano atual.
```json
// Response 200
{
  "plan": "free",
  "scans_remaining": 5,
  "scans_used_today": 0,
  "expires_at": null,
  "features": {
    "routes": 3,
    "scans_per_day": 5,
    "airline_filters": true,
    "alerts_interval": 60
  }
}
```

### GET /plans
Planos disponíveis.
```json
// Response 200
{
  "plans": [
    { "id": "basic", "name": "Básico", "price_monthly": 9.90, "features": {...} },
    { "id": "pro", "name": "Pro", "price_monthly": 29.90, "features": {...} }
  ]
}
```

### POST /user/plan/upgrade
Fazer upgrade de plano.
```json
// Request
{ "plan_id": "pro", "payment_method": "pix" }

// Response 200
{ "status": "pending", "payment_url": "...", "pix_code": "000201..." }
```

### GET /user/payments
Histórico de pagamentos.
```json
// Response 200
{ "payments": [{ "id": 1, "plan": "pro", "amount": 29.90, "status": "paid", "date": "..." }] }
```

---

## 💬 Suporte

### GET /support/conversations
Listar conversas do usuário.
```json
// Response 200
{
  "conversations": [
    {
      "thread_id": 1,
      "last_message": "Obrigado pelo contato!",
      "updated_at": "2026-05-06T10:00:00",
      "unread": 0
    }
  ]
}
```

### GET /support/conversations/:thread_id/messages
Mensagens de uma conversa.
```json
// Response 200
{
  "messages": [
    { "id": 1, "text": "Olá, preciso de ajuda", "from_user": true, "created_at": "..." },
    { "id": 2, "text": "Como posso ajudar?", "from_user": false, "created_at": "..." }
  ]
}
```

### POST /support/message
Enviar mensagem de suporte.
```json
// Request
{ "text": "Meu scan não está funcionando" }

// Response 201
{ "id": 3, "status": "sent" }
```

---

## 🛡️ Admin

### GET /admin/dashboard
Métricas do sistema (exige role=admin).
```json
// Response 200
{
  "total_users": 27,
  "active_users_today": 15,
  "scans_today": 141,
  "errors_today": 0,
  "service_uptime": "2d 9h",
  "load_average": 0.46,
  "memory_used_mb": 3072,
  "memory_total_mb": 7939,
  "chrome_guardian": { "ready": true, "session_ok": true },
  "last_round": { "time": "2026-05-06T11:00:00", "jobs": 23, "status": "ok" }
}
```

### GET /admin/users
Listar usuários (paginado).
```json
// Query params: ?page=1&per_page=20&search=Jr&status=active
// Response 200
{
  "users": [
    {
      "chat_id": "1748352987",
      "name": "Jr Teles",
      "username": "@telesjr",
      "plan": "free",
      "blocked": false,
      "routes_count": 2,
      "last_scan_at": "2026-05-06T11:00:03",
      "is_test": false
    }
  ],
  "total": 27,
  "page": 1,
  "total_pages": 2
}
```

### GET /admin/users/:chat_id
Detalhes de um usuário.
```json
// Response 200
{
  "chat_id": "1748352987",
  "name": "Jr Teles",
  "username": "@telesjr",
  "plan": "free",
  "blocked": false,
  "exempt_from_maintenance": false,
  "can_trigger_scan": true,
  "is_test_user": false,
  "created_at": "2026-04-15T10:30:00",
  "routes": [...],
  "last_scans": [...],
  "payments": [...]
}
```

### PUT /admin/users/:chat_id
Gerenciar usuário.
```json
// Request
{
  "blocked": false,
  "exempt_from_maintenance": false,
  "can_trigger_scan": true,
  "is_test_user": false
}
// Response 200
{ "status": "updated" }
```

### DELETE /admin/users/:chat_id
Deletar usuário.

### POST /admin/broadcast
Enviar mensagem para todos os usuários.
```json
// Request
{ "text": "Manutenção programada para hoje às 22:00" }
// Response 200
{ "sent_to": 27, "status": "ok" }
```

### POST /admin/session/renew
Forçar renovação da sessão Google.
```json
// Response 200
{ "status": "started", "message": "Sessão Google em renovação" }
```

### GET /admin/session/status
Status da sessão Google.
```json
// Response 200
{
  "ready": true,
  "session_ok": true,
  "instances": [ { "ready": true, "alive": true, "ws_endpoint": "ws://..." } ]
}
```

### GET /admin/system/status
Status completo do sistema.
```json
// Response 200
{
  "service": "active",
  "uptime": "2d 9h",
  "workers": 7,
  "chrome_guardian": "active",
  "cpu_load": 0.46,
  "memory_used_mb": 3072,
  "memory_available_mb": 4800,
  "db_connections": 5,
  "last_healthcheck": "2026-05-06T11:55:00",
  "healthcheck_ok": true
}
```

### POST /admin/system/restart
Reiniciar serviço (admin master).
```json
// Response 200
{ "status": "restarting" }
```

### GET /admin/metrics/cycles
Métricas de ciclos de scan.
```json
// Response 200
{
  "cycles": [
    {
      "round": "2026-05-06T11:00",
      "users": 12,
      "jobs": 23,
      "completed": 23,
      "errors": 0,
      "avg_duration_s": 116,
      "total_duration_s": 145
    }
  ]
}
```

---

## 🌐 Aeroportos (referência)

### GET /airports
Lista de aeroportos suportados.
```json
// Query params: ?search=porto
// Response 200
{
  "airports": [
    { "code": "PVH", "name": "Porto Velho", "city": "Porto Velho", "country": "BR" },
    { "code": "POA", "name": "Porto Alegre", "city": "Porto Alegre", "country": "BR" }
  ]
}
```

### GET /airports/:code
Detalhes de um aeroporto.
```json
// Response 200
{
  "code": "PVH",
  "name": "Governador Jorge Teixeira",
  "city": "Porto Velho",
  "country": "BR",
  "state": "RO"
}
```

---

## 📊 WebSocket (opcional)

### WS /ws/scan/:run_id
Acompanhar scan em tempo real.
```
→ { "type": "progress", "route": "PVH→FOR", "status": "searching" }
→ { "type": "progress", "route": "PVH→FOR", "status": "booking" }
→ { "type": "result", "route": "PVH→FOR", "price": 1329.00, "airline": "LATAM" }
→ { "type": "complete", "summary": "ok: 4/4" }
```

---

## Erros

```json
// 400 Bad Request
{ "error": "validation", "message": "origin é obrigatório", "fields": { "origin": "required" } }

// 401 Unauthorized
{ "error": "auth", "message": "Token inválido ou expirado" }

// 403 Forbidden
{ "error": "forbidden", "message": "Acesso admin necessário" }

// 404 Not Found
{ "error": "not_found", "message": "Rota não encontrada" }

// 429 Rate Limit
{ "error": "rate_limit", "message": "Muitas requisições. Tente em 30s", "retry_after": 30 }

// 500 Internal Error
{ "error": "internal", "message": "Erro interno do servidor" }
```

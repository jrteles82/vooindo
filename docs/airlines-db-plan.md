# Airlines DB Plan

Base inicial para arquitetura orientada a banco de dados das companhias aéreas.

## Tabela proposta

- `airlines`
  - `iata_code` - chave primária, ex: `LA`, `AR`
  - `name` - nome canônico da companhia
  - `is_active` - flag para monitoramento
  - `created_at`
  - `updated_at`

## Seed inicial

- `LA` - LATAM Airlines
- `AR` - Aerolineas Argentinas
- `G3` - Gol
- `AD` - Azul
- `FO` - Flybondi
- `WJ` - JetSmart
- `AV` - Avianca
- `DM` - Arajet

## Objetivo

Essa base prepara o projeto para:
- remover hardcodes de companhia no futuro
- ativar/desativar companhias via banco
- associar histórico de preço por companhia
- testar futuramente filtros por IATA no Google Flights com menor regressão

## Fora de escopo por enquanto

- integrar scraping ao catálogo `airlines`
- criar tabela relacional de `prices` por `iata_code`
- filtrar Google Flights por URL com IATA em produção

# ─────────────────────────────────────────────────────────────
#  PerGo — Makefile
#  Uso:
#    make dev        → hot-reload (carrega .env automaticamente)
#    make up         → build + sobe app, PostgreSQL e NATS via docker compose
#    make down       → para todos os serviços via docker compose (preserva volumes)
#    make prod       → alias de make up
#    make infra      → sobe só postgres + nats (sem o app)
#    make infra-down → derruba infra
#    make build      → compila o binário para ./bin/pergo
#    make generate   → regenera arquivos templ
#    make test       → testes rápidos
#    make test-race  → testes com race detector
#    make lint       → golangci-lint
# ─────────────────────────────────────────────────────────────

.PHONY: dev up down prod prod-logs prod-down infra infra-down build generate test test-race lint clean help

# Carrega variáveis do .env se ele existir (sem expor no shell pai)
ifneq (,$(wildcard .env))
  include .env
  export
endif

# Garante que ~/go/bin esteja no PATH para encontrar air e templ
export PATH := $(HOME)/go/bin:$(PATH)

BINARY        := ./bin/pergo
BUILD_FLAGS   := -ldflags="-s -w"

# ─── Dev ─────────────────────────────────────────────────────

## dev: hot-reload com air (reinicia automaticamente a cada mudança)
dev: _check-air _check-templ
	@echo "→ Iniciando em modo dev com hot-reload..."
	@air

## watch: alias para make dev
watch: dev

# ─── Produção ────────────────────────────────────────────────

## up: build da imagem e sobe PerGo, PostgreSQL e NATS via Docker Compose
up:
	@echo "→ Fazendo build e subindo PerGo + PostgreSQL + NATS via Docker Compose..."
	@docker compose up --build -d
	@echo "✓ Rodando em http://localhost:$${PERGO_HOST_PORT:-8080}"

## down: para todos os serviços via Docker Compose sem apagar volumes
down:
	@if [ -n "$$(docker compose ps -aq)" ]; then \
		echo "→ Parando PerGo, PostgreSQL e NATS (volumes preservados)..."; \
		docker compose down; \
	else \
		echo "✓ PerGo ya estaba detenido."; \
	fi

## prod: alias de make up
prod: up

## prod-logs: acompanha os logs do container em produção
prod-logs:
	@docker compose --env-file .env logs -f pergo

## prod-down: alias de make down
prod-down: down

# ─── Infra local (mesmo Docker Compose) ───────────────────────

## infra: sobe apenas PostgreSQL e NATS do Compose local
infra:
	@echo "→ Subindo PostgreSQL e NATS do Compose local..."
	@docker compose up -d postgres nats
	@echo "✓ Postgres (localhost:5433) | NATS (localhost:4222)"

## infra-down: para apenas PostgreSQL e NATS do Compose local
infra-down:
	@echo "→ Parando PostgreSQL e NATS do Compose local..."
	@docker compose stop postgres nats

# ─── Build ───────────────────────────────────────────────────

## build: compila o binário otimizado para ./bin/pergo
build: generate
	@echo "→ Compilando..."
	@mkdir -p bin
	@go build $(BUILD_FLAGS) -o $(BINARY) ./cmd/pergo
	@echo "✓ Binário em $(BINARY)"

## generate: regenera os arquivos Go a partir dos templates templ
generate: _check-templ
	@templ generate ./...

# ─── Qualidade ───────────────────────────────────────────────

## test: executa testes rápidos (sem race detector)
test:
	@go test ./... -short

## test-race: executa testes com race detector
test-race:
	@go test ./... -race -count=1

## lint: análise estática com golangci-lint
lint:
	@golangci-lint run

## clean: remove binários e arquivos temporários
clean:
	@rm -rf bin/ tmp/

# ─── Help ────────────────────────────────────────────────────

## help: lista todos os targets disponíveis
help:
	@echo ""
	@echo "  PerGo — comandos disponíveis:"
	@echo ""
	@grep -E '^## ' Makefile | sed 's/## /  make /' | column -t -s ':'
	@echo ""

# ─── Checks internos ─────────────────────────────────────────

_check-air:
	@which air > /dev/null 2>&1 || (echo "✗ 'air' não encontrado. Instale: go install github.com/air-verse/air@latest" && exit 1)

_check-templ:
	@which templ > /dev/null 2>&1 || (echo "✗ 'templ' não encontrado. Instale: go install github.com/a-h/templ/cmd/templ@latest" && exit 1)

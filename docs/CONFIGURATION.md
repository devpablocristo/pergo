# Configuração do Sistema (Environment Variables)

O **PerGo** segue o padrão de 12-factor app. Em staging e produção, a
configuração é validada antes de abrir conexões ou iniciar handlers; defaults
locais inseguros são rejeitados.

Abaixo está o mapeamento detalhado das variáveis de ambiente disponíveis.

---

## Ambiente e perfis de processo

| Variável | Valores | Regra |
|---|---|---|
| `PERGO_ENVIRONMENT` | `development`, `test`, `staging`, `production` (`dev`, `local`, `stg`, `prd` também aceitos) | Todo ambiente fora de development/test ativa validação estrita. |
| `PERGO_RUNTIME_PROFILE` | `all`, `api`, `webhook`, `worker`, `migrate` | `all` é exclusivamente local. O argumento `pergo <perfil>` tem precedência. |
| `PERGO_RUN_MIGRATIONS` | `true`/`false` | Só é permitido em runtime normal durante development/test. Produção usa `pergo migrate`. |
| `PERGO_MEDIA_MODE` | `disabled`, `memory` | `memory` é um fake local; fora de dev/test somente `disabled` é aceito. |

O perfil `migrate` não é um cliente somente de banco. Ele aplica as migrações
Goose, cria as partições de auditoria do mês atual e dos seis meses seguintes,
cifra e limpa linhas legadas da DLQ e faz bootstrap/reconciliação de streams e
consumers JetStream. Portanto, em staging/produção, o job precisa
simultaneamente de:

- `PERGO_DATABASE_URL` com TLS verificado ou socket Unix aprovado;
- `PERGO_KEK_BASE64`;
- `PERGO_NATS_URLS`, `PERGO_NATS_CREDS_FILE`, `PERGO_NATS_ACCOUNT` e
  `PERGO_NATS_STREAM_REPLICAS`;
- CA/Nome TLS do NATS quando aplicável;
- uma credencial NATS administrativa, exclusiva do job.

## NATS seguro e isolado

- `PERGO_NATS_URLS`: lista separada por vírgulas; `PERGO_NATS_URL` continua
  aceito como compatibilidade.
- `PERGO_NATS_CREDS_FILE`: arquivo `.creds`; obrigatório fora de dev/test.
- `PERGO_NATS_CA_FILE`: CA privada opcional.
- `PERGO_NATS_TLS_SERVER_NAME`: nome esperado no certificado.
- `PERGO_NATS_ACCOUNT`: identidade declarada da conta; obrigatória e distinta
  para STG/PRD.
- `PERGO_NATS_STREAM_REPLICAS`: `1` local, mínimo `3` em produção.
- `PERGO_NATS_ADOPT_DRAINED_LEGACY`: gate de uso único, permitido somente em
  `migrate`; exige que todos os streams legados estejam vazios antes de criar o
  guard e os recursos V2.

Fora de dev/test se rejeitam URLs sem `tls://`/`wss://`, credenciais embutidas
na URL e uma conta sem identidade. Além disso, um guard durável impede que a
mesma conta JetStream seja usada por dois ambientes.

`PERGO_NATS_ACCOUNT` é um rótulo de isolamento, não uma autorização. Monte um
arquivo `.creds` diferente em cada workload e aplique privilégio mínimo:

| Identidade | Permissões esperadas |
|---|---|
| `migrate` | Administrar somente os streams/consumers PerGo, ler o inventário legado e criar/verificar `PERGO_ENVIRONMENT_GUARD`. |
| `api` | Ler o guard e publicar nos subjects V2 necessários à API; sem criar, atualizar ou excluir streams/consumers. |
| `webhook` | Ler o guard e publicar eventos inbound/webhook V2; sem privilégios administrativos. |
| `worker` | Ler o guard e contratos de streams/consumers, fazer fetch/ack dos durables V2 e publicar os eventos V2 gerados pelo processamento; sem administrar recursos. |

Os runtimes falham no startup se os contratos de stream/consumer divergirem.
Corrija o drift executando o job `migrate` com a identidade administrativa;
nunca conceda permissão de reconciliação aos serviços de longa duração.
Para a primeira adoção dos streams V2, siga o
[runbook coordenado 038–042](runbooks/038-042-coordinated-rollout.md).

## Credenciais de webhook WABA

Cada conexão `whatsapp_cloud` deve armazenar seu próprio `phone_number_id`,
`verify_token` aleatório (mínimo 32 caracteres) e `app_secret` da aplicação
Meta. O GET de verificação aceita somente o token persistido; tokens derivados
do workspace são rejeitados. Todo POST deve incluir `X-Hub-Signature-256` e a
assinatura HMAC-SHA256 é verificada sobre os bytes brutos com o segredo da
conexão identificada pelo `phone_number_id`. O envelope está limitado a 2 MiB;
um corpo maior recebe `413` antes de lookup ou HMAC.

Conexões antigas devem receber ou rotacionar ambos os segredos mediante o job
offline `pergo rotate-waba-webhook-secrets`. Os valores são lidos de arquivos
montados pelo Secret Manager, nunca de argumentos, variáveis com o valor do
segredo, seed, UI ou logs:

| Variável do job | Conteúdo |
|---|---|
| `PERGO_ROTATE_WORKSPACE_ID` | UUID exato do tenant |
| `PERGO_ROTATE_CONNECTION_ID` | UUID exato da conexão `whatsapp_cloud` |
| `PERGO_ROTATE_APP_SECRET_FILE` | caminho do arquivo montado com `app_secret` |
| `PERGO_ROTATE_VERIFY_TOKEN_FILE` | caminho do arquivo montado com `verify_token` |

O job preserva token Meta, conta WABA, número de telefone e campos desconhecidos,
valida tenant/canal/revisão de credencial e substitui ambos os segredos em uma
única atualização cifrada.

---

## Variáveis do Servidor e Banco de Dados

### `PERGO_DATABASE_URL`

* **Descrição:** String de conexão (DSN) com o banco de dados PostgreSQL.
* **Padrão:** `postgres://postgres:postgres@localhost:5432/pergo?sslmode=disable`
* **Exemplo TCP de Produção:** `postgres://user:password@db.example.internal:5432/pergo_db?sslmode=verify-full&sslrootcert=/var/run/secrets/postgres/ca.crt`
* **Exemplo Cloud SQL por socket Unix:** `postgresql:///pergo_db?host=/cloudsql/project:region:instance`

Fora de development/test, conexões TCP exigem `sslmode=verify-full` e
`sslrootcert`; `sslmode=require` é rejeitado porque não autentica o hostname.
Como alternativa, são aceitos sockets sob `/cloudsql/` ou
`/var/run/postgresql/`. O arquivo de CA e o socket devem ser legíveis pelo
UID/GID `65532:65532` da imagem.

### `PERGO_NATS_URLS`

* **Descrição:** Endereço de conexão com o servidor NATS (com suporte a JetStream ativado).
* **Padrão:** `nats://localhost:4222`

### `PERGO_META_GRAPH_VERSION`

* **Descrição:** Versão auditada usada por todos os adapters Meta.
* **Valor aceito:** `v25.0`.
* **Regra:** qualquer outro valor falha na validação. Atualizar a versão exige
  revisão coordenada dos adapters e testes; não faça override durante um deploy.

### `PERGO_SERVER_PORT`

* **Descrição:** Porta TCP onde o servidor HTTP do PerGo (Console Admin + API pública + Webhooks) irá escutar.
* **Padrão:** `8080`

### `PERGO_DEBUG_PORT`

* **Descrição:** Porta TCP exclusiva para endpoints de profiling (`pprof`) e monitoramento/métricas. Fica isolada por questões de segurança.
* **Padrão:** `6060`

### `PERGO_EXTERNAL_URL`

* **Descrição:** A URL externa pública segura através da qual o seu servidor PerGo é acessado pela internet. Essencial para o registro automático de webhooks (Telegram/Meta).
* **Padrão:** `http://localhost:8080`
* **Exemplo de Produção:** `https://api.pergo.app`

### `PERGO_WHATSAPP_MOCK_ENABLED`

* **Descrição:** Habilita o canal local `whatsapp_mock` para testar a API, o JetStream, os workers e a auditoria sem acessar WhatsApp, Meta ou qualquer conta externa.
* **Padrão:** `false`
* **Ativação:** apenas o valor exato `true` habilita o canal. Mantenha desabilitado em produção.

---

## Segurança e Painel Administrativo

### `PERGO_KEK_BASE64`

* **Descrição:** Chave Mestra de Criptografia (Key Encryption Key) no formato **Base64**. Deve corresponder a uma chave de exatamente 32 bytes (256 bits) para ser utilizada no algoritmo AES-256-GCM. Usada para criptografar credenciais, sessões e os campos sensíveis da DLQ de webhooks.
* **Padrão:** *(Sem padrão — obrigatório definir em produção)*
* **Como gerar:**

  ```bash
  openssl rand -base64 32
  ```

  Staging e produção rejeitam chaves nulas, de byte repetido, frases ASCII, a
  chave de desenvolvimento e vetores sequenciais públicos. O valor deve ser
  gerado aleatoriamente e armazenado no Secret Manager.

### `PERGO_ADMIN_PASSWORD`

* **Descrição:** Senha de acesso para a console de gerenciamento (`/admin`).
* **Padrão:** `pergo-dev-2026`
* **Produção:** obrigatório no perfil `api`, com pelo menos 16 caracteres e sem
  valores conhecidos de desenvolvimento.

### `PERGO_SESSION_SECRET`

* **Descrição:** segredo HMAC estável dos cookies administrativos.
* **Desenvolvimento/teste:** se omitido, é gerado aleatoriamente no boot e as
  sessões deixam de ser válidas após reinício.
* **Staging/produção:** obrigatório no perfil `api`, com pelo menos 32
  caracteres; deve ser aleatório, estável e armazenado no Secret Manager.

---

## Mídias e anexos

Este build não contém storage de mídia apto para produção. Fora de
development/test, `PERGO_MEDIA_MODE=disabled` é obrigatório e as variáveis
`PERGO_S3_*`/`S3_*` não habilitam um adapter produtivo. `memory` usa somente o
fake local.

Mensagens de texto continuam operando. Requisições outbound com mídia falham
explicitamente; webhooks WABA com anexos respondem `503` e `Retry-After: 300`
antes de buscar o arquivo na Meta, para que o provedor possa reentregar. Não
habilite tráfego produtivo com anexos até existir storage durável e uma revisão
específica desse adapter.

---

## Tabela Geral de Variáveis de Ambiente

Abaixo está o mapeamento unificado de todas as variáveis de ambiente aceitas pelo PerGo, seus valores padrão e se são obrigatórias ou opcionais.

| Variável | Alternativa | Default local | Obrigatória em deploy? | Regra |
|---|---|---|---|---|
| `PERGO_ENVIRONMENT` | - | `development` | Sim | Use `staging` ou `production`; aliases `stg`/`prd` são aceitos. |
| `PERGO_RUNTIME_PROFILE` | argumento CLI | `all` | Sim | `all` é proibido fora de development/test. |
| `PERGO_RUN_MIGRATIONS` | - | `true` em dev, `false` fora | Não | Em deploy normal deve permanecer `false`; use o perfil `migrate`. |
| `PERGO_DATABASE_URL` | - | DSN local com `sslmode=disable` | Sim | TCP: `verify-full` + `sslrootcert`; ou socket Unix aprovado. |
| `PERGO_NATS_URLS` | `PERGO_NATS_URL` | `nats://localhost:4222` | Sim | Lista separada por vírgulas; somente `tls://`/`wss://` fora de dev/test. |
| `PERGO_NATS_CREDS_FILE` | - | vazio | Sim | Arquivo `.creds` distinto por papel; admin somente no job `migrate`. |
| `PERGO_NATS_CA_FILE` | - | vazio | Conforme PKI | CA privada adicional. |
| `PERGO_NATS_TLS_SERVER_NAME` | - | vazio | Conforme endpoint | Nome esperado no certificado NATS. |
| `PERGO_NATS_ACCOUNT` | - | vazio | Sim | Rótulo da conta/ambiente usado pelo guard; não substitui ACL. |
| `PERGO_NATS_STREAM_REPLICAS` | - | `1` | Sim | Mínimo `3` em produção. |
| `PERGO_NATS_ADOPT_DRAINED_LEGACY` | - | `false` | Só na primeira migração V2 | Gate de uso único e apenas no perfil `migrate`. |
| `PERGO_META_GRAPH_VERSION` | - | `v25.0` | Defina explicitamente | Somente `v25.0` é aceito neste build. |
| `PERGO_KEK_BASE64` | - | vazio | Sim | Deve decodificar exatamente 32 bytes aleatórios. |
| `PERGO_ADMIN_PASSWORD` | - | `pergo-dev-2026` | Perfil `api` | Mínimo 16 caracteres; default local é rejeitado. |
| `PERGO_SESSION_SECRET` | - | gerado no boot | Perfil `api` | Segredo estável de pelo menos 32 caracteres. |
| `PERGO_MEDIA_MODE` | - | `memory` em dev, `disabled` fora | Sim | Somente `disabled` é aceito em deploy. |
| `PERGO_SERVER_PORT` | - | `8080` | Não | Porta HTTP dos perfis `api` e `webhook`. |
| `PERGO_DEBUG_PORT` | - | `6060` | Não | Escuta somente em `127.0.0.1`; não publique externamente. |
| `PERGO_EXTERNAL_URL` | - | `http://localhost:8080` | API/webhook/worker | Origem HTTPS absoluta, sem path, query, fragment ou userinfo. |
| `PERGO_MAX_WHATSAPP_CONNECTIONS` | - | `5` | Não | Limite local de WhatsApp Web; o canal não é suportado neste piloto produtivo. |
| `PERGO_WHATSAPP_MOCK_ENABLED` | - | `false` | Não | Deve ser `false` fora de development/test. |
| `PERGO_S3_*` | `S3_*` | vazio | Não | Compatibilidade local; não habilita mídia em staging/produção. |

---

## Fonte de verdade da configuração

A estrutura `Config`, seus defaults e a validação fail-closed estão em
[internal/config/config.go](../internal/config/config.go). O transporte NATS
está em [internal/platform/queue/connect.go](../internal/platform/queue/connect.go).
Esta documentação não autoriza relaxar essas validações por flags da plataforma.

---

## Configurações Obrigatórias vs Opcionais

### Configurações Obrigatórias

* **PostgreSQL (`PERGO_DATABASE_URL`)**: é o sistema de registro central.
* **NATS JetStream**: URL, credencial, conta e réplicas são obrigatórias fora
  de development/test, inclusive para `migrate`.
* **KEK (`PERGO_KEK_BASE64`)**: cifra credenciais, sessões e os campos sensíveis
  da DLQ de webhooks; é obrigatória fora de development/test, inclusive em
  `migrate`.
* **API administrativa**: `PERGO_ADMIN_PASSWORD` e
  `PERGO_SESSION_SECRET` são obrigatórios no perfil `api`.

### Configurações Opcionais

* **Portas (`PERGO_SERVER_PORT`, `PERGO_DEBUG_PORT`)**: podem manter os
  defaults quando a plataforma faz o roteamento correspondente.
* **CA e nome TLS NATS**: dependem da PKI usada, sem reduzir a obrigação de
  validar o certificado.
* **WhatsApp Web e simulador**: são recursos de development/test. Na topologia
  separada não há coordenador durável para pairing/QR/desconexão e não fazem
  parte do piloto produtivo.

---

## Sobrescritas e Configuração por Ambiente

O PerGo carrega variáveis do sistema operacional de forma direta para respeitar os princípios do *12-Factor App*. No entanto, o fluxo de sobrescrita e setup varia por ambiente:

### Ambiente de Desenvolvimento (Local)

Para facilitar o desenvolvimento, o projeto disponibiliza
[.env.example](../.env.example).

1. Copie o arquivo de exemplo para criar a sua configuração local:
   ```bash
   cp .env.example .env
   ```
2. Modifique os valores desejados. Por exemplo, a URL do banco PostgreSQL local ou credenciais do MinIO de desenvolvimento.
3. Carregue o arquivo no ambiente do seu terminal antes de rodar o PerGo:
   ```bash
   source .env && go run ./cmd/pergo
   ```

### Ambiente Docker / Docker Compose

No arquivo [docker-compose.yml](../docker-compose.yml), a configuração local é
estruturada com base nos containers.
O arquivo carrega as configurações mapeando variáveis de ambiente locais do host ou definindo valores default. Por exemplo, a URL do banco de dados aponta para o nome do serviço no compose (`postgres:5432`) ao invés de `localhost`.
Para personalizar as variáveis rodando via compose, é possível definir um arquivo `.env` na raiz do projeto, que será injetado automaticamente nos serviços do compose.

### Ambiente de Produção

Em produção, utilize a infraestrutura de orquestração (Kubernetes, AWS ECS, systemd, etc.) para injetar as variáveis de forma segura no processo:

* Mantenha a chave `PERGO_KEK_BASE64` guardada de forma segura (ex: AWS Secrets Manager, Vault) e nunca a exponha em commits de código.
* Use `sslmode=verify-full` e `sslrootcert` no PostgreSQL TCP, ou um dos sockets
  Unix aprovados; `sslmode=require` é insuficiente e rejeitado.
* Monte uma credencial NATS distinta por workload; somente o job `migrate`
  recebe a identidade administrativa.
* Fixe `PERGO_META_GRAPH_VERSION=v25.0` e
  `PERGO_MEDIA_MODE=disabled`.
* Bloqueie acessos externos à porta de debug `PERGO_DEBUG_PORT` (padrão `6060`), pois ela expõe rotas críticas `/debug/pprof/` de profiling de memória/CPU que podem ser exploradas para ataques de negação de serviço.

---

## Valores Padrão

Os defaults da tabela existem para development/test. Não formam uma
configuração de produção: a validação de staging/produção rejeita os defaults
locais e os recursos ausentes indicados como obrigatórios.

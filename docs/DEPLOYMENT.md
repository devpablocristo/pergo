# Guia de Implantação (Deployment Guide)

Este guia orienta a construção da imagem e a implantação do **PerGo** em
workloads separados. O Docker Compose do repositório é exclusivamente local.

---

## Topologia de Runtime para Cloud Run

A imagem contém um único binário, mas cada workload deve selecionar um perfil
explícito. Em `staging`/`production`, o perfil monolítico `all` é rejeitado:

| Workload | Comando | Responsabilidade |
|---|---|---|
| API privada/admin | `/app/pergo api` | API, console e publicação na fila |
| Webhooks públicos | `/app/pergo webhook` | callbacks dos provedores e health checks |
| Worker Pool | `/app/pergo worker` | mensagens, campanhas e entregas webhook |
| Job de migração/bootstrap | `/app/pergo migrate` | aplica migrações, mantém partições, cifra a DLQ e reconcilia JetStream |
| Job de segredos WABA | `/app/pergo rotate-waba-webhook-secrets` | rotação tenant-scoped e término |

O mesmo valor pode ser configurado em `PERGO_RUNTIME_PROFILE`. O argumento do
comando tem precedência. API, webhook e worker nunca aplicam migrações fora de
development/test. O job `migrate` deve concluir antes de promover os demais
workloads e precisa de PostgreSQL, da KEK e de uma credencial NATS
administrativa. Ele não recebe segredos de provedores.

Na topologia separada, WhatsApp Web não tem coordenador durável entre API e
worker: pairing, QR e desconexão são rejeitados no perfil API e as sessões não
são restauradas fora do ambiente de desenvolvimento. Não habilite WhatsApp Web
no piloto produtivo. WhatsApp Cloud (WABA) e Telegram não dependem dessa memória.

Todos os processos de longa duração respeitam `SIGTERM` com um orçamento global
de oito segundos. O perfil worker não abre uma porta HTTP; deve ser implantado
como Worker Pool, não como serviço Cloud Run tradicional.

### Política obrigatória de staging/produção

- `PERGO_ENVIRONMENT=staging|production`;
- KEK Base64 de exatamente 32 bytes, sem fallback;
- KEK gerada aleatoriamente; zeros, bytes repetidos, frases, defaults de
  desenvolvimento e vetores públicos são rejeitados;
- PostgreSQL TCP com `sslmode=verify-full` e `sslrootcert`, ou socket Unix sob
  `/cloudsql/` ou `/var/run/postgresql/`;
- NATS por `tls://`/`wss://`, credenciais em arquivo e CA explícita quando privada;
- `PERGO_NATS_ACCOUNT` diferente por ambiente;
- `PERGO_NATS_STREAM_REPLICAS>=3` em produção;
- identidade NATS distinta para `api`, `webhook`, `worker` e `migrate`; somente
  `migrate` pode administrar streams e consumers;
- `PERGO_SESSION_SECRET` aleatório, estável e com pelo menos 32 caracteres no
  perfil `api`;
- `PERGO_META_GRAPH_VERSION=v25.0`;
- segredo de aplicação Meta e verify token aleatório por conexão WABA;
- `PERGO_MEDIA_MODE=disabled` neste build.

O marcador `PERGO_ENVIRONMENT_GUARD` em JetStream impede que dois ambientes
reivindiquem acidentalmente a mesma conta. Se uma conta já contiver streams
PerGo sem marcador, o bootstrap falha: um operador deve identificar o ambiente
antes de criar o guard. Nunca apague streams como forma de contornar esse
bloqueio. O modo `memory` de mídia usa um fake local e é rejeitado fora de
development/test; mídia real fica desabilitada até um adapter de storage
produtivo ser incorporado. Quando um webhook WABA contém anexo, o modo
desabilitado falha antes de buscar o arquivo na Meta e responde
`503 Retry-After: 300`. Não habilite tráfego produtivo com anexos.

### Adoção controlada do guard em uma conta NATS legada

Uma conta que contém `MESSAGES`, `WEBHOOKS`, `WEBHOOK_DELIVERIES`, `INBOUND` ou
`CAMPAIGNS`, mas ainda não possui `PERGO_ENVIRONMENT_GUARD`, exige implantação
coordenada. Não crie o guard manualmente e não apague backlog:

1. bloqueie novos ingressos;
2. mantenha somente os workers antigos até todos os streams legados chegarem a
   zero mensagens e zero pendências;
3. pare todos os processos, faça backup do PostgreSQL e snapshot/export do
   JetStream;
4. execute uma única vez o novo job `migrate` com
   `PERGO_NATS_ADOPT_DRAINED_LEGACY=true` e a credencial NATS administrativa;
5. remova a flag, verifique os contratos V2 e inicie primeiro o novo worker,
   depois webhook e API.

O gate falha se existir uma mensagem legada, se já houver um stream V2 sem
guard ou se a conta pertencer a outro ambiente. O procedimento completo,
incluindo validações de 038–042 e critérios de rollback, está no
[runbook de rollout coordenado 038–042](runbooks/038-042-coordinated-rollout.md).

### Credenciais NATS separadas por workload

Monte um `PERGO_NATS_CREDS_FILE` distinto em cada workload. Todos podem ler o
guard do ambiente, mas as autorizações devem permanecer separadas:

| Workload | Acesso NATS |
|---|---|
| `migrate` | Administração dos streams/consumers PerGo, inspeção dos streams legados e bootstrap do guard. |
| `api` | Publicação dos subjects V2 usados pela API; nenhum create/update/delete de recursos JetStream. |
| `webhook` | Publicação de inbound e eventos webhook V2; nenhum privilégio administrativo. |
| `worker` | Fetch/ack dos consumers V2, leitura de seus contratos e publicação dos eventos resultantes; nenhum privilégio administrativo. |

Os runtimes verificam o guard e, no caso do worker, fazem bind read-only aos
contratos já criados. Se o startup acusar drift, não amplie a ACL do runtime:
pare e execute novamente o job `migrate` com a identidade administrativa.

### Rotação ou backfill seguro de segredos WABA

Execute um job efêmero por conexão. Monte `app_secret` e `verify_token` a partir
do Secret Manager como arquivos separados e passe somente IDs e caminhos:

```bash
PERGO_ENVIRONMENT=production \
PERGO_DATABASE_URL='...' \
PERGO_KEK_BASE64='...' \
PERGO_ROTATE_WORKSPACE_ID='uuid-tenant' \
PERGO_ROTATE_CONNECTION_ID='uuid-conexion' \
PERGO_ROTATE_APP_SECRET_FILE='/var/run/secrets/meta-app-secret' \
PERGO_ROTATE_VERIFY_TOKEN_FILE='/var/run/secrets/meta-verify-token' \
/app/pergo rotate-waba-webhook-secrets
```

Não coloque os valores secretos em argumentos, variáveis de shell, imagens,
seed ou logs. O job valida tenant, canal e revisão da credencial, preserva token
de acesso e identidade Meta e grava ambos os segredos cifrados. Repita por
conexão. Depois, teste o GET de verificação e um POST assinado antes de retirar
a versão anterior do segredo.

A imagem executa como UID/GID `65532:65532`. Arquivos montados (`nats.creds`,
CA PostgreSQL/NATS e segredos do job) devem pertencer a esse UID ou ser legíveis
pelo grupo `65532`, idealmente em modo `0400` ou `0440`. Nunca os incorpore à
imagem nem os torne world-writable.

---

## Estrutura da Imagem Docker

O PerGo utiliza um arquivo `Dockerfile` multi-stage otimizado para produção:

1. **Stage 1 (Builder):** Compila o código Go e gera os templates do `a-h/templ` a partir de uma imagem base do Go com Alpine.
2. **Stage 2 (Runtime):** Utiliza uma imagem minimalista e segura do Google (**Distroless Static**), contendo apenas o binário compilado e os certificados de CA para conexões HTTPS externas com as APIs do Telegram e Facebook. Isso reduz drasticamente a superfície de ataque e o tamanho final da imagem.

---

## Docker Compose: somente desenvolvimento local

O arquivo `docker-compose.yml` inclui três serviços com credenciais, transporte
e topologia deliberadamente inseguros para produção:

1. **`postgres`**: Banco de dados PostgreSQL 16 com volume persistente em `pgdata`.
2. **`nats`**: Broker NATS com suporte a JetStream ativado (`-js` e persistência de dados em `natsdata`).
3. **`pergo`**: O próprio container da aplicação compilando a partir do diretório local.

Ele fixa `PERGO_ENVIRONMENT=development`, perfil monolítico, migrações
automáticas, PostgreSQL sem TLS e NATS plaintext R1. Use apenas:

```bash
make up
make down
```

`make prod`, `make prod-logs` e `make prod-down` falham fechado para impedir que
esses defaults sejam confundidos com uma implantação. Produção deve executar a
imagem imutável como os quatro workloads descritos no início deste documento.

## Lista de Verificação (Checklist) para Produção

Antes de colocar o servidor em produção, injete as configurações não sensíveis
como variáveis e monte os segredos a partir do Secret Manager; não use um arquivo
`.env` no build, na imagem ou no runtime produtivo:

### 1. URL Externa Segura (HTTPS)

* **Variável:** `PERGO_EXTERNAL_URL`
* **Configuração:** Deve apontar para o domínio HTTPS público através do qual o servidor é acessível pela internet (ex: `https://api.pergo.meu-app.com`).
* **Importante:** Sem HTTPS e com domínios inválidos ou IPs locais, o registro automático de webhooks para o Telegram falhará e a Meta não validará a URL de callback do webhook para o WhatsApp Cloud.

### 2. Chave Mestra de Criptografia (KEK)

* **Variável:** `PERGO_KEK_BASE64`
* **Configuração:** Deve ser configurada com uma chave de criptografia de 32 bytes codificada em Base64.
* **Validação:** Chaves triviais, legíveis ou conhecidas de teste são rejeitadas no startup.
* **Importante:** Nunca modifique essa chave após o início do uso do PerGo, pois todas as credenciais de canais existentes no banco de dados ficarão ilegíveis e corrompidas.
* **Gerar nova chave:**
  ```bash
  openssl rand -base64 32
  ```

### 3. Senha Administrativa Forte

* **Variável:** `PERGO_ADMIN_PASSWORD`
* **Configuração:** Defina uma senha forte de administrador para o acesso ao painel administrativo. Não utilize a senha padrão `pergo-dev-2026`.

### 4. Segredo Estável de Sessão

* **Variável:** `PERGO_SESSION_SECRET`
* **Configuração:** No perfil `api`, use um valor aleatório e estável com pelo
  menos 32 caracteres. Não compartilhe o segredo com outros ambientes.

### 5. PostgreSQL Autenticado

* **Variável:** `PERGO_DATABASE_URL`
* **Configuração TCP:** use `sslmode=verify-full` e monte a CA indicada por
  `sslrootcert`. `sslmode=require` é rejeitado.
* **Alternativa:** use socket Unix em `/cloudsql/` ou
  `/var/run/postgresql/`.

### 6. Papéis NATS e Bootstrap

* Use `tls://`/`wss://`, credenciais em arquivo e ao menos três réplicas em
  produção.
* Monte uma credencial distinta em cada workload. Somente `migrate` recebe
  permissão de administração JetStream.
* Execute `migrate` antes dos runtimes e confirme que cada runtime faz bind
  read-only sem acusar drift.

### 7. Meta e Mídia

* Fixe `PERGO_META_GRAPH_VERSION=v25.0`; qualquer outro valor é rejeitado.
* Fixe `PERGO_MEDIA_MODE=disabled` e restrinja o piloto a texto. Não configure
  `PERGO_S3_*` esperando habilitar anexos neste build.

### 8. Isolamento da Porta de Debug

* O PerGo expõe endpoints de profiling e expvar na porta `6060` (configurada via `PERGO_DEBUG_PORT`).
* **Atenção:** Garanta que a porta `6060` **não esteja exposta** para a internet pública e fique restrita apenas a acessos de monitoramento interno da sua infraestrutura/VPN.

---

## Ambientes de Implantação (Deployment Targets)

O **PerGo** foi projetado para rodar como containers separados em um
orquestrador. Para Pymes, API e webhook são serviços Cloud Run, `worker` é um
Worker Pool e `migrate`/rotação são jobs. PostgreSQL e NATS são dependências
externas protegidas; o Compose monolítico não é um target de homologação ou
produção.

---

## Pipeline de Build (Build Pipeline)

O processo de build do PerGo é automatizado e encapsulado no
[Dockerfile](../Dockerfile) multi-stage, seguindo os seguintes passos:

1. **Geração de Código (Template compilation)**: A ferramenta `a-h/templ` é executada para compilar os templates HTML tipo-seguros localizados no diretório [templates/](../templates/) para código Go nativo (`templ generate ./...`).
2. **Resolução de Dependências**: As dependências declaradas no [go.mod](../go.mod) são baixadas (`go mod download`).
3. **Compilação Estática**: O binário do Go é compilado com a flag `CGO_ENABLED=0` e flags de otimização `-ldflags="-w -s"` para remover tabelas de símbolos e informações de debug, resultando em um binário estático e enxuto.
4. **Construção da Imagem de Runtime**: O binário compilado, os arquivos estáticos de assets ([static/](../static/)) e os certificados de CA atualizados do builder são copiados para a imagem final Distroless fixada por digest, executada como usuário não-root.

Você também pode realizar o build local do binário (fora de containers) usando o comando:
```bash
make build
```

---

## Configuração do Ambiente (Environment Setup)

Para configurar o ambiente de produção do zero:

1. **Provisionar Infraestrutura de Apoio**: Suba uma instância segura do PostgreSQL (versão 15+) e um servidor NATS configurado com suporte a JetStream (com opções de persistência ativas).
2. **Criar Configurações**: Configure variáveis por workload e monte segredos
   a partir do gestor do ambiente. `.env.example` é somente uma referência local.
3. **Gerar Segredos**:
   - Defina a senha do administrador via `PERGO_ADMIN_PASSWORD`.
   - Gere e persista um segredo de sessão de pelo menos 32 caracteres via
     `PERGO_SESSION_SECRET`.
   - Gere uma chave de criptografia mestra segura de 32 bytes codificada em base64 usando:
     ```bash
     openssl rand -base64 32
     ```
     e salve-a na variável `PERGO_KEK_BASE64`.
   - Configure a URL externa pública estável e segura (`https`) em `PERGO_EXTERNAL_URL`.
4. **Separar os papéis NATS**: Monte credenciais não administrativas em API,
   webhook e worker e reserve a credencial administrativa para `migrate`.
5. **Executar migração e bootstrap**: Execute a mesma imagem como job único com
   `/app/pergo migrate`, fornecendo DB seguro, KEK e NATS administrativo. O job
   deve aplicar Goose, manter partições de auditoria, concluir o backfill
   cifrado da DLQ e reconciliar os recursos V2 antes de terminar.
6. **Promover os runtimes**: Inicie primeiro o worker e confirme seus binds;
   depois promova webhook e API. Em staging/produção esses runtimes rejeitam
   `PERGO_RUN_MIGRATIONS=true`.

### Manutenção mensal das partições de auditoria

`audit_logs` não possui partição default. Cada execução de `migrate` cria, sob
advisory lock, as partições do mês atual e dos seis meses seguintes. Agende o
mesmo job ao menos uma vez por mês (por exemplo, `0 3 1 * *` em UTC) com
exatamente as mesmas credenciais seguras de DB/KEK/NATS usadas no bootstrap.
A operação é idempotente e chamadas concorrentes são serializadas pelo lock.

Crie um alerta para qualquer execução não concluída do job e verifique que
existem sete partições mensais anexadas, cobrindo o mês atual até `+6`. Uma
falha não interrompe imediatamente as escritas enquanto o horizonte ainda
existe, mas deve ser corrigida antes que reste menos de um mês futuro. O job
também reconcilia JetStream de forma idempotente; hoje não existe um comando
separado somente para manutenção de partições.

---

## Procedimento de Recuperação e Reversão (Rollback Procedure)

As migrações 038–042 e a mudança para streams V2 **não permitem rollback
binário isolado**. Depois que `migrate` conclui, o procedimento padrão é
roll-forward:

- 038 troca unicidade global de trace pela chave por workspace; binários antigos
  usam um `ON CONFLICT` incompatível;
- 039 cifra payload, URL e razão de falha da DLQ. Depois que existir ciphertext,
  seu `Down` falha deliberadamente porque SQL não pode reconstruir plaintext;
- 040 introduz o outbox de receipts; removê-lo pode perder eventos ainda não
  publicados;
- 041 fixa idioma/shape de template em campanhas; removê-lo altera o protocolo
  de batches;
- 042 torna a identidade de deduplicação inbound específica por conexão; o
  `Down` recusa colapsar identidades repetidas entre conexões;
- mensagens nos streams `PERGO_V2_*` não são consumidas pelo binário legado.

Se houver falha após o job:

1. mantenha produtores públicos bloqueados e preserve a KEK;
2. não execute `goose down`, não remova colunas cifradas e não apague streams;
3. corrija a imagem e repita `migrate`, que é o caminho de recuperação
   idempotente;
4. se o roll-forward for impossível, restaure **conjuntamente** o backup
   PostgreSQL e os snapshots JetStream obtidos antes da janela, aceite o RPO
   correspondente e só então promova o binário anterior.

Restaurar apenas banco, apenas broker ou apenas imagem cria protocolos
incompatíveis. Consulte os critérios e comandos de verificação no
[runbook 038–042](runbooks/038-042-coordinated-rollout.md).

---

## Monitoramento (Monitoring)

O PerGo disponibiliza três mecanismos nativos para monitoramento de saúde e telemetria:

### 1. Probes de Saúde (Health & Readiness)

* **Liveness Probe (`GET /healthz`)**: Disponível na porta HTTP do servidor. Sempre retorna `200 OK` (corpo `ok`). Usado por orquestradores para verificar se o processo está respondendo.
* **Readiness Probe (`GET /readyz`)**: Disponível na porta HTTP do servidor. Executa conexões reais (Ping) ao banco de dados PostgreSQL e ao broker NATS JetStream. Retorna `200 OK` se ambos estiverem acessíveis, ou `503 Service Unavailable` em caso de falha de conexão.

### 2. Logs Estruturados

* Todos os logs da aplicação são direcionados para a saída padrão (`stdout`) estruturados em formato JSON usando o pacote nativo `log/slog`.
* As mensagens de logs incluem o atributo `trace_id` correlacionado de ponta a ponta na API HTTP e no processamento assíncrono em workers.

### 3. Métricas e Profiling Interno

* Expostos na porta de debug isolada `PERGO_DEBUG_PORT` (padrão `6060` no host `127.0.0.1`):
  * **/debug/vars**: Expõe métricas padrão de runtime via `expvar`.
  * **/debug/pprof/**: Fornece endpoints de profiling de CPU, memória, blocos e concorrência para análise detalhada e diagnóstico sob carga.

`/debug/vars` expõe `audit_drops`. O contador aumenta quando o buffer está
cheio, quando um lote aceito falha ao persistir ou quando o encerramento cancela
eventos ainda pendentes. Alerte em qualquer incremento e correlacione-o com os
logs `audit channel full`, `failed to flush audit batch` e
`audit writer shutdown cancelled`. Alerte também quando o job mensal
`migrate` falhar ou quando o horizonte cair abaixo de um mês futuro.

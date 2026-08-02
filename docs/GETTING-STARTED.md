# Como Começar (Getting Started)

Este guia ajuda você a configurar e executar o **PerGo** em seu ambiente local para fins de desenvolvimento e testes.

---

## Pré-requisitos

Certifique-se de ter os seguintes itens instalados na sua máquina:

* **Go 1.26+** (se preferir compilar localmente fora do Docker)
* **Docker** e **Docker Compose**
* **Air** (ferramenta de hot-reload para Go) — Instale com:
  ```bash
  go install github.com/air-verse/air@latest
  ```
* **Templ** (compilador de templates HTML para Go) — Instale com:
  ```bash
  go install github.com/a-h/templ/cmd/templ@latest
  ```

---

## 1. Clonar e Acessar o Repositório

Navegue até o diretório do projeto:
```bash
cd PerGo
```

---

## 2. Configurar Variáveis de Ambiente

Copie o arquivo de exemplo do ambiente e configure as chaves necessárias:
```bash
cp .env.example .env
```
Abra o arquivo `.env` gerado no seu editor de preferência e confira os valores. Para testes de desenvolvimento, os valores padrões são suficientes. Certifique-se de que a variável `PERGO_KEK_BASE64` contém uma chave válida Base64 de 32 bytes (gerada usando `openssl rand -base64 32`).

---

## 3. Subir Infraestrutura Local

O PerGo depende do banco de dados **PostgreSQL** e do broker **NATS** (com JetStream ativo). Suba estes serviços de forma isolada usando o Docker Compose local:
```bash
make infra
```
*Isto irá iniciar o banco em `localhost:5433` e o NATS em `localhost:4222`.*

---

## 4. Compilar e Executar em Desenvolvimento

Gere os templates de UI estáticos e inicie o servidor com hot-reload automático (ele recompila o projeto a cada alteração de arquivo):
```bash
make dev
```
Na primeira inicialização, o PerGo executará automaticamente as migrações
embutidas do banco de dados usando `goose`, preparando o schema local.

Esse comportamento depende de `PERGO_ENV=development`/`test` e
`PERGO_RUN_MIGRATIONS=true`; ele não se aplica aos workloads implantados.
Em staging/production, execute primeiro o job dedicado `pergo migrate` e só
depois promova `api`, `webhook` e `worker`. Consulte o
[guia de implantação](DEPLOYMENT.md).

---

## 5. Primeiro Acesso e Configuração

1. Abra o navegador em: **`http://localhost:8080/admin`**
2. Faça login informando a senha de administrador configurada em `PERGO_ADMIN_PASSWORD` (padrão de desenvolvimento: `pergo-dev-2026`).
3. Crie o seu primeiro **Workspace** (ex: "Empresa Principal" ou "Ambiente de Testes").
4. Clique no Workspace criado para acessar a tela de detalhes:
   * **Gerar API Key:** Clique em *Generate Key* e anote a chave gerada. Você usará essa chave no cabeçalho `Authorization: Bearer <key>` para enviar mensagens pela API REST do PerGo.
   * **Configurar Canais:** Adicione credenciais para Telegram Bot ou WhatsApp
     Cloud. O fluxo de QR Code do WhatsApp Web pode ser exercitado apenas neste
     ambiente de development/test; ele não é suportado nos perfis implantados.

---

## Problemas Comuns de Configuração (Common Setup Issues)

Abaixo estão listados alguns problemas frequentes durante a configuração inicial do ambiente e como resolvê-los:

1. **Falha de Conexão com o Banco de Dados (`dial tcp 127.0.0.1:5432` ou `5433: connect: connection refused`)**
   * **Causa:** O container do PostgreSQL não está em execução ou a porta
     especificada em `PERGO_DATABASE_URL` no seu arquivo `.env` está incorreta.
     Consulte [docker-compose.yml](../docker-compose.yml) para o mapeamento local.
   * **Solução:** Certifique-se de inicializar a infraestrutura utilizando `make infra`. No arquivo `.env`, garanta que a string de conexão está configurada para a porta do host correta (por padrão, `5433` em desenvolvimento local):
     ```env
     PERGO_DATABASE_URL=postgres://postgres:postgres@localhost:5433/pergo?sslmode=disable
     ```

2. **Falha de Conexão com o Broker NATS (`nats: connect: connection refused`)**
   * **Causa:** O broker do NATS com JetStream ativo não está rodando localmente
     ou a variável `PERGO_NATS_URLS` aponta para uma porta/endereço incorreto.
     `PERGO_NATS_URL` é apenas um alias legado para uso local.
   * **Solução:** Certifique-se de que os containers locais foram iniciados corretamente com o comando `make infra`. Verifique se o NATS está respondendo na porta padrão local `4222`.

3. **Comando `air` ou `templ` não encontrado ao rodar `make dev`**
   * **Causa:** As ferramentas de compilação e hot-reload não foram instaladas globalmente no Go ou o diretório de binários do Go (`GOBIN`) não está configurado na variável de ambiente `PATH` do seu terminal.
   * **Solução:** Primeiro, certifique-se de executar os comandos de instalação indicados na seção de pré-requisitos. Depois, adicione o caminho do seu workspace de Go ao `PATH` do sistema:
     ```bash
     export PATH=$PATH:$(go env GOPATH)/bin
     ```

4. **Erro de compilação dos arquivos `.templ`**
   * **Causa:** Erros de sintaxe ou arquivos autogerados inconsistentes na pasta `templates/`.
   * **Solução:** Tente rodar `make generate` diretamente para ver as mensagens de erro detalhadas do compilador do Templ. Isso ajuda a isolar problemas de compilação de UI antes de tentar rodar a aplicação em desenvolvimento.

5. **Aviso ou erro de chave KEK (`PERGO_KEK_BASE64`) inválida**
   * **Causa:** A variável `PERGO_KEK_BASE64` não foi configurada ou o valor fornecido não decodifica para uma chave de exatamente 32 bytes (256 bits) necessários para o algoritmo AES-256-GCM.
   * **Solução:** Em ambiente de desenvolvimento local, o PerGo avisa no log e automaticamente carrega uma chave padrão insegura de 32 bytes para fins de teste. Para ambientes de staging ou produção, gere uma chave criptograficamente segura usando:
     ```bash
     openssl rand -base64 32
     ```
     E configure-a na variável de ambiente `PERGO_KEK_BASE64`.

---

## Próximos Passos (Next Steps)

Com a aplicação rodando localmente e a console administrativa configurada, siga para os seguintes guias para aprofundar-se no projeto:

* **Desenvolvimento Interno:** Consulte o
  [Guia de Desenvolvimento](DEVELOPMENT.md).
* **Testando a Aplicação:** Consulte o [Guia de Testes](TESTING.md).
* **Detalhamento de Configurações:** Consulte a
  [Configuração do Sistema](CONFIGURATION.md).
* **Documentação de Canais:** Acesse [Canais e Credenciais](CHANNELS.md).
* **Referência da API REST:** Veja a [API REST](API.md).

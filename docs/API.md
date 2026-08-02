# Referência da API (API Reference)

A API do **PerGo** foi projetada para ser simples, unificada e performática, permitindo o envio de mensagens em canais variados a partir de um único payload padronizado JSON.

---

## Autenticação

Todas as requisições para a API pública de mensagens devem conter a chave de API (API Key) do seu Workspace fornecida no cabeçalho `Authorization`:

```http
Authorization: Bearer <sua_api_key_aqui>
```

Você pode gerar chaves de API na console do administrador em `http://localhost:8080/admin` selecionando o seu Workspace.

---

## 1. Enviar Mensagem

Envia texto ou template para um dos canais habilitados no build implantado:
Telegram, WhatsApp Cloud ou Instagram. O adaptador não oficial de WhatsApp Web
(`whatsapp`) existe somente em development/test e não faz parte do contrato de
deploy.

* **Endpoint:** `POST /api/v1/messages`
* **Content-Type:** `application/json`
* **Cabeçalhos obrigatórios:**
  * `Idempotency-Key`: identidade da operação dentro do Workspace. Deve ter de
    1 a 255 caracteres, começar com letra ou número e conter somente letras,
    números, `.`, `_`, `:`, `/` ou `-`.
  * `X-Trace-ID`: identidade estável de correlação e deduplicação dentro do
    Workspace (1–255 caracteres).
* **Respostas:**
  * `202 Accepted` — Mensagem recebida com sucesso e enfileirada para envio durável.
  * `400 Bad Request` — Payload inválido ou malformado.
  * `401 Unauthorized` — Chave de API inválida ou ausente.
  * `409 Conflict` — A chave de idempotência foi reutilizada com outro payload ou Trace ID.
  * `413 Payload Too Large` — O corpo HTTP excede 1 MiB.
  * `422 Unprocessable Entity` — O processamento de mídia falhou ou excedeu o
    limite; no build implantado isso inclui qualquer tentativa de envio de
    mídia, pois o armazenamento está desabilitado.
  * `425 Too Early` — Outra requisição idêntica ainda detém o claim de publicação; respeite `Retry-After`.
  * `429 Too Many Requests` — A fila de mensagens do seu Workspace atingiu o limite de capacidade de retenção de backpressure (padrão: 1.000 mensagens pendentes).

Uma repetição com o mesmo Workspace, `Idempotency-Key`, `X-Trace-ID` e corpo
retorna novamente `202` com o mesmo `message_id` e `queued_at`, sem uma nova
publicação, e inclui `Idempotency-Replayed: true`. O ledger permanece no
PostgreSQL durante toda a vida do Workspace. Se o processo cair depois de o
JetStream aceitar a publicação, o retry reutiliza o mesmo receipt, Trace ID e
identificador de deduplicação. O `Nats-Msg-Id` físico é
`<workspace_id>:<trace_id>`, portanto Trace IDs iguais em Workspaces distintos
não colidem; o claim vencido pode ser retomado com fencing.
O `message_id` é o receipt público estável e também identifica os
eventos de entrega `queued`, `sent`, `delivered`, `read` e `failed`.
`sending`, `failed_transient` e `uncertain` são estados internos e nunca nomes
de eventos públicos. Quando a resposta do provedor é ambígua, PerGo não repete
nem usa fallback: publica `failed` com `error: "DELIVERY_UNCERTAIN"` para
reconciliação operacional.
Os outros códigos públicos de falha produzidos pelo worker são
`DELIVERY_EXPIRED` e `DELIVERY_FAILED`; detalhes brutos de transporte ou do
provedor não fazem parte do contrato e não são persistidos nem publicados.

### Payload Padrão (Mensagem de Texto)

```json
{
  "to": "5511999999999",
  "channel": "whatsapp_cloud",
  "body": "Olá! Esta é uma mensagem de teste enviada pelo PerGo."
}
```

* **Campos:**
  * `to` (string, obrigatório): Destinatário no formato exigido pelo provedor.
    Para WABA, use o número completo com DDI e DDD (ex: `5511999999999`);
    para Telegram, o `chat_id`; para Instagram, o identificador de destinatário.
  * `channel` (string, obrigatório): No build implantado, use
    `"whatsapp_cloud"`, `"telegram"` ou `"instagram"`. Identificadores como
    `"whatsapp"`, `"whatsapp_mock"` e os canais SMTP são auxiliares locais e
    não possuem dispatcher nos perfis implantados.
  * `body` (string): Texto da mensagem. É obrigatório para uma mensagem textual;
    pode ficar vazio quando um template ou payload interativo válido é enviado.

---

### Disponibilidade de Mídia

O build implantado é somente texto e exige `PERGO_MEDIA_MODE=disabled`.
Requisições outbound que incluem `media` falham antes do enqueue com HTTP 422 e
`code: "media_download_failed"`; o detalhe interno é `media_disabled`.
Webhooks inbound de WABA ou Telegram com anexos recebem HTTP 503 com
`Retry-After: 300`, sem confirmar o evento antes de armazenar a mídia.

As variáveis `PERGO_S3_*` não habilitam mídia em produção. O modo `memory`
mantém o schema de mídia exercitável apenas em development/test e não representa
persistência S3.

---

### Envio de Templates (Exclusivo WhatsApp Cloud)

Para iniciar conversas com clientes (fora da janela de 24 horas) via WhatsApp Cloud API, você deve utilizar templates pré-aprovados na Meta:

```json
{
  "to": "5511999999999",
  "channel": "whatsapp_cloud",
  "body": "",
  "template_name": "welcome_message",
  "language": "pt_BR",
  "components": [
    {
      "type": "body",
      "parameters": [
        {
          "type": "text",
          "text": "João"
        }
      ]
    }
  ]
}
```

* **Parâmetros de Template:**
  * `template_name` (string): Nome técnico do template cadastrado na Meta.
  * `language` (string): Código do idioma do template (ex: `pt_BR`, `en_US`).
  * `components` (array, opcional): Parâmetros dinâmicos do corpo (`body`), cabeçalho (`header`) ou botões do template.

---

## 2. Webhooks de Notificação (Inbound)

Para escutar as mensagens recebidas de volta dos seus clientes ou atualizações de status de entrega (enviado, entregue, lido), configure seu servidor de escuta no dashboard do PerGo sob a aba **Webhooks**.

O PerGo irá disparar requisições `POST` contendo os dados do evento sempre que houver novidades.

No build implantado, a entrada de mensagens está disponível para WABA e
Telegram; Instagram é somente outbound. Eventos de entrega públicos usam apenas
`queued`, `sent`, `delivered`, `read` e `failed`.

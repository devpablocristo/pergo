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

Envia uma mensagem (texto, mídia ou template) para um canal específico (Telegram, WhatsApp Cloud ou WhatsApp Web).

* **Endpoint:** `POST /api/v1/messages`
* **Content-Type:** `application/json`
* **Cabeçalhos obrigatórios:**
  * `Idempotency-Key`: identidade da operação dentro do Workspace. Deve ter de
    1 a 255 caracteres, começar com letra ou número e conter somente letras,
    números, `.`, `_`, `:`, `/` ou `-`.
  * `X-Trace-ID`: identidade estável de correlação e deduplicação (1–255 caracteres).
* **Respostas:**
  * `202 Accepted` — Mensagem recebida com sucesso e enfileirada para envio durável.
  * `400 Bad Request` — Payload inválido ou malformado.
  * `401 Unauthorized` — Chave de API inválida ou ausente.
  * `409 Conflict` — A chave de idempotência foi reutilizada com outro payload ou Trace ID.
  * `413 Payload Too Large` — O corpo HTTP excede 1 MiB.
  * `425 Too Early` — Outra requisição idêntica ainda detém o claim de publicação; respeite `Retry-After`.
  * `429 Too Many Requests` — A fila de mensagens do seu Workspace atingiu o limite de capacidade de retenção de backpressure (padrão: 1.000 mensagens pendentes).

Uma repetição com o mesmo Workspace, `Idempotency-Key`, `X-Trace-ID` e corpo
retorna novamente `202` com o mesmo `message_id` e `queued_at`, sem uma nova
publicação, e inclui `Idempotency-Replayed: true`. O ledger permanece no
PostgreSQL durante toda a vida do Workspace. Se o processo cair depois de o
JetStream aceitar a publicação, o retry reutiliza o mesmo receipt, Trace ID e
identificador de deduplicação; o claim vencido pode ser retomado com fencing.
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
  "channel": "whatsapp",
  "body": "Olá! Esta é uma mensagem de teste enviada pelo PerGo."
}
```

* **Campos:**
  * `to` (string, obrigatório): Destinatário. Para WhatsApp, utilize o formato completo com DDI e DDD (ex: `5511999999999`). Para Telegram, o ID numérico do chat (`chat_id`).
  * `channel` (string, obrigatório): Canal de disparo. Valores válidos: `"whatsapp"` (WhatsApp Web/whatsmeow), `"whatsapp_cloud"` (WABA oficial) ou `"telegram"`.
  * `body` (string, obrigatório): Texto da mensagem.

---

### Envio de Mídia (Imagens, Documentos e Áudios)

Você pode enviar mídias anexando o objeto `media` ao payload:

```json
{
  "to": "5511999999999",
  "channel": "whatsapp_cloud",
  "body": "",
  "media": {
    "media_url": "https://meuservidor.com/comprovante.pdf",
    "media_type": "document",
    "filename": "comprovante.pdf",
    "caption": "Segue o comprovante de pagamento."
  }
}
```

* **Subcampos do objeto `media`:**
  * `media_url` (string, obrigatório): URL direta e pública do arquivo de mídia.
  * `media_type` (string, obrigatório): Tipo de mídia. Valores aceitos: `"image"`, `"document"` ou `"audio"`.
  * `filename` (string, obrigatório se `media_type` for `"document"`, opcional para os demais): Nome do arquivo exibido para o usuário.
  * `caption` (string, opcional): Legenda da imagem ou documento.

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

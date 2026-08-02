# Runbook: rollout coordenado das migrações 038–042

Este runbook é obrigatório para a primeira promoção do protocolo V2 quando o
ambiente já possui banco ou streams PerGo legados. O procedimento exige uma
janela de manutenção: não é seguro misturar binários antigos e novos.

## Por que o rollout não pode ser gradual

| Migração | Mudança incompatível |
|---|---|
| 038 | A unicidade de `trace_id` passa a ser por workspace. O `ON CONFLICT` do binário antigo deixa de corresponder ao índice. |
| 039 | Payload, URL e razão de falha da DLQ são cifrados e os campos plaintext são limpos. O `Down` recusa executar depois que existir ciphertext. |
| 040 | Receipts de provedor passam por um outbox transacional; remover a tabela pode perder eventos não publicados. |
| 041 | Campanhas WABA persistem idioma e shape exatos do template; campanhas ambíguas não podem ser iniciadas. |
| 042 | Deduplicação inbound inclui `connection_id`; escritores antigos não fornecem a nova chave obrigatória. |

O binário novo também usa exclusivamente os streams `PERGO_V2_*`; o binário
antigo usa `MESSAGES`, `WEBHOOKS`, `WEBHOOK_DELIVERIES`, `INBOUND` e
`CAMPAIGNS`. Trocar somente a imagem, somente o banco ou somente o broker deixa
mensagens órfãs ou escritores incompatíveis.

## Pré-condições

- Reserve uma janela com bloqueio de ingressos API e webhooks.
- Fixe por digest a imagem antiga e a nova; não reconstrua durante a janela.
- Confirme que o restore de PostgreSQL e JetStream foi ensaiado e registre o
  RPO/RTO aceito.
- Preserve a mesma `PERGO_KEK_BASE64`; valide que o Secret Manager consegue
  montá-la no job.
- Prepare quatro arquivos NATS `.creds`: API, webhook, worker e migrate. Somente
  migrate pode administrar streams/consumers e inspecionar os streams legados.
- Use PostgreSQL TCP com `sslmode=verify-full&sslrootcert=...` ou socket Unix
  aprovado; monte CA/credenciais legíveis por UID/GID `65532:65532`.
- Fixe `PERGO_META_GRAPH_VERSION=v25.0`,
  `PERGO_MEDIA_MODE=disabled` e, no perfil API, um
  `PERGO_SESSION_SECRET` estável com pelo menos 32 caracteres.
- Confirme que `PERGO_NATS_ACCOUNT` identifica somente este ambiente e que
  `PERGO_NATS_STREAM_REPLICAS` é pelo menos `3` em produção.

Não exponha DSN, KEK ou conteúdo dos arquivos `.creds` em comandos copiados para
ticket, logs ou histórico de shell. Injete-os pela configuração segura da
plataforma.

## Fase 1 — inventário e backup

1. Registre o digest dos workloads em execução e a versão Goose aplicada:

   ```sql
   SELECT id, version_id, is_applied, tstamp
   FROM goose_db_version
   ORDER BY id DESC
   LIMIT 10;
   ```

2. Liste campanhas ainda ativas. Complete ou cancele conscientemente qualquer
   campanha `sending` antes de prosseguir; não presuma que o protocolo novo
   retomará um batch legado:

   ```sql
   SELECT id, workspace_id, channel, status, template_name
   FROM campaigns
   WHERE status IN ('scheduled', 'sending')
   ORDER BY created_at;
   ```

3. Inventarie no NATS os streams, subjects, consumers, mensagens, pendências e
   sequência:

   ```bash
   nats stream report
   nats stream info MESSAGES
   nats stream info WEBHOOKS
   nats stream info WEBHOOK_DELIVERIES
   nats stream info INBOUND
   nats stream info CAMPAIGNS
   ```

   Um stream ausente é aceitável. Um stream `PERGO_V2_*` já existente sem
   `PERGO_ENVIRONMENT_GUARD` não é aceitável: pare e investigue sua origem.

4. Faça backup consistente do PostgreSQL e snapshot/export de cada stream
   existente. Registre checksums, localização, retenção e procedimento de
   restore. Não prossiga sem ambos os lados do protocolo.

## Fase 2 — quiesce e drain legado

1. Bloqueie novos requests de API no edge.
2. Suspenda os endpoints públicos de webhook. Provedores devem receber falha
   retryable, não `2xx`, durante a janela.
3. Pare API, webhook, schedulers e qualquer publisher externo. Mantenha somente
   os workers antigos necessários para consumir o backlog legado.
4. Aguarde todos os streams legados chegarem a **zero mensagens**, e todos os
   consumers a zero `num_pending` e zero `num_ack_pending`. Revalide duas vezes,
   separadas pelo maior intervalo de retry configurado.
5. Pare os workers antigos e confirme que nenhuma conexão antiga continua
   publicando.

Não use purge/delete para satisfazer o gate. Se uma mensagem não pode ser
processada, registre e resolva a exceção de negócio antes de continuar.

## Fase 3 — migrations, backfill, adoção e bootstrap

Execute uma única instância da imagem nova com o comando `/app/pergo migrate` e:

```text
PERGO_ENVIRONMENT=production
PERGO_DATABASE_URL=<DSN seguro montado pela plataforma>
PERGO_KEK_BASE64=<segredo injetado>
PERGO_NATS_URLS=tls://nats-1.example:4222,tls://nats-2.example:4222
PERGO_NATS_CREDS_FILE=/var/run/secrets/nats/migrate.creds
PERGO_NATS_CA_FILE=/var/run/secrets/nats/ca.crt
PERGO_NATS_TLS_SERVER_NAME=nats.example
PERGO_NATS_ACCOUNT=<rótulo exclusivo do ambiente>
PERGO_NATS_STREAM_REPLICAS=3
PERGO_NATS_ADOPT_DRAINED_LEGACY=true
PERGO_META_GRAPH_VERSION=v25.0
PERGO_MEDIA_MODE=disabled
```

O comando deve, nesta ordem:

1. aplicar Goose até 042 e criar as partições de auditoria atual e seguinte;
2. cifrar em lotes as linhas legadas da DLQ, limpar seus campos plaintext e
   confirmar que nenhuma linha ficou sem ciphertext;
3. verificar novamente que os streams legados estão vazios;
4. criar `PERGO_ENVIRONMENT_GUARD`;
5. criar/reconciliar streams e consumers V2.

Se o job falhar depois de uma etapa parcial, preserve os artefatos e repita o
mesmo job após corrigir a causa. As etapas são projetadas para roll-forward
idempotente. Não execute `Down`.

Depois do primeiro sucesso, remova
`PERGO_NATS_ADOPT_DRAINED_LEGACY` da definição do job. A flag é um gate de
adoção, não uma configuração permanente.

## Fase 4 — verificação antes de reabrir tráfego

### PostgreSQL

Confirme as versões aplicadas e que a DLQ não contém plaintext legado:

```sql
SELECT id, version_id, is_applied, tstamp
FROM goose_db_version
ORDER BY id DESC
LIMIT 10;

SELECT count(*) AS dlq_without_ciphertext
FROM webhook_dlqs
WHERE encrypted_data IS NULL;

SELECT count(*) AS dlq_with_unscrubbed_plaintext
FROM webhook_dlqs
WHERE encrypted_data IS NOT NULL
  AND (
    payload <> '{}'::jsonb
    OR webhook_url <> '[encrypted]'
    OR failure_reason IS NOT NULL
  );
```

Ambos os contadores da DLQ devem ser zero. Não selecione nem imprima payload,
URL, ciphertext ou razão de falha.

Liste as partições anexadas e confira cobertura do mês atual e seguinte:

```sql
SELECT relid::regclass AS partition_name, pg_get_expr(c.relpartbound, c.oid) AS bounds
FROM pg_partition_tree('audit_logs') AS tree
JOIN pg_class AS c ON c.oid = tree.relid
WHERE tree.level = 1
ORDER BY c.relname;
```

Campanhas WABA legadas cujo template não pôde ser resolvido de forma unívoca
permanecem sem idioma e devem ser recriadas antes de iniciar:

```sql
SELECT id, workspace_id, connection_id, template_name
FROM campaigns
WHERE channel = 'whatsapp_cloud'
  AND template_name IS NOT NULL
  AND template_language IS NULL;
```

### JetStream

Confirme description, storage `file`, réplicas, retention, subjects, limites,
janela de deduplicação e consumers de:

| Stream | Subject | Consumer |
|---|---|---|
| `PERGO_V2_OUTBOUND` | `pergo.v2.outbound.>` | `pergo-v2-outbound-worker` |
| `PERGO_V2_WEBHOOK_EVENTS` | `pergo.v2.webhook_events.>` | `pergo-v2-webhook-events-worker` |
| `PERGO_V2_WEBHOOK_DELIVERIES` | `pergo.v2.webhook_deliveries.>` | `pergo-v2-webhook-deliveries-worker` |
| `PERGO_V2_INBOUND` | `pergo.v2.inbound.>` | `pergo-v2-inbound-events-worker` |
| `PERGO_V2_CAMPAIGNS` | `pergo.v2.campaigns.>` | `pergo-v2-campaign-worker` |

`PERGO_ENVIRONMENT_GUARD` deve ter description exatamente
`environment=<ambiente>;account=<PERGO_NATS_ACCOUNT>`, subject
`_pergo.environment.guard`, storage `file`, `max_msgs=1` e o número de réplicas
configurado. Não edite a description para contornar conflito de conta.

## Fase 5 — promoção

1. Inicie uma réplica do novo worker com a credencial não administrativa. Ele
   deve verificar o guard e bindar todos os contratos sem mutá-los.
2. Confirme que não há erro de DLQ sem ciphertext, drift de stream/consumer ou
   falha de decifrado.
3. Escale o worker e inicie webhook.
4. Envie canários de texto por Telegram e WABA, incluindo receipt de status e
   webhook de tenant. Um canário com mídia deve ser rejeitado; não o trate como
   regressão neste build.
5. Inicie API, valide `/healthz` e `/readyz`, autenticação administrativa,
   segredo de sessão estável e isolamento por workspace.
6. Reabra tráfego gradualmente e monitore backlog, redeliveries, DLQ, receipts,
   erro de auditoria e latência.

Conserve os streams legados vazios e os snapshots até expirar a janela formal de
recuperação. Sua remoção deve ser uma mudança separada e aprovada.

## Rollback e recuperação

Depois que 039 gravar qualquer ciphertext, **não há downgrade destrutivo
suportado**. Não execute `goose down`, não remova `encrypted_data` e não volte
apenas o binário.

O caminho normal é:

1. bloquear novamente os produtores;
2. manter DB, KEK, guard e streams V2;
3. corrigir a imagem;
4. repetir `migrate`;
5. promover worker, webhook e API nessa ordem.

Se roll-forward for impossível, a única volta ao protocolo antigo é restaurar,
como uma unidade, o backup PostgreSQL e os snapshots JetStream anteriores à
janela. Isso descarta tudo após o ponto de backup e exige aprovação explícita do
RPO. Valide checksums e mantenha o tráfego bloqueado até banco, broker e imagem
antiga pertencerem ao mesmo ponto no tempo.

## Manutenção recorrente

Agende `/app/pergo migrate` ao menos uma vez por mês, antes da virada. Mesmo sem
nova migração, o comando cria sob advisory lock as partições de auditoria do mês
atual e seguinte, revalida o backfill cifrado e reconcilia JetStream. Ele requer
as mesmas credenciais seguras de DB, KEK e NATS admin. A ausência da próxima
partição deve gerar alerta; `audit_logs` não possui partição default.

-- +goose Up
ALTER TABLE campaigns
    ADD COLUMN template_language TEXT;

-- Backfill only an unambiguous template variant. Draft legacy campaigns that
-- cannot be resolved remain NULL and are rejected by the application preflight
-- until an administrator recreates them with an exact approved template.
UPDATE campaigns AS c
   SET template_language = (
      SELECT min(t.language) AS language
        FROM waba_templates AS t
       WHERE t.connection_id = c.connection_id
         AND t.workspace_id = c.workspace_id
         AND t.name = c.template_name
       HAVING count(*) = 1
   )
 WHERE c.channel = 'whatsapp_cloud'
   AND c.template_name IS NOT NULL
   AND c.template_language IS NULL
   AND (
       SELECT count(*)
         FROM waba_templates AS t
        WHERE t.connection_id = c.connection_id
          AND t.workspace_id = c.workspace_id
          AND t.name = c.template_name
   ) = 1;

ALTER TABLE campaigns
    ADD CONSTRAINT campaigns_template_language_shape
    CHECK (
        template_language IS NULL
        OR (
            length(template_language) BETWEEN 2 AND 32
            AND template_language !~ '[[:space:]]'
        )
    ) NOT VALID;

UPDATE campaigns
   SET template_language = NULL
 WHERE template_language IS NOT NULL
   AND (
       length(template_language) NOT BETWEEN 2 AND 32
       OR template_language ~ '[[:space:]]'
   );

ALTER TABLE campaigns
    VALIDATE CONSTRAINT campaigns_template_language_shape;

-- +goose Down
ALTER TABLE campaigns
    DROP CONSTRAINT IF EXISTS campaigns_template_language_shape;

ALTER TABLE campaigns
    DROP COLUMN IF EXISTS template_language;

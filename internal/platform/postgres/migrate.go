package postgres

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

// RunMigrations applies all pending goose migrations using the embedded SQL files.
func RunMigrations(db *sql.DB) error {
	goose.SetBaseFS(embedMigrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	if err := goose.Up(db, "migrations"); err != nil {
		return err
	}
	return ensureAuditPartitions(db)
}

func ensureAuditPartitions(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin audit partition maintenance: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtext('pergo:audit-partitions'))`); err != nil {
		return fmt.Errorf("lock audit partition maintenance: %w", err)
	}
	if _, err := tx.Exec(`SELECT create_monthly_partition(CURRENT_DATE)`); err != nil {
		return fmt.Errorf("create current audit partition: %w", err)
	}
	if _, err := tx.Exec(`SELECT create_monthly_partition((CURRENT_DATE + interval '1 month')::date)`); err != nil {
		return fmt.Errorf("create next audit partition: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit audit partition maintenance: %w", err)
	}
	return nil
}

package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

// AuditPartitionMonthsAhead is the number of complete future calendar months
// kept ready in addition to the current month.
const AuditPartitionMonthsAhead = 6

// RunMigrations applies all pending goose migrations using the embedded SQL files.
func RunMigrations(db *sql.DB) error {
	goose.SetBaseFS(embedMigrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	if err := goose.Up(db, "migrations"); err != nil {
		return err
	}
	return MaintainAuditPartitions(context.Background(), db)
}

// MaintainAuditPartitions idempotently creates audit partitions for the
// current month and the next AuditPartitionMonthsAhead months. It is safe to
// invoke after every migration and from a periodic maintenance job.
func MaintainAuditPartitions(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("audit partition maintenance requires a database")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return ensureAuditPartitions(
		ctx,
		sqlAuditPartitionBeginner{db: db},
		AuditPartitionMonthsAhead,
	)
}

type auditPartitionTransaction interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

type auditPartitionBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (auditPartitionTransaction, error)
}

type sqlAuditPartitionBeginner struct {
	db *sql.DB
}

func (b sqlAuditPartitionBeginner) BeginTx(
	ctx context.Context,
	options *sql.TxOptions,
) (auditPartitionTransaction, error) {
	return b.db.BeginTx(ctx, options)
}

func ensureAuditPartitions(
	ctx context.Context,
	db auditPartitionBeginner,
	monthsAhead int,
) error {
	if monthsAhead < 0 {
		return errors.New("audit partition horizon cannot be negative")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin audit partition maintenance: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtext('pergo:audit-partitions'))`,
	); err != nil {
		return fmt.Errorf("lock audit partition maintenance: %w", err)
	}
	for monthOffset := 0; monthOffset <= monthsAhead; monthOffset++ {
		if _, err := tx.ExecContext(
			ctx,
			`SELECT create_monthly_partition(
				(CURRENT_DATE + make_interval(months => $1))::date
			)`,
			monthOffset,
		); err != nil {
			return fmt.Errorf(
				"create audit partition at month offset %d: %w",
				monthOffset,
				err,
			)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit audit partition maintenance: %w", err)
	}
	return nil
}

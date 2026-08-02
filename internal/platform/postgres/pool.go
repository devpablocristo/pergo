// Package postgres provides PostgreSQL connection management and migration
// utilities for PerGo.
package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// NewPool creates a new pgxpool.Pool connected to the given DSN.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// pgx parse errors may echo the complete DSN, including its password.
		return nil, errors.New("invalid PostgreSQL connection configuration")
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("failed to initialize PostgreSQL connection pool")
	}
	return pool, nil
}

// NewSQLDB wraps a pgxpool.Pool as a *sql.DB via the pgx/v5/stdlib bridge.
// This is used by goose migrations and the whatsmeow sqlstore.
func NewSQLDB(pool *pgxpool.Pool) (*sql.DB, error) {
	db := stdlib.OpenDBFromPool(pool)
	return db, nil
}

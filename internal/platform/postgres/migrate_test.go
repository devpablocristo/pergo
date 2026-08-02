package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestEnsureAuditPartitionsCreatesCurrentThroughSixFutureMonths(t *testing.T) {
	tx := &recordingAuditPartitionTx{}
	beginner := &recordingAuditPartitionBeginner{transactions: []*recordingAuditPartitionTx{tx}}

	if err := ensureAuditPartitions(
		context.Background(),
		beginner,
		AuditPartitionMonthsAhead,
	); err != nil {
		t.Fatalf("ensureAuditPartitions(): %v", err)
	}
	if !tx.committed {
		t.Fatal("transaction was not committed")
	}
	if len(tx.calls) != AuditPartitionMonthsAhead+2 {
		t.Fatalf(
			"ExecContext calls = %d, want %d",
			len(tx.calls),
			AuditPartitionMonthsAhead+2,
		)
	}
	if !strings.Contains(tx.calls[0].query, "pg_advisory_xact_lock") {
		t.Fatalf("first query = %q, want advisory lock", tx.calls[0].query)
	}
	for offset, call := range tx.calls[1:] {
		if !strings.Contains(call.query, "create_monthly_partition") {
			t.Fatalf("partition query %d = %q", offset, call.query)
		}
		if len(call.args) != 1 || call.args[0] != offset {
			t.Fatalf("partition query %d args = %#v, want [%d]", offset, call.args, offset)
		}
	}
}

func TestEnsureAuditPartitionsIsRepeatable(t *testing.T) {
	first := &recordingAuditPartitionTx{}
	second := &recordingAuditPartitionTx{}
	beginner := &recordingAuditPartitionBeginner{
		transactions: []*recordingAuditPartitionTx{first, second},
	}

	for run := 0; run < 2; run++ {
		if err := ensureAuditPartitions(context.Background(), beginner, 2); err != nil {
			t.Fatalf("run %d: %v", run+1, err)
		}
	}
	if !first.committed || !second.committed {
		t.Fatalf(
			"commits = first:%t second:%t, want both true",
			first.committed,
			second.committed,
		)
	}
	if len(first.calls) != len(second.calls) {
		t.Fatalf("call counts differ: first=%d second=%d", len(first.calls), len(second.calls))
	}
	for i := range first.calls {
		if first.calls[i].query != second.calls[i].query {
			t.Fatalf("query %d differs between runs", i)
		}
	}
}

func TestEnsureAuditPartitionsRollsBackOnCreationFailure(t *testing.T) {
	tx := &recordingAuditPartitionTx{failAt: 4}
	beginner := &recordingAuditPartitionBeginner{transactions: []*recordingAuditPartitionTx{tx}}

	err := ensureAuditPartitions(context.Background(), beginner, 6)
	if err == nil || !strings.Contains(err.Error(), "month offset 2") {
		t.Fatalf("ensureAuditPartitions() error = %v, want offset 2", err)
	}
	if tx.committed {
		t.Fatal("failed transaction was committed")
	}
	if !tx.rolledBack {
		t.Fatal("failed transaction was not rolled back")
	}
}

func TestEnsureAuditPartitionsRejectsNegativeHorizon(t *testing.T) {
	err := ensureAuditPartitions(
		context.Background(),
		&recordingAuditPartitionBeginner{},
		-1,
	)
	if err == nil {
		t.Fatal("negative horizon error = nil")
	}
}

func TestMaintainAuditPartitionsRejectsNilDatabase(t *testing.T) {
	if err := MaintainAuditPartitions(context.Background(), nil); err == nil {
		t.Fatal("MaintainAuditPartitions(nil) error = nil")
	}
}

type auditPartitionExecCall struct {
	query string
	args  []any
}

type recordingAuditPartitionBeginner struct {
	transactions []*recordingAuditPartitionTx
	next         int
}

func (b *recordingAuditPartitionBeginner) BeginTx(
	context.Context,
	*sql.TxOptions,
) (auditPartitionTransaction, error) {
	if b.next >= len(b.transactions) {
		return nil, errors.New("unexpected BeginTx")
	}
	tx := b.transactions[b.next]
	b.next++
	return tx, nil
}

type recordingAuditPartitionTx struct {
	calls      []auditPartitionExecCall
	failAt     int
	committed  bool
	rolledBack bool
}

func (tx *recordingAuditPartitionTx) ExecContext(
	_ context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	tx.calls = append(tx.calls, auditPartitionExecCall{query: query, args: args})
	if tx.failAt > 0 && len(tx.calls) == tx.failAt {
		return nil, errors.New("injected partition failure")
	}
	return auditPartitionResult{}, nil
}

func (tx *recordingAuditPartitionTx) Commit() error {
	tx.committed = true
	return nil
}

func (tx *recordingAuditPartitionTx) Rollback() error {
	tx.rolledBack = true
	return nil
}

type auditPartitionResult struct{}

func (auditPartitionResult) LastInsertId() (int64, error) { return 0, nil }
func (auditPartitionResult) RowsAffected() (int64, error) { return 0, nil }

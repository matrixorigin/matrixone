package db

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestQuoteIdent(t *testing.T) {
	if got := QuoteIdent("a`b"); got != "`a``b`" {
		t.Fatalf("QuoteIdent() = %q, want `a``b`", got)
	}
}

func TestDSN(t *testing.T) {
	ep := Endpoint{Host: "127.0.0.1", Port: 6001, User: "acc:admin", Password: "111"}
	got := DSN(ep, "db1")
	want := "acc#admin:111@tcp(127.0.0.1:6001)/db1?allowAllFiles=true&multiStatements=true&parseTime=true"
	if got != want {
		t.Fatalf("DSN() = %q, want %q", got, want)
	}
}

func TestDSNEmptyDatabase(t *testing.T) {
	ep := Endpoint{Host: "127.0.0.1", Port: 6001, User: "acc:admin", Password: "111"}
	got := DSN(ep, "")
	want := "acc#admin:111@tcp(127.0.0.1:6001)/?allowAllFiles=true&multiStatements=true&parseTime=true"
	if got != want {
		t.Fatalf("DSN() = %q, want %q", got, want)
	}
}

func TestDSNPasswordSpecialCharacters(t *testing.T) {
	ep := Endpoint{Host: "mo.local", Port: 6001, User: "acc:admin", Password: "p:a/s@s?x=y&z"}
	got := DSN(ep, "db1")
	want := "acc#admin:p:a/s@s?x=y&z@tcp(mo.local:6001)/db1?allowAllFiles=true&multiStatements=true&parseTime=true"
	if got != want {
		t.Fatalf("DSN() = %q, want %q", got, want)
	}

	cfg, err := mysql.ParseDSN(got)
	if err != nil {
		t.Fatalf("ParseDSN() error = %v", err)
	}
	if cfg.Passwd != ep.Password {
		t.Fatalf("parsed password = %q, want %q", cfg.Passwd, ep.Password)
	}
}

func TestClientCloseNilIsNoop(t *testing.T) {
	var client *Client
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := (&Client{}).Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestClientListOrdinaryTablesFiltersInternalTables(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{db: sqlDB}
	defer client.Close()

	rows := sqlmock.NewRows([]string{"relname", "relkind"}).
		AddRow("t1", "r").
		AddRow("__mo_internal", "r").
		AddRow("%!%hidden", "r").
		AddRow("t2", "r")
	mock.ExpectQuery("select relname, relkind from mo_catalog.mo_tables").
		WithArgs("db1").
		WillReturnRows(rows)

	tables, err := client.ListOrdinaryTables(context.Background(), "db1")
	if err != nil {
		t.Fatalf("ListOrdinaryTables() error = %v", err)
	}
	if len(tables) != 2 || tables[0].Name != "t1" || tables[1].Name != "t2" {
		t.Fatalf("tables = %+v, want t1 and t2", tables)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClientListOrdinaryTablesReturnsQueryError(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{db: sqlDB}
	defer client.Close()

	errBoom := errors.New("boom")
	mock.ExpectQuery("select relname, relkind from mo_catalog.mo_tables").
		WithArgs("db1").
		WillReturnError(errBoom)

	if _, err := client.ListOrdinaryTables(context.Background(), "db1"); !errors.Is(err, errBoom) {
		t.Fatalf("ListOrdinaryTables() error = %v, want boom", err)
	}
}

func TestClientCountRowsQuotesIdentifiers(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{db: sqlDB}
	defer client.Close()

	mock.ExpectQuery("select count\\(\\*\\) from `db``1`.`t``1`").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(7)))

	count, err := client.CountRows(context.Background(), "db`1", "t`1")
	if err != nil {
		t.Fatalf("CountRows() error = %v", err)
	}
	if count != 7 {
		t.Fatalf("CountRows() = %d, want 7", count)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClientEnsureDatabaseQuotesIdentifier(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{db: sqlDB}
	defer client.Close()

	mock.ExpectExec("create database if not exists `db``1`").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := client.EnsureDatabase(context.Background(), "db`1"); err != nil {
		t.Fatalf("EnsureDatabase() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

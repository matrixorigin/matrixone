package db

import (
	"testing"

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

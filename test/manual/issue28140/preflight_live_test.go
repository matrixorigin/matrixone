//go:build manual

package main

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestPreflightLive(t *testing.T) {
	dsn := os.Getenv("MO_ISSUE28140_DSN")
	require.NotEmpty(t, dsn, "set the system-account MO_ISSUE28140_DSN")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, run(ctx, db))
}

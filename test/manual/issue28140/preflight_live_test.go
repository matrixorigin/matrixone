//go:build manual

package main

import (
	"context"
	"database/sql"
	"fmt"
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

func TestPreflightLiveIgnoresOtherSessionTemporaryTable(t *testing.T) {
	dsn := os.Getenv("MO_ISSUE28140_DSN")
	require.NotEmpty(t, dsn, "set the system-account MO_ISSUE28140_DSN")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(2)

	database := fmt.Sprintf("issue28140_preflight_%d", time.Now().UnixNano())
	_, err = db.ExecContext(ctx, "create database `"+database+"`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, dropErr := db.ExecContext(context.Background(), "drop database if exists `"+database+"`")
		require.NoError(t, dropErr)
	})

	owner, err := db.Conn(ctx)
	require.NoError(t, err)
	defer owner.Close()
	_, err = owner.ExecContext(ctx, "use `"+database+"`")
	require.NoError(t, err)
	_, err = owner.ExecContext(ctx, "create temporary table t (a int, b varchar(20), c timestamp)")
	require.NoError(t, err)
	// owner retains its dedicated connection while run opens another session.
	require.NoError(t, run(ctx, db))
}

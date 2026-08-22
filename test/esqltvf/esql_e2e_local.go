// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command esql_e2e_local drives the esql_tvf end-to-end test. It connects to a
// running MatrixOne over a MySQL DSN and issues esql_tvf queries against a
// seeded Elasticsearch index. It is launched by optools/esql_ci.bash, which
// stands up and tears down both services. Run via `go run`.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type report struct {
	Status string   `json:"status"`
	Cases  []string `json:"cases"`
	Error  string   `json:"error,omitempty"`
}

// esConfig mirrors the JSON tags of elasticsearch.Config that the server-side
// config parser understands (addresses/username/password).
type esConfig struct {
	Addresses []string `json:"addresses"`
	Username  string   `json:"username"`
	Password  string   `json:"password"`
}

// schema spec (long form) shared with parse_jsonl_data: name/type per column.
const employeesSchema = `{"cols":[{"name":"name","type":"string"},{"name":"dept","type":"string"},{"name":"salary","type":"int64"}]}`

func main() {
	var dsn, esEndpoint, esUser, esPassword, reportDir string
	flag.StringVar(&dsn, "dsn", "root:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=30s&writeTimeout=30s", "MO DSN")
	flag.StringVar(&esEndpoint, "es-endpoint", "http://127.0.0.1:9200", "Elasticsearch endpoint")
	flag.StringVar(&esUser, "es-user", "elastic", "Elasticsearch username")
	flag.StringVar(&esPassword, "es-password", "", "Elasticsearch password")
	flag.StringVar(&reportDir, "report-dir", "test/esqltvf/reports/local", "report directory")
	flag.Parse()

	r := report{Status: "failed"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err == nil {
		defer db.Close()
		err = waitForMO(ctx, db)
	}
	if err == nil {
		err = run(ctx, db, esEndpoint, esUser, esPassword, &r)
	}
	if err == nil {
		r.Status = "passed"
	} else {
		r.Error = err.Error()
	}
	if writeErr := writeReport(reportDir, r); writeErr != nil && err == nil {
		err = writeErr
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func waitForMO(ctx context.Context, db *sql.DB) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for MatrixOne: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func run(ctx context.Context, db *sql.DB, esEndpoint, esUser, esPassword string, r *report) error {
	cfg := esConfig{Addresses: []string{esEndpoint}, Username: esUser, Password: esPassword}
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	cfgStr := string(cfgJSON)

	// Path A: explicit connect() handle. The handle is a session variable that
	// esql_tvf resolves at runtime.
	if _, err := db.ExecContext(ctx, "set @h = esql_tvf_connect('"+sqlEscape(cfgStr)+"')"); err != nil {
		return fmt.Errorf("esql_tvf_connect: %w", err)
	}
	r.Cases = append(r.Cases, "esql_tvf_connect")

	// Path B: default connection via the @esql_tvf_config session variable.
	if _, err := db.ExecContext(ctx, "set @esql_tvf_config = '"+sqlEscape(cfgStr)+"'"); err != nil {
		return fmt.Errorf("set @esql_tvf_config: %w", err)
	}

	schema := sqlEscape(employeesSchema)

	// All 5 rows, typed schema, explicit handle.
	if err := expectScalar(ctx, db,
		"select count(*) from esql_tvf('FROM employees | KEEP name, dept, salary | LIMIT 100', '"+schema+"', @h) t", "5"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "count-all")

	// Filter pushed as ES|QL: Bob (150000) and Dave (120000) exceed 100000.
	if err := expectScalar(ctx, db,
		"select count(*) from esql_tvf('FROM employees | WHERE salary > 100000 | KEEP name, dept, salary', '"+schema+"', @h) t", "2"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "esql-where")

	// MO-side predicate over the typed column returns the same set.
	if err := expectScalar(ctx, db,
		"select count(*) from esql_tvf('FROM employees | KEEP name, dept, salary | LIMIT 100', '"+schema+"', @h) t where salary > 100000", "2"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "mo-where")

	// Null salary (Eve) maps to SQL NULL.
	if err := expectScalar(ctx, db,
		"select count(*) from esql_tvf('FROM employees | KEEP name, dept, salary | LIMIT 100', '"+schema+"', @h) t where salary is null", "1"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "null-salary")

	// Typed aggregate over the pushed-down projection.
	if err := expectScalar(ctx, db,
		"select cast(max(salary) as char) from esql_tvf('FROM employees | KEEP name, dept, salary | LIMIT 100', '"+schema+"', @h) t", "150000"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "max-salary")

	// No-schema path uses the default @esql_tvf_config connection and returns a
	// single json-array column; still 5 rows.
	if err := expectScalar(ctx, db,
		"select count(*) from esql_tvf('FROM employees | LIMIT 100') t", "5"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "no-schema-default-conn")

	// Disconnect; a subsequent use of the handle must fail.
	if _, err := db.ExecContext(ctx, "select esql_tvf_disconnect(@h)"); err != nil {
		return fmt.Errorf("esql_tvf_disconnect: %w", err)
	}
	r.Cases = append(r.Cases, "esql_tvf_disconnect")

	return runExternalTable(ctx, db, cfgStr, r)
}

// runExternalTable exercises CREATE EXTERNAL TABLE ... ENGINE = ESQL over the
// same seeded index: schema-typed rows, IN of two ES|QL queries, a local
// predicate on a declared column, and config via env: (the harness exports
// MO_ESQL_E2E_CFG into the mo-service process) and via @esql_tvf_config.
func runExternalTable(ctx context.Context, db *sql.DB, cfgStr string, r *report) error {
	stmts := []string{
		"drop database if exists esql_ext",
		"create database esql_ext",
		"use esql_ext",
		// config comes from the environment of the CN process (env:NAME),
		// so no secret is inlined in the DDL.
		"create external table emp (name varchar(100), dept varchar(50), salary bigint) engine = esql with ('config' = 'env:MO_ESQL_E2E_CFG')",
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exttab setup %q: %w", stmt, err)
		}
	}
	r.Cases = append(r.Cases, "exttab-create-env-config")

	const keep = " | KEEP name, dept, salary"
	q1 := "FROM employees" + keep + " | LIMIT 100"

	// schema-typed rows: all 5.
	if err := expectScalar(ctx, db,
		"select count(*) from emp where __mo_query = '"+sqlEscape(q1)+"'", "5"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "exttab-count-all")

	// local predicate on a declared column.
	if err := expectScalar(ctx, db,
		"select count(*) from emp where __mo_query = '"+sqlEscape(q1)+"' and salary > 100000", "2"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "exttab-local-predicate")

	// null maps to SQL NULL.
	if err := expectScalar(ctx, db,
		"select count(*) from emp where __mo_query = '"+sqlEscape(q1)+"' and salary is null", "1"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "exttab-null")

	// IN of two ES|QL queries: rows from both, tagged by __mo_query.
	qEng := "FROM employees | WHERE dept == \"eng\"" + keep
	qSales := "FROM employees | WHERE dept == \"sales\"" + keep
	if err := expectScalar(ctx, db,
		"select count(*) from emp where __mo_query in ('"+sqlEscape(qEng)+"', '"+sqlEscape(qSales)+"')", "4"); err != nil {
		return err
	}
	if err := expectScalar(ctx, db,
		"select count(distinct __mo_query) from emp where __mo_query in ('"+sqlEscape(qEng)+"', '"+sqlEscape(qSales)+"')", "2"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "exttab-in-two-queries")

	// config via @esql_tvf_config on a table created without a config option.
	if _, err := db.ExecContext(ctx,
		"create external table emp_sess (name varchar(100), dept varchar(50), salary bigint) engine = esql"); err != nil {
		return fmt.Errorf("exttab sess create: %w", err)
	}
	if _, err := db.ExecContext(ctx, "set @esql_tvf_config = '"+sqlEscape(cfgStr)+"'"); err != nil {
		return err
	}
	if err := expectScalar(ctx, db,
		"select count(*) from emp_sess where __mo_query = '"+sqlEscape(q1)+"'", "5"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "exttab-session-config")

	if _, err := db.ExecContext(ctx, "drop database esql_ext"); err != nil {
		return err
	}
	return nil
}

func expectScalar(ctx context.Context, db *sql.DB, query, expected string) error {
	var actual string
	if err := db.QueryRowContext(ctx, query).Scan(&actual); err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	}
	if actual != expected {
		return fmt.Errorf("query %s: expected %q, got %q", query, expected, actual)
	}
	return nil
}

// sqlEscape doubles single quotes so a JSON config string can be embedded in a
// single-quoted SQL literal.
func sqlEscape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func writeReport(dir string, value report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), data, 0o600); err != nil {
		return err
	}
	summary := "## esql_tvf E2E\n\nStatus: **" + value.Status + "**\n\nCases: " +
		strings.Join(value.Cases, ", ") + "\n"
	if value.Error != "" {
		summary += "\nError: " + value.Error + "\n"
	}
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(summary), 0o600)
}

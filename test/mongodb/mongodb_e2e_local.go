// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

type report struct {
	Status string   `json:"status"`
	Cases  []string `json:"cases"`
	Error  string   `json:"error,omitempty"`
}

type fixtureManifest struct {
	Rows [][]string `json:"rows"`
}

func main() {
	var dsn, host, reportDir string
	flag.StringVar(&dsn, "dsn", "root:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=30s&writeTimeout=30s", "MO DSN")
	flag.StringVar(&host, "mongo-host", "127.0.0.1:27017", "MongoDB seed")
	flag.StringVar(&reportDir, "report-dir", "test/mongodb/reports/local", "report directory")
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
		err = runWithDSN(ctx, db, dsn, host, &r)
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

func run(ctx context.Context, db *sql.DB, host string, r *report) error {
	return runWithDSN(ctx, db, "", host, r)
}

func runWithDSN(ctx context.Context, db *sql.DB, dsn, host string, r *report) error {
	manifest, err := loadFixtureManifest("test/mongodb/fixture_manifest.json")
	if err != nil {
		return err
	}
	statements := []string{
		"drop database if exists mongodb_ci",
		"create database mongodb_ci",
		"create mongodb connection if not exists mongodb_ci with ('hosts'='" + host + "','replica_set'='rs0','auth_source'='mongodb_source','auth_mechanism'='SCRAM-SHA-256','credential_secret_ref'='secret://env/MO_MONGODB_E2E_CREDENTIAL','tls_mode'='disabled','read_preference'='primary','read_concern'='majority','options_json'='{\"direct\":true}')",
		"create external table mongodb_ci.events(mongo_id char(24) mongodb_path '_id', device_id varchar(20), site_id varchar(10), ts datetime(3) mongodb_convert 'try_null', measurement double mongodb_convert 'try_null', source_batch varchar(50)) engine=mongodb with ('connection'='mongodb_ci','database'='mongodb_source','collection'='events','schema_mode'='explicit','conversion_mode'='strict','max_parallelism'='1')",
		"create external table mongodb_ci.temporal_edges(ts datetime(0) mongodb_convert 'try_null') engine=mongodb with ('connection'='mongodb_ci','database'='mongodb_source','collection'='temporal_edges','schema_mode'='explicit','conversion_mode'='strict','max_parallelism'='1')",
		"create external table mongodb_ci.decoded_budget(payload_1 text mongodb_path 'payload', payload_2 text mongodb_path 'payload', payload_3 text mongodb_path 'payload', payload_4 text mongodb_path 'payload', payload_5 text mongodb_path 'payload', payload_6 text mongodb_path 'payload', payload_7 text mongodb_path 'payload', payload_8 text mongodb_path 'payload') engine=mongodb with ('connection'='mongodb_ci','database'='mongodb_source','collection'='decoded_budget','schema_mode'='explicit','conversion_mode'='strict','max_parallelism'='1')",
		"create external table mongodb_ci.json_scalar(value json, payload json, arr json) engine=mongodb with ('connection'='mongodb_ci','database'='mongodb_source','collection'='json_scalar','schema_mode'='explicit','conversion_mode'='strict','max_parallelism'='1')",
		"create external table mongodb_ci.binary_padding(id varchar(2) mongodb_path '_id', value binary(4)) engine=mongodb with ('connection'='mongodb_ci','database'='mongodb_source','collection'='binary_padding','schema_mode'='explicit','conversion_mode'='strict','max_parallelism'='1')",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("setup %s: %w", redact(statement), err)
		}
	}
	r.Cases = append(r.Cases, "secret-backed-ddl")
	if err := verifyShowMongoDBConnections(ctx, db); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "show-connections-admin-metadata-redaction")
	if err := verifyShowCreate(ctx, db); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "show-create-redaction-roundtrip")
	if dsn != "" {
		if err := verifyAuthorizationBoundary(ctx, db, dsn); err != nil {
			return err
		}
		r.Cases = append(r.Cases, "non-admin-marker-injection-boundary")
	}
	if err := expectScalar(ctx, db, "select cast(value as char) from mongodb_ci.json_scalar", `"text"`); err != nil {
		return err
	}
	if err := expectScalar(ctx, db, "select json_unquote(json_extract(payload, '$.a')) from mongodb_ci.json_scalar", "2"); err != nil {
		return err
	}
	if err := expectScalar(ctx, db, "select json_contains(arr, '2', '$') from mongodb_ci.json_scalar", "1"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "json-relaxed-extended-conversion")
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.binary_padding where octet_length(value) = 4", "4"); err != nil {
		return err
	}
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.binary_padding where binary value = _binary'a'", "0"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "fixed-binary-padding")

	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.events", "5"); err != nil {
		return err
	}
	if err := expectRows(ctx, db,
		"select mongo_id,device_id,site_id,cast(ts as char),coalesce(cast(measurement as char),'NULL'),coalesce(source_batch,'NULL') from mongodb_ci.events order by mongo_id",
		manifest.Rows); err != nil {
		return err
	}
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.events where measurement >= 14", "3"); err != nil {
		return err
	}
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.events where source_batch is null", "3"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "scan-projection-pushdown-null-conversion")

	// BSON DateTime preserves milliseconds, while DATETIME(0) truncates them.
	// The source predicate must therefore remain residual-only: an exact MongoDB
	// equality on 10:00:05.000 would incorrectly exclude this .100 source row.
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.temporal_edges where ts = '2026-07-27 10:00:05'", "1"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "low-precision-temporal-residual")

	// One BSON string is below max-value-bytes and the raw document is below
	// max-batch-bytes, but projecting it into eight vectors exceeds the decoded
	// batch budget. This guards the allocation amplification fixed by #26485.
	if err := expectQueryFailure(ctx, db,
		"select payload_1,payload_2,payload_3,payload_4,payload_5,payload_6,payload_7,payload_8 from mongodb_ci.decoded_budget",
		"decoded batch byte limit exceeded"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "decoded-vector-budget-enforced")

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := db.QueryRowContext(cancelCtx, "select count(*) from mongodb_ci.events").Scan(new(string)); err == nil {
		return fmt.Errorf("canceled MongoDB statement unexpectedly succeeded")
	}
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.events", "5"); err != nil {
		return fmt.Errorf("scan after cancellation: %w", err)
	}
	r.Cases = append(r.Cases, "multi-batch-cancel-recovery")

	windowSQL := "select count(*) from (select _wstart, device_id, site_id, avg(measurement) from mongodb_ci.events where device_id='device-001' group by device_id,site_id interval(ts,1,minute) gapfill(partition) fill(null)) x"
	if err := expectScalar(ctx, db, windowSQL, "4"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "mongoscan-timewin-gapfill")

	for _, statement := range []string{
		"create table mongodb_ci.minute_aggregate(device_id varchar(20), site_id varchar(10), window_start datetime, measurement double, primary key(device_id,site_id,window_start))",
		"create table mongodb_ci.ingest_watermark(id int primary key, high datetime)",
		"insert into mongodb_ci.ingest_watermark values(1,'2026-07-27 10:00:00')",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create ingestion tables: %w", err)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "replace into mongodb_ci.minute_aggregate select device_id,site_id,_wstart,avg(measurement) from mongodb_ci.events where ts >= '2026-07-27 10:00:00' and ts < '2026-07-27 10:03:00' group by device_id,site_id interval(ts,1,minute) gapfill(partition) fill(null)"); err == nil {
		_, err = tx.ExecContext(ctx, "update mongodb_ci.ingest_watermark set high='2026-07-27 10:03:00' where id=1")
	}
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("bounded ingestion: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err := expectScalar(ctx, db, "select cast(high as char) from mongodb_ci.ingest_watermark where id=1", "2026-07-27 10:03:00"); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "atomic-aggregate-watermark")

	// The fixture contains one malformed numeric value. A strict mapping must
	// abort the statement and leave both aggregate rows and watermark unchanged,
	// even if earlier source rows were already converted.
	if _, err := db.ExecContext(ctx, "create external table mongodb_ci.events_strict(mongo_id char(24) mongodb_path '_id', device_id varchar(20), site_id varchar(10), ts datetime(3), measurement double) engine=mongodb with ('connection'='mongodb_ci','database'='mongodb_source','collection'='events','schema_mode'='explicit','conversion_mode'='strict','max_parallelism'='1')"); err != nil {
		return fmt.Errorf("create strict mapping: %w", err)
	}
	var targetBefore, watermarkBefore string
	if err := db.QueryRowContext(ctx, "select count(*) from mongodb_ci.minute_aggregate").Scan(&targetBefore); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, "select cast(high as char) from mongodb_ci.ingest_watermark where id=1").Scan(&watermarkBefore); err != nil {
		return err
	}
	failedTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	_, scanErr := failedTx.ExecContext(ctx, "replace into mongodb_ci.minute_aggregate select device_id,site_id,_wstart,avg(measurement) from mongodb_ci.events_strict where ts >= '2026-07-27 10:00:00' and ts < '2026-07-27 10:04:00' group by device_id,site_id interval(ts,1,minute) gapfill(partition) fill(null)")
	if scanErr == nil {
		_ = failedTx.Rollback()
		return fmt.Errorf("strict MongoDB conversion unexpectedly succeeded")
	}
	_ = failedTx.Rollback()
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.minute_aggregate", targetBefore); err != nil {
		return fmt.Errorf("target rollback: %w", err)
	}
	if err := expectScalar(ctx, db, "select cast(high as char) from mongodb_ci.ingest_watermark where id=1", watermarkBefore); err != nil {
		return fmt.Errorf("watermark rollback: %w", err)
	}
	r.Cases = append(r.Cases, "conversion-error-atomic-rollback")

	// Repeating the committed bounded write is idempotent under the composite
	// result key and does not create additional aggregate rows.
	idempotentTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = idempotentTx.ExecContext(ctx, "replace into mongodb_ci.minute_aggregate select device_id,site_id,_wstart,avg(measurement) from mongodb_ci.events where ts >= '2026-07-27 10:00:00' and ts < '2026-07-27 10:03:00' group by device_id,site_id interval(ts,1,minute) gapfill(partition) fill(null)"); err == nil {
		_, err = idempotentTx.ExecContext(ctx, "update mongodb_ci.ingest_watermark set high='2026-07-27 10:03:00' where id=1")
	}
	if err != nil {
		_ = idempotentTx.Rollback()
		return fmt.Errorf("idempotent replay: %w", err)
	}
	if err = idempotentTx.Commit(); err != nil {
		return err
	}
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.minute_aggregate", targetBefore); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "bounded-idempotent-replay")

	// Rotating the secret reference increments the catalog generation. The next
	// statement must authenticate with the new read-only identity; package tests
	// assert that the old idle client is drained when this generation is leased.
	if _, err := db.ExecContext(ctx, "alter mongodb connection mongodb_ci set ('credential_secret_ref'='secret://env/MO_MONGODB_E2E_CREDENTIAL_NEXT')"); err != nil {
		return fmt.Errorf("rotate MongoDB connection: %w", err)
	}
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.events", "5"); err != nil {
		return fmt.Errorf("scan after credential rotation: %w", err)
	}
	r.Cases = append(r.Cases, "credential-generation-rotation")

	if _, err := db.ExecContext(ctx, "alter mongodb connection mongodb_ci disable"); err != nil {
		return fmt.Errorf("disable MongoDB connection: %w", err)
	}
	if err := expectQueryFailure(ctx, db, "select count(*) from mongodb_ci.events", "disabled"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "alter mongodb connection mongodb_ci enable"); err != nil {
		return fmt.Errorf("enable MongoDB connection: %w", err)
	}
	if err := expectScalar(ctx, db, "select count(*) from mongodb_ci.events", "5"); err != nil {
		return fmt.Errorf("scan after connection re-enable: %w", err)
	}
	r.Cases = append(r.Cases, "connection-disable-enable")
	return nil
}

func verifyAuthorizationBoundary(ctx context.Context, adminDB *sql.DB, dsn string) error {
	const (
		roleName = "mongodb_ci_creator"
		userName = "mongodb_ci_user"
		password = "mongodb_ci_password"
	)
	for _, statement := range []string{
		"drop user if exists " + userName,
		"drop role if exists " + roleName,
		"create role " + roleName,
		"create user " + userName + " identified by '" + password + "' default role " + roleName,
		"grant connect on account * to " + roleName,
		"grant create table on database mongodb_ci to " + roleName,
	} {
		if _, err := adminDB.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("authorization boundary setup %s: %w", statement, err)
		}
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = adminDB.ExecContext(cleanupCtx, "drop user if exists "+userName)
		_, _ = adminDB.ExecContext(cleanupCtx, "drop role if exists "+roleName)
	}()

	config, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse MatrixOne DSN: %w", err)
	}
	config.User = userName
	config.Passwd = password
	config.DBName = "mongodb_ci"
	userDB, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return fmt.Errorf("open non-admin MatrixOne session: %w", err)
	}
	defer userDB.Close()
	if err := userDB.PingContext(ctx); err != nil {
		return fmt.Errorf("connect non-admin MatrixOne session: %w", err)
	}
	if err := expectStatementRejected(ctx, userDB, "show mongodb connections", "do not have privilege"); err != nil {
		return fmt.Errorf("non-admin SHOW MONGODB CONNECTIONS boundary: %w", err)
	}

	if _, err := userDB.ExecContext(ctx,
		"create external table mongodb_ci.denied_mongodb(value bigint) engine=mongodb with ('connection'='mongodb_ci','database'='mongodb_source','collection'='events','schema_mode'='explicit','conversion_mode'='strict','max_parallelism'='1')"); err == nil {
		return fmt.Errorf("non-admin MongoDB table creation unexpectedly succeeded")
	}

	// Keep the E2E runner independent from MatrixOne's kernel packages: importing
	// the production envelope builder here pulls the kernel CGo dependency graph
	// into a runtime `go run`. This valid legacy envelope is deliberately local
	// test data; type_id 23 is BIGINT in the version-1 catalog encoding.
	const columnsJSON = `[{"name":"value","path":"measurement","type_id":23,"conversion":"strict"}]`
	marker := "/* MO_MONGODB: version=1; connection=mongodb_ci; database=mongodb_source; collection=events; " +
		"schema_mode=explicit; conversion_mode=strict; split_key=; max_parallelism=1; columns=" +
		url.QueryEscape(columnsJSON) + " */"
	// Before the parser boundary was anchored, a generic external-table filepath
	// containing this valid marker was mistaken for planner-owned MongoDB DDL.
	injectionSQL := "create external table mongodb_ci.marker_injection(value bigint) infile{\"filepath\"='" +
		strings.ReplaceAll(marker, "'", "''") + "'} fields terminated by ',' lines terminated by '\\n'"
	if _, err := userDB.ExecContext(ctx, injectionSQL); err != nil {
		return fmt.Errorf("generic marker-injection control table must remain creatable: %w", err)
	}
	if err := expectScalar(ctx, adminDB,
		"select count(*) from mo_catalog.mo_mongodb_tables m join mo_catalog.mo_tables t on m.account_id=t.account_id and m.table_id=t.rel_id where t.account_id=0 and t.reldatabase='mongodb_ci' and t.relname='marker_injection'",
		"0"); err != nil {
		return fmt.Errorf("generic marker injection created a MongoDB mapping: %w", err)
	}
	return nil
}

func loadFixtureManifest(path string) (fixtureManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fixtureManifest{}, fmt.Errorf("read MongoDB fixture manifest: %w", err)
	}
	var manifest fixtureManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fixtureManifest{}, fmt.Errorf("decode MongoDB fixture manifest: %w", err)
	}
	if len(manifest.Rows) == 0 {
		return fixtureManifest{}, fmt.Errorf("MongoDB fixture manifest has no rows")
	}
	return manifest, nil
}

func expectRows(ctx context.Context, db *sql.DB, query string, expected [][]string) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query %s: %w", redact(query), err)
	}
	defer rows.Close()
	actual := make([][]string, 0, len(expected))
	for rows.Next() {
		row := make([]string, 6)
		dest := make([]any, len(row))
		for i := range row {
			dest[i] = &row[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		actual = append(actual, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("fixture result mismatch: expected %v, got %v", expected, actual)
	}
	return nil
}

func verifyShowCreate(ctx context.Context, db *sql.DB) error {
	var tableName, ddl string
	if err := db.QueryRowContext(ctx, "show create table mongodb_ci.events").Scan(&tableName, &ddl); err != nil {
		return fmt.Errorf("SHOW CREATE MongoDB table: %w", err)
	}
	lower := strings.ToLower(ddl)
	if tableName != "events" || !strings.Contains(lower, "engine = mongodb") ||
		!strings.Contains(lower, "mongodb_path") || !strings.Contains(lower, "connection") {
		return fmt.Errorf("SHOW CREATE MongoDB table omitted reconstructable mapping metadata")
	}
	for _, forbidden := range []string{"secret://", "127.0.0.1", "mo_reader", "password", "credential"} {
		if strings.Contains(lower, forbidden) {
			return fmt.Errorf("SHOW CREATE MongoDB table exposed forbidden source metadata")
		}
	}
	return nil
}

func verifyShowMongoDBConnections(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "show mongodb connections")
	if err != nil {
		return fmt.Errorf("SHOW MONGODB CONNECTIONS: %w", err)
	}
	defer rows.Close()
	expectedColumns := []string{
		"name", "discovery_mode", "auth_mechanism", "tls_mode",
		"read_preference", "read_concern", "version", "disabled",
	}
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("SHOW MONGODB CONNECTIONS columns: %w", err)
	}
	if !reflect.DeepEqual(columns, expectedColumns) {
		return fmt.Errorf("SHOW MONGODB CONNECTIONS exposed unexpected columns: %v", columns)
	}

	found := false
	for rows.Next() {
		var actual [8]string
		dest := make([]any, len(actual))
		for i := range actual {
			dest[i] = &actual[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("SHOW MONGODB CONNECTIONS scan: %w", err)
		}
		if actual[0] != "mongodb_ci" {
			continue
		}
		found = true
		expected := [8]string{"mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", actual[6], "0"}
		if actual != expected {
			return fmt.Errorf("SHOW MONGODB CONNECTIONS metadata mismatch: expected %v, got %v", expected, actual)
		}
		version, err := strconv.ParseUint(actual[6], 10, 64)
		if err != nil || version == 0 {
			return fmt.Errorf("SHOW MONGODB CONNECTIONS returned invalid version %q", actual[6])
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("SHOW MONGODB CONNECTIONS rows: %w", err)
	}
	if !found {
		return fmt.Errorf("SHOW MONGODB CONNECTIONS omitted mongodb_ci")
	}
	return nil
}

func expectScalar(ctx context.Context, db *sql.DB, query, expected string) error {
	var actual string
	if err := db.QueryRowContext(ctx, query).Scan(&actual); err != nil {
		return fmt.Errorf("query %s: %w", redact(query), err)
	}
	if actual != expected {
		return fmt.Errorf("query %s: expected %q, got %q", redact(query), expected, actual)
	}
	return nil
}

func expectQueryFailure(ctx context.Context, db *sql.DB, query, contains string) error {
	var value string
	err := db.QueryRowContext(ctx, query).Scan(&value)
	if err == nil {
		return fmt.Errorf("query %s unexpectedly succeeded", redact(query))
	}
	if contains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(contains)) {
		return fmt.Errorf("query %s failed without %q: %w", redact(query), contains, err)
	}
	return nil
}

func expectStatementRejected(ctx context.Context, db *sql.DB, query, contains string) error {
	rows, err := db.QueryContext(ctx, query)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("query %s failed while reading rows: %w", redact(query), err)
		}
		return fmt.Errorf("query %s unexpectedly succeeded", redact(query))
	}
	if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(contains)) {
		return fmt.Errorf("query %s failed without %q: %w", redact(query), contains, err)
	}
	return nil
}

func redact(value string) string {
	if strings.Contains(strings.ToLower(value), "credential_secret_ref") {
		return "<redacted MongoDB DDL>"
	}
	return value
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
	summary := "## MongoDB connector E2E\n\nStatus: **" + value.Status + "**\n"
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(summary), 0o600)
}

//go:build issue28319_perf

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

var (
	issue28319PerfRunMode = flag.String("issue28319-run-mode", "embedded", "embedded or service")
	issue28319PerfMode    = flag.String("issue28319-mode", "query", "query, text, binary, or executor")
	issue28319PerfRunID   = flag.String("issue28319-run-id", "issue28319-local", "stable run identifier")
	issue28319PerfWorkers = flag.Int("issue28319-workers", 2, "number of concurrent table workers")
	issue28319PerfRows    = flag.Int("issue28319-rows", 8192, "rows in each source table")
	issue28319PerfWidth   = flag.Int("issue28319-width", 1024, "payload bytes in each source row")
	issue28319PerfRounds  = flag.Int("issue28319-rounds", 20, "ADD/DROP rounds per worker")
	issue28319PerfTimeout = flag.Duration("issue28319-timeout", 30*time.Second, "per ALTER request deadline")
	issue28319PerfOutput  = flag.String("issue28319-output", "", "directory for JSONL, CSV and manifest artifacts")
)

const issue28319PerfToolVersion = "issue28319-perf-driver-v1"

type issue28319PerfRecord struct {
	RunID         string `json:"run_id"`
	Mode          string `json:"mode"`
	Worker        int    `json:"worker"`
	Operation     string `json:"operation"`
	StartedAt     string `json:"started_at"`
	DurationNanos int64  `json:"duration_nanos"`
	Result        string `json:"result"`
	Error         string `json:"error,omitempty"`
	TxnAttempts   string `json:"txn_attempts"`
	CopyStarted   int    `json:"copy_started"`
	CopyCompleted int    `json:"copy_completed"`
	CopyRows      string `json:"copy_rows"`
	CopyBytes     string `json:"copy_bytes"`
}

type issue28319PerfManifest struct {
	ToolVersion    string `json:"tool_version"`
	RunID          string `json:"run_id"`
	RunMode        string `json:"run_mode"`
	Mode           string `json:"mode"`
	Workers        int    `json:"workers"`
	Rows           int    `json:"rows"`
	Width          int    `json:"width"`
	Rounds         int    `json:"rounds"`
	RequestTimeout string `json:"request_timeout"`
	SourceRevision string `json:"source_revision,omitempty"`
	Command        string `json:"command"`
	Database       string `json:"database"`
	Status         string `json:"status"`
	StartedAt      string `json:"started_at"`
	FinishedAt     string `json:"finished_at"`
	Error          string `json:"error,omitempty"`
}

type issue28319PerfWorkerConn struct {
	conn    *sql.Conn
	addStmt *sql.Stmt
}

type issue28319PerfWriters struct {
	mu        sync.Mutex
	directory string
	jsonFile  *os.File
	jsonEnc   *json.Encoder
	csvFile   *os.File
	csvWrite  *csv.Writer
	records   []issue28319PerfRecord
}

func (w *issue28319PerfWriters) write(record issue28319PerfRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.jsonEnc.Encode(record); err != nil {
		return err
	}
	if err := w.csvWrite.Write([]string{
		record.RunID,
		record.Mode,
		strconv.Itoa(record.Worker),
		record.Operation,
		record.StartedAt,
		strconv.FormatInt(record.DurationNanos, 10),
		record.Result,
		record.Error,
		record.TxnAttempts,
		strconv.Itoa(record.CopyStarted),
		strconv.Itoa(record.CopyCompleted),
		record.CopyRows,
		record.CopyBytes,
	}); err != nil {
		return err
	}
	w.records = append(w.records, record)
	return nil
}

func (w *issue28319PerfWriters) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.csvWrite.Flush()
	var closeErr error
	closeErr = errors.Join(closeErr, w.csvWrite.Error())
	closeErr = errors.Join(closeErr, w.jsonFile.Close())
	closeErr = errors.Join(closeErr, w.csvFile.Close())
	summaryFile, err := os.Create(filepath.Join(w.directory, "summary.csv"))
	if err != nil {
		return errors.Join(closeErr, err)
	}
	summaryWrite := csv.NewWriter(summaryFile)
	if err := summaryWrite.Write([]string{"metric", "value"}); err != nil {
		_ = summaryFile.Close()
		return errors.Join(closeErr, err)
	}
	durations := make([]int64, 0, len(w.records))
	errorsCount, copyStarted, copyCompleted := 0, 0, 0
	for _, record := range w.records {
		durations = append(durations, record.DurationNanos)
		if record.Result != "OK" {
			errorsCount++
		}
		copyStarted += record.CopyStarted
		copyCompleted += record.CopyCompleted
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	percentile := func(fraction float64) int64 {
		if len(durations) == 0 {
			return 0
		}
		index := int(float64(len(durations)-1) * fraction)
		return durations[index]
	}
	for _, metric := range [][2]string{
		{"records", strconv.Itoa(len(w.records))},
		{"errors", strconv.Itoa(errorsCount)},
		{"copy_started", strconv.Itoa(copyStarted)},
		{"copy_completed", strconv.Itoa(copyCompleted)},
		{"p50_duration_nanos", strconv.FormatInt(percentile(0.50), 10)},
		{"p95_duration_nanos", strconv.FormatInt(percentile(0.95), 10)},
		{"max_duration_nanos", strconv.FormatInt(percentile(1.00), 10)},
	} {
		if err := summaryWrite.Write(metric[:]); err != nil {
			closeErr = errors.Join(closeErr, err)
			break
		}
	}
	summaryWrite.Flush()
	closeErr = errors.Join(closeErr, summaryWrite.Error(), summaryFile.Close())
	return closeErr
}

type issue28319PerfCopyStats struct {
	starts    int
	completed int
	txnIDs    map[string]struct{}
}

type issue28319PerfObserver struct {
	mu      sync.Mutex
	byTable map[string]issue28319PerfCopyStats
}

func newIssue28319PerfObserver(database string) (*issue28319PerfObserver, func()) {
	observer := &issue28319PerfObserver{byTable: make(map[string]issue28319PerfCopyStats)}
	restore := compile.SetAlterCopyPhaseHookForTest(func(_ context.Context, dbName, table, phase string, op client.TxnOperator) error {
		if dbName != database || !strings.HasPrefix(table, "t_") {
			return nil
		}
		if phase != "copy-started" && phase != "data-copied" {
			return nil
		}
		observer.mu.Lock()
		stats := observer.byTable[table]
		if stats.txnIDs == nil {
			stats.txnIDs = make(map[string]struct{})
		}
		if phase == "copy-started" {
			stats.starts++
		}
		if phase == "data-copied" {
			stats.completed++
		}
		if op != nil {
			stats.txnIDs[fmt.Sprintf("%x", op.Txn().ID)] = struct{}{}
		}
		observer.byTable[table] = stats
		observer.mu.Unlock()
		return nil
	})
	return observer, restore
}

func (o *issue28319PerfObserver) snapshot(table string) issue28319PerfCopyStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	stats := o.byTable[table]
	ids := make(map[string]struct{}, len(stats.txnIDs))
	for id := range stats.txnIDs {
		ids[id] = struct{}{}
	}
	stats.txnIDs = ids
	return stats
}

func issue28319PerfSafeName(value string) string {
	value = strings.ToLower(value)
	value = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, value)
	value = strings.Trim(value, "_")
	if value == "" {
		value = "run"
	}
	if len(value) > 32 {
		value = value[:32]
	}
	return value
}

func issue28319PerfOpenWriters(t *testing.T, directory string) (*issue28319PerfWriters, string) {
	t.Helper()
	if directory == "" {
		directory = t.TempDir()
	}
	require.NoError(t, os.MkdirAll(directory, 0o755))
	jsonFile, err := os.Create(filepath.Join(directory, "requests.jsonl"))
	require.NoError(t, err)
	csvFile, err := os.Create(filepath.Join(directory, "requests.csv"))
	if err != nil {
		jsonFile.Close()
		require.NoError(t, err)
	}
	writers := &issue28319PerfWriters{
		directory: directory,
		jsonFile:  jsonFile,
		jsonEnc:   json.NewEncoder(jsonFile),
		csvFile:   csvFile,
		csvWrite:  csv.NewWriter(csvFile),
	}
	require.NoError(t, writers.csvWrite.Write([]string{
		"run_id", "mode", "worker", "operation", "started_at", "duration_nanos",
		"result", "error", "txn_attempts", "copy_started", "copy_completed",
		"copy_rows", "copy_bytes",
	}))
	return writers, directory
}

func issue28319PerfWriteManifest(path string, manifest issue28319PerfManifest) error {
	file, err := os.Create(filepath.Join(path, "manifest.json"))
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(manifest)
}

func TestIssue28319Performance(t *testing.T) {
	if *issue28319PerfWorkers < 1 || *issue28319PerfWorkers > 32 {
		t.Fatalf("workers must be between 1 and 32, got %d", *issue28319PerfWorkers)
	}
	if *issue28319PerfRows < 1 || *issue28319PerfWidth < 1 || *issue28319PerfRounds < 1 {
		t.Fatalf("rows, width and rounds must be positive")
	}
	if *issue28319PerfTimeout <= 0 {
		t.Fatalf("timeout must be positive")
	}
	if *issue28319PerfMode != "query" && *issue28319PerfMode != "text" &&
		*issue28319PerfMode != "binary" && *issue28319PerfMode != "executor" {
		t.Fatalf("unsupported mode %q", *issue28319PerfMode)
	}
	if *issue28319PerfRunMode != "embedded" && *issue28319PerfRunMode != "service" {
		t.Fatalf("unsupported run mode %q", *issue28319PerfRunMode)
	}
	if *issue28319PerfRunMode == "service" && *issue28319PerfMode == "executor" {
		t.Fatalf("executor mode is available only in embedded run mode")
	}

	writers, outputDir := issue28319PerfOpenWriters(t, *issue28319PerfOutput)
	startedAt := time.Now()
	manifest := issue28319PerfManifest{
		ToolVersion:    issue28319PerfToolVersion,
		RunID:          *issue28319PerfRunID,
		RunMode:        *issue28319PerfRunMode,
		Mode:           *issue28319PerfMode,
		Workers:        *issue28319PerfWorkers,
		Rows:           *issue28319PerfRows,
		Width:          *issue28319PerfWidth,
		Rounds:         *issue28319PerfRounds,
		RequestTimeout: issue28319PerfTimeout.String(),
		SourceRevision: os.Getenv("MO_ISSUE28319_SOURCE_SHA"),
		Command:        strings.Join(os.Args, " "),
		Status:         "RUNNING",
		StartedAt:      startedAt.Format(time.RFC3339Nano),
	}
	status := "PASS"
	var runErr error
	defer func() {
		if runErr != nil || t.Failed() {
			status = "FAIL"
		}
		manifest.Status = status
		manifest.FinishedAt = time.Now().Format(time.RFC3339Nano)
		if runErr != nil {
			manifest.Error = runErr.Error()
		}
		if err := writers.close(); err != nil {
			t.Errorf("close performance artifacts: %v", err)
		}
		if err := issue28319PerfWriteManifest(outputDir, manifest); err != nil {
			t.Errorf("write performance manifest: %v", err)
		}
		t.Logf("issue28319 performance artifacts: %s", outputDir)
	}()

	if *issue28319PerfRunMode == "service" {
		runErr = issue28319PerfService(t, writers, &manifest)
		if runErr != nil {
			t.Error(runErr)
		}
		return
	}
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		runErr = issue28319PerfEmbedded(t, cluster, writers, &manifest)
		if runErr != nil {
			t.Error(runErr)
		}
	})
}

func issue28319PerfEmbedded(t *testing.T, cluster embed.Cluster, writers *issue28319PerfWriters, manifest *issue28319PerfManifest) error {
	cn, err := cluster.GetCNService(0)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
	if err != nil {
		return err
	}
	defer db.Close()
	// Keep one spare pooled connection for the post-run consistency checks while
	// each worker holds its own connection for the whole measurement.
	db.SetMaxOpenConns(*issue28319PerfWorkers + 1)
	database := "issue_28319_perf_" + issue28319PerfSafeName(*issue28319PerfRunID)
	manifest.Database = database
	return issue28319PerfRun(t, db, cn, writers, manifest)
}

func issue28319PerfService(t *testing.T, writers *issue28319PerfWriters, manifest *issue28319PerfManifest) error {
	dsn := os.Getenv("MO_ISSUE28319_DSN")
	if dsn == "" {
		return errors.New("MO_ISSUE28319_DSN is required for service mode")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(*issue28319PerfWorkers + 1)
	database := "issue_28319_perf_" + issue28319PerfSafeName(*issue28319PerfRunID)
	manifest.Database = database
	return issue28319PerfRun(t, db, nil, writers, manifest)
}

func issue28319PerfRun(t *testing.T, db *sql.DB, cn embed.ServiceOperator, writers *issue28319PerfWriters, manifest *issue28319PerfManifest) (runErr error) {
	totalRequests := *issue28319PerfWorkers * *issue28319PerfRounds * 2
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(totalRequests+8)*(*issue28319PerfTimeout))
	defer cancel()
	database := manifest.Database
	if _, err := db.ExecContext(ctx, "drop database if exists "+database); err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := db.ExecContext(cleanupCtx, "drop database if exists "+database); runErr == nil && err != nil {
			runErr = err
		}
	}()
	if _, err := db.ExecContext(ctx, "create database "+database); err != nil {
		return err
	}
	for worker := 0; worker < *issue28319PerfWorkers; worker++ {
		table := fmt.Sprintf("t_%d", worker)
		if _, err := db.ExecContext(ctx, fmt.Sprintf("create table %s.%s(id int not null, v varchar(%d))", database, table, *issue28319PerfWidth)); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("insert into %s.%s select result, repeat('x',%d) from generate_series(1,%d) g", database, table, *issue28319PerfWidth, *issue28319PerfRows)); err != nil {
			return err
		}
	}

	var observer *issue28319PerfObserver
	var restore func()
	if cn != nil {
		observer, restore = newIssue28319PerfObserver(database)
	}
	if restore != nil {
		defer func() {
			if restore != nil {
				restore()
			}
		}()
	}
	workers := make([]issue28319PerfWorkerConn, *issue28319PerfWorkers)
	defer func() {
		for worker := range workers {
			if workers[worker].addStmt != nil {
				workers[worker].addStmt.Close()
			}
			if *issue28319PerfMode == "text" && workers[worker].conn != nil {
				_, _ = workers[worker].conn.ExecContext(context.Background(), fmt.Sprintf("deallocate prepare issue_28319_perf_add_%d", worker))
			}
			if workers[worker].conn != nil {
				workers[worker].conn.Close()
			}
		}
	}()
	for worker := range workers {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		workers[worker].conn = conn
		if *issue28319PerfMode == "binary" {
			addStmt, prepareErr := conn.PrepareContext(ctx, issue28319PerfAlterSQL(database, worker, "add"))
			if prepareErr != nil {
				conn.Close()
				return prepareErr
			}
			workers[worker].addStmt = addStmt
		} else if *issue28319PerfMode == "text" {
			addPrepareSQL := fmt.Sprintf("prepare issue_28319_perf_add_%d from '%s'", worker,
				strings.ReplaceAll(issue28319PerfAlterSQL(database, worker, "add"), "'", "''"))
			if _, prepareErr := conn.ExecContext(ctx, addPrepareSQL); prepareErr != nil {
				conn.Close()
				return prepareErr
			}
		}
	}

	start := make(chan struct{})
	ready := make(chan struct{}, len(workers))
	results := make(chan error, len(workers))
	var wg sync.WaitGroup
	for worker := range workers {
		worker, workerConn := worker, workers[worker]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			for round := 0; round < *issue28319PerfRounds; round++ {
				for _, operation := range []string{"add_primary_key", "drop_primary_key"} {
					table := fmt.Sprintf("t_%d", worker)
					before := issue28319PerfCopyStats{}
					if observer != nil {
						before = observer.snapshot(table)
					}
					requestStarted := time.Now()
					requestCtx, requestCancel := context.WithTimeout(ctx, *issue28319PerfTimeout)
					err := issue28319PerfExec(requestCtx, cn, workerConn, database, worker, operation)
					requestCancel()
					after := before
					if observer != nil {
						after = observer.snapshot(table)
					}
					txnAttempts := "UNAVAILABLE"
					if observer != nil {
						txnAttempts = strconv.Itoa(len(after.txnIDs) - len(before.txnIDs))
					}
					record := issue28319PerfRecord{
						RunID:         *issue28319PerfRunID,
						Mode:          *issue28319PerfMode,
						Worker:        worker,
						Operation:     operation,
						StartedAt:     requestStarted.Format(time.RFC3339Nano),
						DurationNanos: time.Since(requestStarted).Nanoseconds(),
						Result:        "OK",
						TxnAttempts:   txnAttempts,
						CopyRows:      "UNAVAILABLE",
						CopyBytes:     "UNAVAILABLE",
					}
					if err != nil {
						record.Result = "ERROR"
						record.Error = err.Error()
					}
					if observer != nil {
						record.CopyStarted = after.starts - before.starts
						record.CopyCompleted = after.completed - before.completed
					}
					if writeErr := writers.write(record); writeErr != nil {
						results <- writeErr
						return
					}
					if err != nil {
						results <- fmt.Errorf("worker %d %s: %w", worker, operation, err)
						return
					}
				}
			}
			results <- nil
		}()
	}
	for range workers {
		<-ready
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil && runErr == nil {
			runErr = err
		}
	}
	if runErr != nil {
		return runErr
	}

	if restore != nil {
		restore()
		restore = nil
	}
	expected := *issue28319PerfWorkers * *issue28319PerfRounds * 2
	if observer != nil {
		starts, completed := 0, 0
		for worker := range workers {
			stats := observer.snapshot(fmt.Sprintf("t_%d", worker))
			starts += stats.starts
			completed += stats.completed
		}
		if starts != expected || completed != expected {
			return fmt.Errorf("copy accounting mismatch: started=%d completed=%d expected=%d", starts, completed, expected)
		}
	}
	for worker := range workers {
		table := fmt.Sprintf("t_%d", worker)
		var rows, ids, payload int
		if err := db.QueryRowContext(ctx, "select count(*),count(distinct id),coalesce(sum(length(v)),0) from "+database+"."+table).Scan(&rows, &ids, &payload); err != nil {
			return err
		}
		if rows != *issue28319PerfRows || ids != *issue28319PerfRows || payload != *issue28319PerfRows**issue28319PerfWidth {
			return fmt.Errorf("table %s data mismatch: rows=%d ids=%d payload=%d", table, rows, ids, payload)
		}
		var primaryColumns int
		if err := db.QueryRowContext(ctx, "select count(*) from information_schema.columns where table_schema=? and table_name=? and column_key='PRI'", database, table).Scan(&primaryColumns); err != nil {
			return err
		}
		if primaryColumns != 0 {
			return fmt.Errorf("table %s retains a primary key after the ADD/DROP workload", table)
		}
	}
	var tableCount int
	if err := db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=?", database).Scan(&tableCount); err != nil {
		return err
	}
	if tableCount != *issue28319PerfWorkers {
		return fmt.Errorf("temporary table cleanup mismatch: tables=%d expected=%d", tableCount, *issue28319PerfWorkers)
	}
	if _, err := db.ExecContext(ctx, "alter table "+database+".t_0 algorithm=copy, add primary key(id)"); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "alter table "+database+".t_0 algorithm=copy, drop primary key")
	return err
}

func issue28319PerfAlterSQL(database string, worker int, operation string) string {
	table := fmt.Sprintf("t_%d", worker)
	if operation == "drop" {
		return "alter table " + database + "." + table + " algorithm=copy, drop primary key"
	}
	return "alter table " + database + "." + table + " algorithm=copy, add primary key(id)"
}

func issue28319PerfExec(ctx context.Context, cn embed.ServiceOperator, worker issue28319PerfWorkerConn, database string, workerID int, operation string) error {
	statement := fmt.Sprintf("alter table %s.t_%d algorithm=copy, ", database, workerID)
	if operation == "add_primary_key" {
		statement += "add primary key(id)"
	} else {
		statement += "drop primary key"
	}
	switch *issue28319PerfMode {
	case "query":
		_, err := worker.conn.ExecContext(ctx, statement)
		return err
	case "binary":
		if operation == "drop_primary_key" {
			_, err := worker.conn.ExecContext(ctx, statement)
			return err
		}
		_, err := worker.addStmt.ExecContext(ctx)
		return err
	case "text":
		if operation == "drop_primary_key" {
			_, err := worker.conn.ExecContext(ctx, statement)
			return err
		}
		name := fmt.Sprintf("issue_28319_perf_add_%d", workerID)
		_, err := worker.conn.ExecContext(ctx, "execute "+name)
		return err
	case "executor":
		if cn == nil {
			return errors.New("executor mode requires an embedded CN")
		}
		result, err := testutils.GetSQLExecutor(cn).Exec(ctx, statement, executor.Options{}.WithAccountID(0).WithDatabase(database))
		result.Close()
		return err
	default:
		return fmt.Errorf("unsupported mode %q", *issue28319PerfMode)
	}
}

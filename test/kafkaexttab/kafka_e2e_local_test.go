// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The scripted driver simulates the Kafka external-table read semantics over
// an in-memory message log, so the complete E2E script (run()) executes as a
// unit test with no MatrixOne and no broker. The simulator IS the oracle the
// real stack must match: message i (id i+1) sits at offset i, a read starts
// after __mo_read_start_id (-1 = everything), __mo_read_size caps the count,
// and last_kafka_message_id() is per-connection state set only by non-empty
// reads. If run()'s expectations and this simulator drift, the test fails.

// simHigh is the simulated log: offsets 0..simHigh-1 exist. The fake seed
// grows it. Single-goroutine test — package state is race-free.
var simHigh int64

func simSeed(_ context.Context, _ string, first, n int) error {
	simHigh += int64(n)
	_ = first
	return nil
}

var (
	reStart = regexp.MustCompile(`__mo_read_start_id = (-?\d+)`)
	reSize  = regexp.MustCompile(`__mo_read_size = (\d+)`)
)

// simRange resolves a query's read window [lo, hi] of offsets; ok=false for
// an empty read.
func simRange(q string) (int64, int64, bool) {
	lo := int64(0)
	if m := reStart.FindStringSubmatch(q); m != nil {
		start, _ := strconv.ParseInt(m[1], 10, 64)
		if start >= 0 {
			lo = start + 1
		}
	}
	hi := simHigh - 1
	if m := reSize.FindStringSubmatch(q); m != nil {
		size, _ := strconv.ParseInt(m[1], 10, 64)
		if lo+size-1 < hi {
			hi = lo + size - 1
		}
	}
	if lo > hi {
		return 0, 0, false
	}
	return lo, hi, true
}

type simDriver struct{}
type simConn struct {
	lastID  int64
	lastSet bool
	// explicit-transaction modeling: reads inside an open transaction stage
	// their effects; COMMIT applies them, ROLLBACK drops them
	txnOpen     bool
	pendLast    int64
	pendLastSet bool
	pendDst     int64
}

// simDstRows models the kafka_e2e.dst_txn destination table.
var simDstRows int64

// simGTxOffset models the g_tx group's committed offset (-1 = none).
var simGTxOffset int64 = -1

type simStmt struct {
	conn *simConn
	q    string
}
type simRows struct {
	cols []string
	rows [][]driver.Value
	next int
}

func (simDriver) Open(string) (driver.Conn, error)       { return &simConn{}, nil }
func (c *simConn) Prepare(q string) (driver.Stmt, error) { return &simStmt{conn: c, q: q}, nil }
func (c *simConn) Close() error                          { return nil }
func (c *simConn) Begin() (driver.Tx, error)             { return nil, driver.ErrSkip }
func (s *simStmt) Close() error                          { return nil }
func (s *simStmt) NumInput() int                         { return 0 }
func (s *simStmt) Exec([]driver.Value) (driver.Result, error) {
	q := s.q
	switch {
	case q == "BEGIN":
		s.conn.txnOpen = true
	case q == "COMMIT":
		if s.conn.pendLastSet {
			s.conn.lastID, s.conn.lastSet = s.conn.pendLast, true
			simGTxOffset = s.conn.pendLast + 1
		}
		simDstRows += s.conn.pendDst
		s.conn.txnOpen, s.conn.pendLastSet, s.conn.pendDst = false, false, 0
	case q == "ROLLBACK":
		s.conn.txnOpen, s.conn.pendLastSet, s.conn.pendDst = false, false, 0
	case strings.Contains(q, "insert into kafka_e2e.kdest (id, name, amount, ts) select"):
		// no error columns in the projection: the scan fails on the first
		// malformed message, as it did before error mode existed
		return nil, fmt.Errorf("internal error: the input value 'abc' is not int32 type for column 0")
	case strings.Contains(q, "insert into kafka_e2e.dst_txn select"):
		lo, hi, ok := simRange(q)
		if ok {
			s.conn.pendLast, s.conn.pendLastSet = hi, true
			s.conn.pendDst += hi - lo + 1
		}
	}
	return driver.ResultNoRows, nil
}

func (s *simStmt) Query([]driver.Value) (driver.Rows, error) {
	q := s.q
	// server-side chaining: the builtin as the control value resolves to this
	// session's last id
	if strings.Contains(q, "__mo_read_start_id = last_kafka_message_id()") && s.conn.lastSet {
		q = strings.Replace(q, "__mo_read_start_id = last_kafka_message_id()",
			fmt.Sprintf("__mo_read_start_id = %d", s.conn.lastID), 1)
	}
	one := func(cols []string, vals ...driver.Value) (driver.Rows, error) {
		return &simRows{cols: cols, rows: [][]driver.Value{vals}}, nil
	}
	switch {
	case strings.Contains(q, "count(*) from kafka_e2e.kdest"):
		return one([]string{"c"}, int64(simErrModeGood()))
	case strings.Contains(q, "count(*) from kafka_e2e.krejects"):
		return one([]string{"c"}, int64(len(simErrModeRejects())))
	case strings.Contains(q, "msg_id, txt from kafka_e2e.krejects"):
		rows := make([][]driver.Value, 0, 8)
		for _, off := range simErrModeRejects() {
			rows = append(rows, []driver.Value{int64(off), []byte(errModeMessages[off])})
		}
		return &simRows{cols: []string{"msg_id", "txt"}, rows: rows}, nil
	case strings.Contains(q, "__mo_file_line is null"):
		// a kafka record has no line in a file, so every failed row has NULL
		return one([]string{"c"}, int64(len(simErrModeRejects())))
	case strings.Contains(q, "count(*) from kafka_e2e.dst_txn"):
		return one([]string{"c"}, simDstRows)
	case strings.Contains(q, "last_kafka_message_id"):
		if !s.conn.lastSet {
			return one([]string{"v"}, nil)
		}
		return one([]string{"v"}, s.conn.lastID)
	case strings.Contains(q, "select 1") || q == "SELECT 1": // Ping fallback
		return one([]string{"1"}, int64(1))
	case strings.HasPrefix(q, "select * from"):
		// declared columns only; one row is enough for the column check
		return one([]string{"id", "name"}, int64(1), []byte("item1"))
	case strings.Contains(q, "korders_auto"):
		// autocommit table: default start reads the whole log, every time
		s.conn.lastID, s.conn.lastSet = simHigh-1, true
		return one([]string{"c"}, simHigh)
	case strings.Contains(q, "count(*)") && strings.Contains(q, "__mo_message_key is null"):
		return one([]string{"c"}, simHigh) // no keys are ever produced
	case strings.Contains(q, "count(*), coalesce(min(__mo_message_id)"):
		lo, hi, ok := simRange(q)
		if !ok {
			return one([]string{"c", "mn", "mx"}, int64(0), int64(-1), int64(-1))
		}
		s.conn.lastID, s.conn.lastSet = hi, true
		return one([]string{"c", "mn", "mx"}, hi-lo+1, lo, hi)
	case strings.Contains(q, "sum(id)"):
		lo, hi, ok := simRange(q)
		if !ok {
			return one([]string{"s"}, nil)
		}
		s.conn.lastID, s.conn.lastSet = hi, true
		var sum int64
		for o := lo; o <= hi; o++ {
			sum += o + 1 // offset o holds id o+1
		}
		return one([]string{"s"}, sum)
	default:
		return nil, fmt.Errorf("simulator does not understand query: %s", q)
	}
}

func (r *simRows) Columns() []string { return r.cols }
func (r *simRows) Close() error      { return nil }
func (r *simRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

// TestRunAgainstSimulator executes the complete E2E script against the
// offset-semantics simulator: every case the real broker run records must be
// recorded here too, in the same order.
func TestRunAgainstSimulator(t *testing.T) {
	sql.Register("kafka-e2e-sim", simDriver{})
	db, err := sql.Open("kafka-e2e-sim", "any")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	simHigh = 0
	simDstRows = 0
	simGTxOffset = -1
	committedOffsetFn = func(group string) (int64, bool) {
		if group == "g_tx" && simGTxOffset >= 0 {
			return simGTxOffset, true
		}
		return 0, false
	}
	defer func() { committedOffsetFn = nil }()

	var r report
	if err := run(context.Background(), db, "127.0.0.1:9092", simSeed, &r); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{
		"setup-seed", "read-to-end-timeout-clean", "exactly-once-chaining",
		"repeated-start-id-replays", "pickup-after-produce",
		"zero-messages-null-and-preserved", "metadata-key-null",
		"autocommit-earliest-and-last-id", "txn-rollback-discards-commit-publishes",
	}
	if len(r.Cases) != len(want) {
		t.Fatalf("cases = %v, want %v", r.Cases, want)
	}
	for i := range want {
		if r.Cases[i] != want[i] {
			t.Fatalf("case[%d] = %q, want %q", i, r.Cases[i], want[i])
		}
	}
}

func TestWriteReportAndWaitForMO(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "rep")
	if err := writeReport(dir, report{Status: "passed", Cases: []string{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"passed"`) {
		t.Fatalf("report: %s", data)
	}
	sum, err := os.ReadFile(filepath.Join(dir, "summary.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sum), "a, b") {
		t.Fatalf("summary: %s", sum)
	}

	sql.Register("kafka-e2e-sim-ping", simDriver{})
	db, err := sql.Open("kafka-e2e-sim-ping", "any")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := waitForMO(context.Background(), db); err != nil {
		t.Fatal(err)
	}
}

// TestSeedKafka runs the real seeding path against an in-process kfake
// broker: topic creation (idempotent) and ordered production.
func TestSeedKafka(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := c.ListenAddrs()[0]
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := seedKafka(ctx, addr, 1, 3); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// second call: topic already exists, production continues at offset 3
	if err := seedKafka(ctx, addr, 4, 2); err != nil {
		t.Fatalf("re-seed: %v", err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	fetchCtx, fcancel := context.WithTimeout(ctx, 20*time.Second)
	defer fcancel()
	var got []string
	for len(got) < 5 {
		fetches := cl.PollFetches(fetchCtx)
		if err := fetchCtx.Err(); err != nil {
			t.Fatalf("only fetched %v: %v", got, err)
		}
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	want := []string{"1,item1", "2,item2", "3,item3", "4,item4", "5,item5"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// simErrModeRejects lists the offsets of errModeMessages that a real scan
// reports as failures, and simErrModeGood counts the rest. This encodes what
// the server does with each message shape -- a non-numeric int, an unparsable
// timestamp, too few and too many fields, and an empty value are failures --
// so the simulator is the oracle runErrorMode's assertions are checked
// against, exactly as simRange is for run().
var simErrModeRejectsFn = func() []int { return []int{1, 2, 3, 4, 5, 6} }

func simErrModeRejects() []int { return simErrModeRejectsFn() }

func simErrModeGood() int { return len(errModeMessages) - len(simErrModeRejects()) }

// TestRunErrorModeAgainstSimulator executes the error-mode scenario with no
// MatrixOne and no broker. It proves the driver's own control flow and
// assertions; the same script runs against a real broker in
// optools/kafka_ci.bash, which is what proves the server behaviour.
func TestRunErrorModeAgainstSimulator(t *testing.T) {
	sql.Register("kafka-e2e-sim-err", simDriver{})
	db, err := sql.Open("kafka-e2e-sim-err", "any")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var seeded []string
	seedRawFn = func(_ context.Context, _, tp string, values []string) error {
		if tp != errTopic {
			t.Fatalf("seeded topic %q, want %q", tp, errTopic)
		}
		seeded = values
		return nil
	}
	defer func() { seedRawFn = seedKafkaRaw }()

	var r report
	if err := runErrorMode(context.Background(), db, "127.0.0.1:9092", &r); err != nil {
		t.Fatalf("runErrorMode: %v", err)
	}
	if len(seeded) != len(errModeMessages) {
		t.Fatalf("seeded %d messages, want %d", len(seeded), len(errModeMessages))
	}
	want := []string{
		"error-mode-split", "error-mode-message-id",
		"error-mode-no-file-line", "error-mode-pruned-still-fails",
	}
	if len(r.Cases) != len(want) {
		t.Fatalf("cases = %v, want %v", r.Cases, want)
	}
	for i := range want {
		if r.Cases[i] != want[i] {
			t.Fatalf("case[%d] = %q, want %q", i, r.Cases[i], want[i])
		}
	}
}

// TestRunErrorModeDetectsAWrongSplit: the assertions are load-bearing, not
// decoration -- if the server ever stopped reporting one of the malformed
// messages, runErrorMode must fail rather than pass quietly.
func TestRunErrorModeDetectsAWrongSplit(t *testing.T) {
	sql.Register("kafka-e2e-sim-err2", simDriver{})
	db, err := sql.Open("kafka-e2e-sim-err2", "any")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seedRawFn = func(context.Context, string, string, []string) error { return nil }
	defer func() { seedRawFn = seedKafkaRaw }()

	orig := simErrModeRejectsFn
	simErrModeRejectsFn = func() []int { return []int{1, 2, 3, 4, 5} } // one failure missed
	defer func() { simErrModeRejectsFn = orig }()

	var r report
	if err := runErrorMode(context.Background(), db, "127.0.0.1:9092", &r); err == nil {
		t.Fatal("runErrorMode accepted a split that lost a malformed message")
	}
}

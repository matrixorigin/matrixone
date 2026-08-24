// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command kafka_e2e_local drives the Kafka external-table end-to-end test
// (docs/cn/kafka_exttab.md, issue #27518). It connects to a running MatrixOne
// over a MySQL DSN and to a real Kafka broker (seeded by itself via
// franz-go), and proves the exactly-once read semantics:
//
//   - a read that drains the stream and ends by TIMEOUT is not an error and
//     still records the correct LAST_KAFKA_MESSAGE_ID()
//   - chaining reads by feeding LAST_KAFKA_MESSAGE_ID() back as the next
//     __mo_read_start_id covers the stream exactly once (no overlap, no gap)
//   - repeating the same __mo_read_start_id returns the same data
//   - after new messages arrive, the next chained read picks up at the right
//     offset
//   - a read that returns 0 messages leaves LAST_KAFKA_MESSAGE_ID() NULL in
//     a fresh session, and UNCHANGED in a session that read before
//
// LAST_KAFKA_MESSAGE_ID() is session state, so every scenario pins one
// database/sql connection (db.Conn) — the pool must not swap sessions
// mid-scenario. It is launched by optools/kafka_ci.bash; run via `go run`.
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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type report struct {
	Status string   `json:"status"`
	Cases  []string `json:"cases"`
	Error  string   `json:"error,omitempty"`
}

const (
	topic = "orders"
	table = "kafka_e2e.korders"
)

func main() {
	var dsn, bootstrap, reportDir string
	flag.StringVar(&dsn, "dsn", "root:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=60s&writeTimeout=60s", "MO DSN")
	flag.StringVar(&bootstrap, "bootstrap", "127.0.0.1:9092", "Kafka bootstrap host:port")
	flag.StringVar(&reportDir, "report-dir", "test/kafkaexttab/reports/local", "report directory")
	flag.Parse()

	r := report{Status: "failed"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err == nil {
		defer db.Close()
		err = waitForMO(ctx, db)
	}
	if err == nil {
		err = run(ctx, db, bootstrap, seedKafka, &r)
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

// seedKafka creates the topic (1 partition) and produces n csv messages
// "id,item<id>" with ids first..first+n-1. It retries topic creation until
// the broker is ready. Offsets are assigned by the broker in produce order.
func seedKafka(ctx context.Context, bootstrap string, first, n int) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, err = adm.CreateTopics(ctx, 1, 1, nil, topic)
		if err == nil || strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("create topic: %w", err)
		}
		time.Sleep(2 * time.Second)
	}

	for i := first; i < first+n; i++ {
		rec := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("%d,item%d", i, i))}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			return fmt.Errorf("produce %d: %w", i, err)
		}
	}
	return nil
}

// readStats runs one positioned read and returns (rowCount, minMsgID,
// maxMsgID); min/max are -1 for an empty read.
func readStats(ctx context.Context, conn *sql.Conn, where string) (int64, int64, int64, error) {
	q := "select count(*), coalesce(min(__mo_message_id), -1), coalesce(max(__mo_message_id), -1) from " +
		table + " where " + where
	var cnt, minID, maxID int64
	if err := conn.QueryRowContext(ctx, q).Scan(&cnt, &minID, &maxID); err != nil {
		return 0, 0, 0, fmt.Errorf("query %s: %w", q, err)
	}
	return cnt, minID, maxID, nil
}

// lastID reads LAST_KAFKA_MESSAGE_ID() on this session; valid=false is NULL.
func lastID(ctx context.Context, conn *sql.Conn) (int64, bool, error) {
	var v sql.NullInt64
	if err := conn.QueryRowContext(ctx, "select last_kafka_message_id()").Scan(&v); err != nil {
		return 0, false, fmt.Errorf("last_kafka_message_id: %w", err)
	}
	return v.Int64, v.Valid, nil
}

func expectStats(ctx context.Context, conn *sql.Conn, where string, cnt, minID, maxID int64) error {
	gc, gmin, gmax, err := readStats(ctx, conn, where)
	if err != nil {
		return err
	}
	if gc != cnt || gmin != minID || gmax != maxID {
		return fmt.Errorf("read [%s]: got (count=%d, min=%d, max=%d), want (%d, %d, %d)",
			where, gc, gmin, gmax, cnt, minID, maxID)
	}
	return nil
}

func expectLastID(ctx context.Context, conn *sql.Conn, want int64) error {
	v, ok, err := lastID(ctx, conn)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("last_kafka_message_id: got NULL, want %d", want)
	}
	if v != want {
		return fmt.Errorf("last_kafka_message_id: got %d, want %d", v, want)
	}
	return nil
}

// seedFunc produces n messages with ids first..first+n-1 into the topic; the
// unit test substitutes a simulator so run() is executable without a broker.
type seedFunc func(ctx context.Context, bootstrap string, first, n int) error

func run(ctx context.Context, db *sql.DB, bootstrap string, seed seedFunc, r *report) error {
	// ---- setup: schema + 10 seeded messages (offsets 0..9, ids 1..10) ----
	for _, stmt := range []string{
		"drop database if exists kafka_e2e",
		"create database kafka_e2e",
		"create external table kafka_e2e.korders (id bigint, name varchar(100)) engine = kafka with ('brokers' = '" + bootstrap + "', 'topic' = '" + topic + "')",
		"create external table kafka_e2e.korders_auto (id bigint, name varchar(100)) engine = kafka with ('brokers' = '" + bootstrap + "', 'topic' = '" + topic + "', 'autocommit' = 'true', 'group' = 'g_auto')",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("setup %q: %w", stmt, err)
		}
	}
	if err := seed(ctx, bootstrap, 1, 10); err != nil {
		return err
	}
	r.Cases = append(r.Cases, "setup-seed")

	// ---- A: read to the end of the stream; the read ENDS BY TIMEOUT and
	// that is not an error — all rows return and the last id is correct ----
	connA, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer connA.Close()
	if err := expectStats(ctx, connA, "__mo_read_start_id = -1 and __mo_read_timeout = 2", 10, 0, 9); err != nil {
		return fmt.Errorf("read-to-end: %w", err)
	}
	if err := expectLastID(ctx, connA, 9); err != nil {
		return fmt.Errorf("read-to-end: %w", err)
	}
	// data columns parsed, synthetic columns hidden from SELECT *
	var sum int64
	if err := connA.QueryRowContext(ctx,
		"select sum(id) from "+table+" where __mo_read_start_id = -1 and __mo_read_timeout = 2").Scan(&sum); err != nil {
		return err
	}
	if sum != 55 {
		return fmt.Errorf("sum(id) = %d, want 55", sum)
	}
	colNames, err := func() ([]string, error) {
		rows, err := connA.QueryContext(ctx,
			"select * from "+table+" where __mo_read_start_id = -1 and __mo_read_size = 1")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		for rows.Next() {
		}
		return cols, rows.Err()
	}()
	if err != nil {
		return err
	}
	if len(colNames) != 2 {
		return fmt.Errorf("select * returned columns %v, want only the 2 declared", colNames)
	}
	r.Cases = append(r.Cases, "read-to-end-timeout-clean")

	// ---- B: exactly-once chaining — LAST_KAFKA_MESSAGE_ID() feeds the next
	// __mo_read_start_id; the three reads tile the stream with no overlap
	// and no gap ----
	connB, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer connB.Close()
	if err := expectStats(ctx, connB, "__mo_read_start_id = -1 and __mo_read_size = 4", 4, 0, 3); err != nil {
		return fmt.Errorf("chain r1: %w", err)
	}
	last, ok, err := lastID(ctx, connB)
	if err != nil || !ok {
		return fmt.Errorf("chain r1 last id (ok=%v): %w", ok, err)
	}
	if last != 3 {
		return fmt.Errorf("chain r1 last id = %d, want 3", last)
	}
	if err := expectStats(ctx, connB,
		fmt.Sprintf("__mo_read_start_id = %d and __mo_read_size = 4", last), 4, 4, 7); err != nil {
		return fmt.Errorf("chain r2: %w", err)
	}
	if last, ok, err = lastID(ctx, connB); err != nil || !ok || last != 7 {
		return fmt.Errorf("chain r2 last id = %d (ok=%v): %w", last, ok, err)
	}
	if err := expectStats(ctx, connB,
		fmt.Sprintf("__mo_read_start_id = %d and __mo_read_timeout = 2", last), 2, 8, 9); err != nil {
		return fmt.Errorf("chain r3: %w", err)
	}
	if err := expectLastID(ctx, connB, 9); err != nil {
		return fmt.Errorf("chain r3: %w", err)
	}
	r.Cases = append(r.Cases, "exactly-once-chaining")

	// ---- C: repeating the same __mo_read_start_id returns the same data
	// (autocommit=false commits the same position, so a retry is a replay) ----
	connC, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer connC.Close()
	statsAndSum := func() (string, error) {
		where := "__mo_read_start_id = 3 and __mo_read_size = 4"
		c, mn, mx, err := readStats(ctx, connC, where)
		if err != nil {
			return "", err
		}
		var s sql.NullInt64
		if err := connC.QueryRowContext(ctx,
			"select sum(id) from "+table+" where "+where).Scan(&s); err != nil {
			return "", err
		}
		return fmt.Sprintf("count=%d min=%d max=%d sum=%d", c, mn, mx, s.Int64), nil
	}
	first, err := statsAndSum()
	if err != nil {
		return fmt.Errorf("repeat r1: %w", err)
	}
	second, err := statsAndSum()
	if err != nil {
		return fmt.Errorf("repeat r2: %w", err)
	}
	if first != second {
		return fmt.Errorf("repeated __mo_read_start_id diverged: %q vs %q", first, second)
	}
	if first != "count=4 min=4 max=7 sum=26" { // offsets 4..7 hold ids 5..8
		return fmt.Errorf("repeated read stats = %q, want count=4 min=4 max=7 sum=26", first)
	}
	r.Cases = append(r.Cases, "repeated-start-id-replays")

	// ---- D: new messages arrive; the chained session picks up at exactly
	// the right offset ----
	if err := seed(ctx, bootstrap, 11, 3); err != nil { // offsets 10..12, ids 11..13
		return err
	}
	if err := expectStats(ctx, connB, "__mo_read_start_id = 9 and __mo_read_timeout = 3", 3, 10, 12); err != nil {
		return fmt.Errorf("pickup: %w", err)
	}
	if err := expectLastID(ctx, connB, 12); err != nil {
		return fmt.Errorf("pickup: %w", err)
	}
	r.Cases = append(r.Cases, "pickup-after-produce")

	// ---- E: a read returning 0 messages. In a FRESH session the last id is
	// NULL (a scan with no messages records nothing); in a session that read
	// before, the previous id is preserved so chaining stays safe ----
	connD, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer connD.Close()
	if err := expectStats(ctx, connD, "__mo_read_start_id = 12 and __mo_read_timeout = 2", 0, -1, -1); err != nil {
		return fmt.Errorf("zero-read: %w", err)
	}
	if _, ok, err := lastID(ctx, connD); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("fresh session after a 0-message read: last_kafka_message_id must be NULL")
	}
	if err := expectStats(ctx, connB, "__mo_read_start_id = 12 and __mo_read_timeout = 2", 0, -1, -1); err != nil {
		return fmt.Errorf("zero-read (chained session): %w", err)
	}
	if err := expectLastID(ctx, connB, 12); err != nil {
		return fmt.Errorf("a 0-message read must not clobber the chained last id: %w", err)
	}
	r.Cases = append(r.Cases, "zero-messages-null-and-preserved")

	// ---- F: message metadata — no keys were produced, so the key is NULL ----
	var nullKeys int64
	if err := connA.QueryRowContext(ctx,
		"select count(*) from "+table+
			" where __mo_read_start_id = -1 and __mo_read_size = 13 and __mo_message_key is null").Scan(&nullKeys); err != nil {
		return err
	}
	if nullKeys != 13 {
		return fmt.Errorf("null message keys = %d, want 13", nullKeys)
	}
	r.Cases = append(r.Cases, "metadata-key-null")

	// ---- G: autocommit=true — default start reads from the earliest every
	// time (committed progress is NOT an implicit start position), and the
	// completed scan still records the last id ----
	connE, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer connE.Close()
	autoCount := func() (int64, error) {
		var c int64
		err := connE.QueryRowContext(ctx,
			"select count(*) from kafka_e2e.korders_auto where __mo_read_timeout = 2").Scan(&c)
		return c, err
	}
	c1, err := autoCount()
	if err != nil {
		return fmt.Errorf("autocommit r1: %w", err)
	}
	c2, err := autoCount()
	if err != nil {
		return fmt.Errorf("autocommit r2: %w", err)
	}
	if c1 != 13 || c2 != 13 {
		return fmt.Errorf("autocommit reads = %d, %d; want 13, 13 (default start is always earliest)", c1, c2)
	}
	if err := expectLastID(ctx, connE, 12); err != nil {
		return fmt.Errorf("autocommit: %w", err)
	}
	r.Cases = append(r.Cases, "autocommit-earliest-and-last-id")

	if _, err := db.ExecContext(ctx, "drop database kafka_e2e"); err != nil {
		return err
	}
	return nil
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
	summary := "## kafka external table E2E\n\nStatus: **" + value.Status + "**\n\nCases: " +
		strings.Join(value.Cases, ", ") + "\n"
	if value.Error != "" {
		summary += "\nError: " + value.Error + "\n"
	}
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(summary), 0o600)
}

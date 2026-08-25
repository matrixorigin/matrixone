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
	// errTopic carries deliberately malformed messages for the error-mode
	// scenario. It is separate from `topic` so the exactly-once scenarios,
	// which assert exact ids and sums, are unaffected.
	errTopic = "orders_errmix"
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
		err = runErrorMode(ctx, db, bootstrap, &r)
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
		"create external table kafka_e2e.korders_tx (id bigint, name varchar(100)) engine = kafka with ('brokers' = '" + bootstrap + "', 'topic' = '" + topic + "', 'autocommit' = 'true', 'group' = 'g_tx')",
		"create table kafka_e2e.dst_txn (id bigint, name varchar(100))",
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
	// r3 chains SERVER-SIDE: the builtin is used directly as the control
	// value, no client round-trip
	if err := expectStats(ctx, connB,
		"__mo_read_start_id = last_kafka_message_id() and __mo_read_timeout = 2", 2, 8, 9); err != nil {
		return fmt.Errorf("chain r3 (server-side): %w", err)
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

	// ---- H: explicit-transaction ownership — progress publishes only when
	// the ENCLOSING transaction commits. BEGIN; INSERT..SELECT FROM kafka;
	// ROLLBACK must leave the chain untouched (no last id, no committed
	// group offset); the same statement followed by COMMIT publishes both.
	connF, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer connF.Close()
	insertSQL := "insert into kafka_e2e.dst_txn select id, name from kafka_e2e.korders_tx where __mo_read_timeout = 2"
	if _, err := connF.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	if _, err := connF.ExecContext(ctx, insertSQL); err != nil {
		return fmt.Errorf("txn insert: %w", err)
	}
	// MID-transaction: nothing may be published yet
	if _, ok, err := lastID(ctx, connF); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("last_kafka_message_id must stay NULL inside an open transaction")
	}
	if _, err := connF.ExecContext(ctx, "ROLLBACK"); err != nil {
		return err
	}
	if _, ok, err := lastID(ctx, connF); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("ROLLBACK must not publish the kafka last id")
	}
	var dstCnt int64
	if err := connF.QueryRowContext(ctx, "select count(*) from kafka_e2e.dst_txn").Scan(&dstCnt); err != nil {
		return err
	}
	if dstCnt != 0 {
		return fmt.Errorf("rolled-back insert left %d rows", dstCnt)
	}
	// the rolled-back read must not have advanced the g_tx group offset
	if committedOffset(ctx, bootstrap, "g_tx") >= 0 {
		return fmt.Errorf("ROLLBACK must not commit the kafka group offset")
	}

	if _, err := connF.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	if _, err := connF.ExecContext(ctx, insertSQL); err != nil {
		return fmt.Errorf("txn insert (commit round): %w", err)
	}
	if _, err := connF.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	if err := expectLastID(ctx, connF, 12); err != nil {
		return fmt.Errorf("COMMIT must publish the kafka last id: %w", err)
	}
	if err := connF.QueryRowContext(ctx, "select count(*) from kafka_e2e.dst_txn").Scan(&dstCnt); err != nil {
		return err
	}
	if dstCnt != 13 {
		return fmt.Errorf("committed insert rows = %d, want 13", dstCnt)
	}
	if got := committedOffset(ctx, bootstrap, "g_tx"); got != 13 {
		return fmt.Errorf("committed g_tx offset = %d, want 13", got)
	}
	r.Cases = append(r.Cases, "txn-rollback-discards-commit-publishes")

	if _, err := db.ExecContext(ctx, "drop database kafka_e2e"); err != nil {
		return err
	}
	return nil
}

// committedOffsetFn is a test seam: the broker-free simulator substitutes
// its own committed-offset model (a real run uses the kadm lookup below).
var committedOffsetFn func(group string) (int64, bool)

// committedOffset returns the committed offset of partition 0 for group, or
// -1 when the group has committed nothing.
func committedOffset(ctx context.Context, bootstrap, group string) int64 {
	if committedOffsetFn != nil {
		if v, ok := committedOffsetFn(group); ok {
			return v
		}
		return -1
	}
	return committedOffsetReal(ctx, bootstrap, group)
}

func committedOffsetReal(ctx context.Context, bootstrap, group string) int64 {
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		return -1
	}
	defer cl.Close()
	fctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(cl).FetchOffsets(fctx, group)
	if err != nil {
		return -1
	}
	o, ok := resp.Lookup(topic, 0)
	if !ok || o.At < 0 {
		return -1
	}
	return o.At
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

// seedKafkaRaw produces the given values verbatim into a topic, creating it
// first. Unlike seedKafka it does not shape the payloads, so a caller can put
// malformed records on the stream.
func seedKafkaRaw(ctx context.Context, bootstrap, tp string, values []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, err = adm.CreateTopics(ctx, 1, 1, nil, tp)
		if err == nil || strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("create topic %s: %w", tp, err)
		}
		time.Sleep(2 * time.Second)
	}
	for i, v := range values {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: tp, Value: []byte(v)}).FirstErr(); err != nil {
			return fmt.Errorf("produce %d into %s: %w", i, tp, err)
		}
	}
	return nil
}

// runErrorMode proves the external error-mode columns (issue #27517) on a REAL
// Kafka stream, and that one multi-table INSERT can split a messy stream into a
// destination table and a rejects table without failing the statement.
//
// It is deliberately not part of run(): run() is also executed by the
// broker-free unit test against a SQL simulator, and teaching that simulator to
// model multi-table INSERT would make it the oracle for a feature it does not
// implement.
func runErrorMode(ctx context.Context, db *sql.DB, bootstrap string, r *report) error {
	// 8 messages: 2 parse cleanly, 6 fail in a different way each.
	msgs := []string{
		"1,alpha,10.50,2024-01-01 00:00:00",      // good
		"abc,beta,20.50,2024-01-02 00:00:00",     // id is not a number
		"3,gamma,notanumber,2024-01-03 00:00:00", // amount is not a number
		"4,delta,40.50,not-a-timestamp",          // ts is unparsable
		"5,epsilon,50.50",                        // too few fields
		"6,zeta,60.50,2024-01-06 00:00:00,extra", // too many fields
		"",                                       // empty value: not one record
		"8,theta,80.50,2024-01-08 00:00:00",      // good
	}
	if err := seedKafkaRaw(ctx, bootstrap, errTopic, msgs); err != nil {
		return err
	}

	const src = "kafka_e2e.kerr"
	for _, stmt := range []string{
		// run() drops its database when it finishes, so own the schema here
		// rather than depending on what another scenario left behind.
		"create database if not exists kafka_e2e",
		"create external table " + src + " (id int, name varchar(20), amount decimal(10,2), ts timestamp)" +
			" engine = kafka with ('brokers' = '" + bootstrap + "', 'topic' = '" + errTopic + "')",
		"create table kafka_e2e.kdest (id int, name varchar(20), amount decimal(10,2), ts timestamp)",
		"create table kafka_e2e.krejects (msg_id bigint, msg varchar(500), txt varchar(500))",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("error-mode setup %q: %w", stmt, err)
		}
	}

	// LAST_KAFKA_MESSAGE_ID() is session state, and the read positions itself
	// with __mo_read_start_id, so pin one connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// A message that fails to parse is a row, not a statement failure: one
	// statement sends the good records to kdest and the bad ones to krejects.
	// A Kafka record has no line in a file, so it is identified by
	// __mo_message_id, and __mo_file_line is NULL.
	if _, err := conn.ExecContext(ctx,
		"insert first"+
			" when errmsg is null then into kafka_e2e.kdest (id, name, amount, ts) values (id, name, amount, ts)"+
			" else into kafka_e2e.krejects (msg_id, msg, txt) values (mid, errmsg, errtxt)"+
			" select id, name, amount, ts, __mo_message_id as mid,"+
			" __mo_error_message as errmsg, __mo_error_text as errtxt"+
			" from "+src+" where __mo_read_start_id = -1 and __mo_read_timeout = 5"); err != nil {
		return fmt.Errorf("error-mode split: %w", err)
	}

	var good, bad int64
	if err := conn.QueryRowContext(ctx, "select count(*) from kafka_e2e.kdest").Scan(&good); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx, "select count(*) from kafka_e2e.krejects").Scan(&bad); err != nil {
		return err
	}
	if good != 2 || bad != 6 {
		return fmt.Errorf("error-mode split: kdest=%d krejects=%d, want 2 and 6", good, bad)
	}
	// every message is accounted for exactly once
	if good+bad != int64(len(msgs)) {
		return fmt.Errorf("error-mode split: %d rows for %d messages", good+bad, len(msgs))
	}
	r.Cases = append(r.Cases, "error-mode-split")

	// the rejects carry the offset of the message that failed, in order
	rows, err := conn.QueryContext(ctx, "select msg_id, txt from kafka_e2e.krejects order by msg_id")
	if err != nil {
		return err
	}
	defer rows.Close()
	wantOffsets := []int64{1, 2, 3, 4, 5, 6}
	var i int
	for rows.Next() {
		var id int64
		var txt string
		if err := rows.Scan(&id, &txt); err != nil {
			return err
		}
		if i >= len(wantOffsets) {
			return fmt.Errorf("error-mode: more rejects than expected")
		}
		if id != wantOffsets[i] {
			return fmt.Errorf("error-mode reject %d: __mo_message_id = %d, want %d", i, id, wantOffsets[i])
		}
		// __mo_error_text is the message value as published
		if txt != msgs[id] {
			return fmt.Errorf("error-mode reject %d: text %q, want %q", i, txt, msgs[id])
		}
		i++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if i != len(wantOffsets) {
		return fmt.Errorf("error-mode: %d rejects, want %d", i, len(wantOffsets))
	}
	r.Cases = append(r.Cases, "error-mode-message-id")

	// __mo_file_line is NULL on a Kafka scan: a message has no line in a file
	var lineNulls int64
	if err := conn.QueryRowContext(ctx,
		"select count(*) from "+src+
			" where __mo_read_start_id = -1 and __mo_read_timeout = 5"+
			" and __mo_file_line is null and __mo_error_message is not null").Scan(&lineNulls); err != nil {
		return err
	}
	if lineNulls != 6 {
		return fmt.Errorf("error-mode: %d failed rows with NULL __mo_file_line, want 6", lineNulls)
	}
	r.Cases = append(r.Cases, "error-mode-no-file-line")

	// without the error columns the same read still fails on the first bad
	// message, exactly as it did before error mode existed
	if _, err := conn.ExecContext(ctx,
		"insert into kafka_e2e.kdest (id, name, amount, ts) select id, name, amount, ts from "+src+
			" where __mo_read_start_id = -1 and __mo_read_timeout = 5"); err == nil {
		return fmt.Errorf("error-mode: a read without the error columns must still fail")
	}
	r.Cases = append(r.Cases, "error-mode-pruned-still-fails")
	return nil
}

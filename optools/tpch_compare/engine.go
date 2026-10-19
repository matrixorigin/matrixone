// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-sql-driver/mysql"
)

type Engine struct {
	Name        string `json:"name"`
	Database    string `json:"database"`
	DSNEnv      string `json:"dsn_env,omitempty"`
	Threads     int    `json:"threads"`
	MemoryBytes int64  `json:"memory_bytes"`
}

func quoteSQL(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func openEngine(ctx context.Context, e Engine) (*sql.DB, error) {
	var db *sql.DB
	var err error
	switch e.Name {
	case "mo":
		if !strings.HasPrefix(e.Database, "tpch_compare_") {
			return nil, errors.New("MO database must use the private tpch_compare_ prefix")
		}
		var cfg *mysql.Config
		cfg, err = mysql.ParseDSN(os.Getenv(e.DSNEnv))
		if err != nil {
			return nil, errors.New("invalid MO DSN")
		}
		if cfg.Net != "tcp" || (!strings.HasPrefix(cfg.Addr, "127.0.0.1:") && !strings.HasPrefix(cfg.Addr, "localhost:")) {
			return nil, errors.New("benchmark MO DSN must address an owned loopback deployment")
		}
		cfg.DBName = e.Database
		cfg.ParseTime = true
		cfg.MultiStatements = false
		cfg.InterpolateParams = false
		cfg.Collation = "utf8mb4_bin"
		cfg.Timeout = 10 * time.Second
		cfg.ReadTimeout = 0
		cfg.WriteTimeout = 0
		db, err = sql.Open("mysql", cfg.FormatDSN())
	case "duckdb":
		db, err = sql.Open("duckdb", e.Database)
	default:
		return nil, fmt.Errorf("unknown engine %q", e.Name)
	}
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(e.Threads + 1)
	db.SetMaxIdleConns(e.Threads + 1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%s readiness: %w", e.Name, err)
	}
	return db, nil
}

func configureConn(ctx context.Context, db *sql.DB, e Engine) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	settings := []string{"SET time_zone = '+00:00'", "SET sql_mode = 'ANSI,ONLY_FULL_GROUP_BY'", "SET div_precision_increment = 6", "SET save_query_result = off"}
	if e.Name == "duckdb" {
		settings = []string{"SET TimeZone = 'UTC'", fmt.Sprintf("SET threads = %d", e.Threads), fmt.Sprintf("SET memory_limit = '%dB'", e.MemoryBytes*4/5), "SET enable_progress_bar = false", "SET default_null_order = 'NULLS_FIRST_ON_ASC_LAST_ON_DESC'"}
	}
	for _, s := range settings {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("%s session setting %s: %w", e.Name, s, err)
		}
	}
	return conn, nil
}

func effectiveSettings(ctx context.Context, conn *sql.Conn, engine string) (map[string]string, error) {
	out := map[string]string{}
	if engine == "mo" {
		for _, name := range []string{"sql_mode", "time_zone", "div_precision_increment", "save_query_result"} {
			var v string
			if err := conn.QueryRowContext(ctx, "SELECT @@session."+name).Scan(&v); err != nil {
				return nil, err
			}
			out[name] = v
		}
		return out, nil
	}
	rows, err := conn.QueryContext(ctx, "SELECT name, value FROM duckdb_settings() WHERE name IN ('threads','memory_limit','TimeZone','enable_profiling','profiling_mode','default_collation','default_null_order')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var value sql.NullString
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		out[name] = value.String
		if !value.Valid {
			out[name] = "<SQL NULL>"
		}
	}
	return out, rows.Err()
}

type LoadReceipt struct {
	SchemaNS     int64            `json:"schema_ns"`
	LoadNS       int64            `json:"load_ns"`
	StatisticsNS int64            `json:"statistics_ns"`
	CheckpointNS int64            `json:"checkpoint_ns"`
	Tables       map[string]int64 `json:"table_rows"`
}

func bulkLoad(ctx context.Context, conn *sql.Conn, e Engine, root string, d Dataset) (receipt LoadReceipt, retErr error) {
	receipt.Tables = map[string]int64{}
	start := time.Now()
	for _, t := range d.Tables {
		if _, err := conn.ExecContext(ctx, t.ddl()); err != nil {
			return receipt, err
		}
	}
	receipt.SchemaNS = time.Since(start).Nanoseconds()
	start = time.Now()
	for _, t := range d.Tables {
		file := d.Inputs[t.Name]
		path := filepath.Join(root, file.Path)
		var s string
		if e.Name == "mo" {
			// The owned server reads the immutable local file, avoiding a second
			// client-side data generator or a row-at-a-time insert path.
			s = "LOAD DATA INFILE " + quoteSQL(path) + " INTO TABLE " + t.Name + " FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n'"
		} else {
			f, err := os.Open(path)
			if err != nil {
				return receipt, err
			}
			scan := bufio.NewScanner(f)
			ok := scan.Scan()
			line := scan.Text()
			scanErr := scan.Err()
			closeErr := f.Close()
			if !ok {
				return receipt, errors.Join(errors.New("empty input"), scanErr, closeErr)
			}
			if closeErr != nil {
				return receipt, closeErr
			}
			var specs, names []string
			for _, c := range t.Columns {
				specs = append(specs, quoteSQL(c.Name)+": "+quoteSQL(c.SQLType))
				names = append(names, c.Name)
			}
			if strings.HasSuffix(line, "|") {
				specs = append(specs, "'_trailing': 'VARCHAR'")
			}
			s = "INSERT INTO " + t.Name + " SELECT " + strings.Join(names, ",") + " FROM read_csv(" + quoteSQL(path) + ", delim='|', header=false, quote='', escape='', columns={" + strings.Join(specs, ",") + "})"
		}
		if _, err := conn.ExecContext(ctx, s); err != nil {
			return receipt, fmt.Errorf("load %s: %w", t.Name, err)
		}
	}
	receipt.LoadNS = time.Since(start).Nanoseconds()
	start = time.Now()
	for _, t := range d.Tables {
		s := "ANALYZE " + t.Name
		if e.Name == "mo" {
			s = "ANALYZE TABLE " + t.Name
		}
		if _, err := conn.ExecContext(ctx, s); err != nil {
			return receipt, fmt.Errorf("statistics %s: %w", t.Name, err)
		}
	}
	receipt.StatisticsNS = time.Since(start).Nanoseconds()
	start = time.Now()
	s := "CHECKPOINT"
	if e.Name == "mo" {
		s = "SELECT mo_ctl('DN', 'Checkpoint', '')"
	}
	if _, err := conn.ExecContext(ctx, s); err != nil {
		return receipt, fmt.Errorf("durable preparation: %w", err)
	}
	receipt.CheckpointNS = time.Since(start).Nanoseconds()
	for _, t := range d.Tables {
		if err := verifyTable(ctx, conn, root, t, d.Inputs[t.Name]); err != nil {
			return receipt, err
		}
		var count int64
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+t.Name).Scan(&count); err != nil {
			return receipt, err
		}
		receipt.Tables[t.Name] = count
	}
	return receipt, verifyRelationships(ctx, conn)
}

type inputRows struct {
	f        *os.File
	scanner  *bufio.Scanner
	table    Table
	pending  Row
	previous Row
	group    []Row
	at       int
}

func openInput(path string, t Table) (*inputRows, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 64<<10), 1<<20)
	return &inputRows{f: f, scanner: s, table: t}, nil
}
func (r *inputRows) line() (Row, error) {
	if r.pending != nil {
		x := r.pending
		r.pending = nil
		return x, nil
	}
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	parts := strings.Split(r.scanner.Text(), "|")
	if len(parts) == len(r.table.Columns)+1 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	if len(parts) != len(r.table.Columns) {
		return nil, fmt.Errorf("%s input width mismatch", r.table.Name)
	}
	row := make(Row, len(parts))
	for i, p := range parts {
		var err error
		row[i], err = normalize(p, r.table.Columns[i])
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}
func (r *inputRows) next() (Row, error) {
	if r.at >= len(r.group) {
		first, err := r.line()
		if err != nil {
			return nil, err
		}
		r.group = []Row{first}
		r.at = 0
		bytes := int64(0)
		for {
			row, err := r.line()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			v := cmpCell(first[0], row[0], r.table.Columns[0])
			if v > 0 {
				return nil, errors.New("DBGEN input is not ordered by first primary-key column")
			}
			if v < 0 {
				r.pending = row
				break
			}
			for _, c := range row {
				bytes += int64(len(c.Value)) + 32
			}
			if bytes > captureMemory {
				return nil, errors.New("DBGEN key group exceeds memory bound")
			}
			r.group = append(r.group, row)
		}
		slices.SortFunc(r.group, func(a, b Row) int {
			for _, k := range r.table.Key {
				if v := cmpCell(a[k], b[k], r.table.Columns[k]); v != 0 {
					return v
				}
			}
			return 0
		})
	}
	row := r.group[r.at]
	r.at++
	if r.previous != nil {
		v := 0
		for _, k := range r.table.Key {
			v = cmpCell(r.previous[k], row[k], r.table.Columns[k])
			if v != 0 {
				break
			}
		}
		if v >= 0 {
			return nil, errors.New("duplicate or unsorted DBGEN primary key")
		}
	}
	r.previous = row
	return row, nil
}

func verifyTable(ctx context.Context, conn *sql.Conn, root string, t Table, id FileID) (retErr error) {
	expected, err := openInput(filepath.Join(root, id.Path), t)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, expected.f.Close()) }()
	var names, keys []string
	for _, c := range t.Columns {
		names = append(names, c.Name)
	}
	for _, k := range t.Key {
		keys = append(keys, t.Columns[k].Name)
	}
	_, _, _, _, err = drain(ctx, conn, "SELECT "+strings.Join(names, ",")+" FROM "+t.Name+" ORDER BY "+strings.Join(keys, ","), t.Columns, func(got Row) error {
		want, err := expected.next()
		if err != nil {
			return fmt.Errorf("%s unexpected database row: %w", t.Name, err)
		}
		if !slices.Equal(want, got) {
			return fmt.Errorf("%s content differs at key %s", t.Name, want[0].Value)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if _, err := expected.next(); !errors.Is(err, io.EOF) {
		return errors.Join(errors.New("database is missing input rows"), err)
	}
	return nil
}

func verifyRelationships(ctx context.Context, conn *sql.Conn) error {
	checks := []string{
		"SELECT COUNT(*) FROM nation n LEFT JOIN region r ON n.n_regionkey=r.r_regionkey WHERE r.r_regionkey IS NULL",
		"SELECT COUNT(*) FROM supplier s LEFT JOIN nation n ON s.s_nationkey=n.n_nationkey WHERE n.n_nationkey IS NULL",
		"SELECT COUNT(*) FROM customer c LEFT JOIN nation n ON c.c_nationkey=n.n_nationkey WHERE n.n_nationkey IS NULL",
		"SELECT COUNT(*) FROM partsupp ps LEFT JOIN part p ON ps.ps_partkey=p.p_partkey WHERE p.p_partkey IS NULL",
		"SELECT COUNT(*) FROM partsupp ps LEFT JOIN supplier s ON ps.ps_suppkey=s.s_suppkey WHERE s.s_suppkey IS NULL",
		"SELECT COUNT(*) FROM orders o LEFT JOIN customer c ON o.o_custkey=c.c_custkey WHERE c.c_custkey IS NULL",
		"SELECT COUNT(*) FROM lineitem l LEFT JOIN orders o ON l.l_orderkey=o.o_orderkey WHERE o.o_orderkey IS NULL",
		"SELECT COUNT(*) FROM lineitem l LEFT JOIN partsupp ps ON l.l_partkey=ps.ps_partkey AND l.l_suppkey=ps.ps_suppkey WHERE ps.ps_partkey IS NULL",
	}
	for i, s := range checks {
		var n int64
		if err := conn.QueryRowContext(ctx, s).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("foreign-key input relationship %d has %d violations", i, n)
		}
	}
	return nil
}

func qualifyReference(root string, d Dataset, q Query, capRoot string, cap Capture) (retErr error) {
	id, ok := d.References[strconv.Itoa(q.Number)]
	if !ok {
		return errors.New("missing independent SF1 reference")
	}
	if err := id.verify(root); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(root, id.Path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64<<10), 1<<20)
	if !scan.Scan() {
		return errors.New("missing reference header")
	}
	actual, err := openRows(filepath.Join(capRoot, cap.File.Path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, actual.close()) }()
	count := int64(0)
	for scan.Scan() {
		line := scan.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) != len(q.Columns) {
			return errors.New("reference width differs from canonical projection")
		}
		row, err := actual.next()
		if err != nil {
			return err
		}
		if len(row) != len(parts) {
			return errors.New("actual reference row width differs")
		}
		for i, c := range q.Columns {
			if row[i].Null {
				return errors.New("NULL differs from qualification reference")
			}
			if c.Kind == "text" {
				ref := strings.TrimRight(parts[i], " ")
				got := row[i].Value
				if c.Char {
					got = strings.TrimRight(got, " ")
				}
				if strings.TrimRight(got, " ") != ref {
					return fmt.Errorf("query Q%d reference string differs at row %d column %d", q.Number, count, i+1)
				}
				continue
			}
			ref := strings.TrimSpace(parts[i])
			if c.Kind == "date" {
				if ref != row[i].Value {
					return errors.New("reference date differs")
				}
				continue
			}
			x, err := number(row[i].Value)
			if err != nil {
				return err
			}
			y, err := number(ref)
			if err != nil {
				return err
			}
			if !referenceNumberEqual(x, y, c.Reference) {
				return fmt.Errorf("query Q%d reference numeric differs row %d column %d: %s / %s", q.Number, count, i+1, row[i].Value, ref)
			}
		}
		count++
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if count != cap.Rows {
		return errors.New("qualification reference row count differs")
	}
	if _, err := actual.next(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing reference result")
	}
	return nil
}

func referenceNumberEqual(actual, reference *big.Rat, rule string) bool {
	bound := new(big.Rat)
	switch rule {
	case "exact":
		return actual.Cmp(reference) == 0
	case "sum_money":
		bound.SetInt64(100)
	case "ratio":
		n := new(big.Int).Mul(actual.Num(), big.NewInt(100))
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(n, actual.Denom(), r)
		if new(big.Int).Mul(new(big.Int).Abs(r), big.NewInt(2)).Cmp(actual.Denom()) >= 0 {
			if actual.Sign() < 0 {
				q.Sub(q, big.NewInt(1))
			} else {
				q.Add(q, big.NewInt(1))
			}
		}
		actual = new(big.Rat).SetFrac(q, big.NewInt(100))
		bound.Quo(new(big.Rat).Abs(reference), big.NewRat(100, 1))
	default:
		return false
	}
	return new(big.Rat).Abs(new(big.Rat).Sub(actual, reference)).Cmp(bound) <= 0
}

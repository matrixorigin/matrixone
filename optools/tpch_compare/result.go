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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Cell struct {
	Null  bool   `json:"null,omitempty"`
	Value string `json:"value,omitempty"`
}
type Row []Cell
type PhysicalColumn struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Precision int64  `json:"precision,omitempty"`
	Scale     int64  `json:"scale,omitempty"`
	HasScale  bool   `json:"has_scale,omitempty"`
}
type Capture struct {
	Version    int              `json:"version"`
	Columns    []PhysicalColumn `json:"columns"`
	File       FileID           `json:"file"`
	Rows       int64            `json:"rows"`
	ElapsedNS  int64            `json:"elapsed_ns"`
	FirstRowNS *int64           `json:"first_row_ns"`
	LastRowNS  *int64           `json:"last_row_ns"`
	CleanupNS  int64            `json:"cleanup_ns"`
}

func number(s string) (*big.Rat, error) {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("invalid finite number %q", s)
	}
	return r, nil
}

func decimal(s string) (string, error) {
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "+"), "-")
	parts := strings.Split(s, ".")
	if len(parts) > 2 || len(parts) == 0 || parts[0] == "" {
		return "", errors.New("invalid exact decimal")
	}
	for _, p := range parts {
		for _, c := range p {
			if c < '0' || c > '9' {
				return "", errors.New("invalid exact decimal digit")
			}
		}
	}
	whole := strings.TrimLeft(parts[0], "0")
	if whole == "" {
		whole = "0"
	}
	frac := ""
	if len(parts) == 2 {
		frac = strings.TrimRight(parts[1], "0")
	}
	out := whole
	if frac != "" {
		out += "." + frac
	}
	if negative && out != "0" {
		out = "-" + out
	}
	return out, nil
}

func normalize(v any, c Column) (Cell, error) {
	if v == nil {
		return Cell{Null: true}, nil
	}
	var s string
	switch x := v.(type) {
	case []byte:
		s = string(x)
	case string:
		s = x
	case int64:
		s = strconv.FormatInt(x, 10)
	case int32:
		s = strconv.FormatInt(int64(x), 10)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return Cell{}, errors.New("nonfinite result")
		}
		s = strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		s = x.Format("2006-01-02")
	case fmt.Stringer:
		s = x.String()
	default:
		return Cell{}, fmt.Errorf("unsupported result value %T", v)
	}
	switch c.Kind {
	case "int":
		i, ok := new(big.Int).SetString(s, 10)
		if !ok {
			return Cell{}, fmt.Errorf("invalid integer %q", s)
		}
		s = i.String()
	case "decimal":
		var err error
		s, err = decimal(s)
		if err != nil {
			return Cell{}, err
		}
	case "approximate":
		if _, err := number(s); err != nil {
			return Cell{}, err
		}
	case "date":
		if _, err := time.Parse("2006-01-02", s); err != nil {
			return Cell{}, err
		}
	case "text":
		if c.Char {
			s = strings.TrimRight(s, " ")
		}
	default:
		return Cell{}, fmt.Errorf("unknown logical kind %s", c.Kind)
	}
	return Cell{Value: s}, nil
}

func physicalColumns(rows *sql.Rows, logical []Column) ([]PhysicalColumn, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	if len(types) != len(logical) {
		return nil, fmt.Errorf("projection has %d columns; want %d", len(types), len(logical))
	}
	out := make([]PhysicalColumn, len(types))
	for i, t := range types {
		p, s, ok := t.DecimalSize()
		out[i] = PhysicalColumn{t.Name(), t.DatabaseTypeName(), p, s, ok}
		if logical[i].Kind == "approximate" {
			if _, err := fixedError(out[i]); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func fixedError(c PhysicalColumn) (*big.Rat, error) {
	t := strings.ToUpper(c.Type)
	if strings.HasPrefix(t, "DECIMAL") || strings.HasPrefix(t, "NUMERIC") {
		if !c.HasScale || c.Scale < 2 || c.Scale > 38 {
			return nil, fmt.Errorf("unqualified decimal division scale: %+v", c)
		}
		return new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(c.Scale), nil)), nil
	}
	if t == "DOUBLE" || t == "DOUBLE PRECISION" || t == "FLOAT8" {
		return new(big.Rat), nil
	}
	return nil, fmt.Errorf("unqualified approximate result type %q", c.Type)
}

func equalCell(a, b Cell, c Column, pa, pb PhysicalColumn) (bool, error) {
	if a.Null || b.Null {
		return a.Null == b.Null, nil
	}
	if c.Kind != "approximate" {
		return a.Value == b.Value, nil
	}
	x, err := number(a.Value)
	if err != nil {
		return false, err
	}
	y, err := number(b.Value)
	if err != nil {
		return false, err
	}
	ea, err := fixedError(pa)
	if err != nil {
		return false, err
	}
	eb, err := fixedError(pb)
	if err != nil {
		return false, err
	}
	bound := new(big.Rat).Add(ea, eb)
	magnitude := new(big.Rat).Abs(x)
	if ay := new(big.Rat).Abs(y); ay.Cmp(magnitude) > 0 {
		magnitude = ay
	}
	if magnitude.Cmp(big.NewRat(1, 1)) < 0 {
		magnitude.SetInt64(1)
	}
	bound.Add(bound, new(big.Rat).Mul(magnitude, big.NewRat(1, 1_000_000_000_000)))
	diff := new(big.Rat).Abs(new(big.Rat).Sub(x, y))
	return diff.Cmp(bound) <= 0, nil
}

// drain closes the cursor on every path and optionally streams rows directly to
// a verifier. Result capture never buffers an entire SQL result in Go memory.
func drain(ctx context.Context, conn interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, s string, columns []Column, consume func(Row) error) (physical []PhysicalColumn, count int64, first, last *time.Time, retErr error) {
	rows, err := conn.QueryContext(ctx, s)
	if err != nil {
		return nil, 0, nil, nil, err
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	physical, err = physicalColumns(rows, columns)
	if err != nil {
		return nil, 0, nil, nil, err
	}
	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	for rows.Next() {
		now := time.Now()
		if first == nil {
			v := now
			first = &v
		}
		if err := rows.Scan(dest...); err != nil {
			return physical, count, first, last, err
		}
		row := make(Row, len(columns))
		for i, v := range values {
			row[i], err = normalize(v, columns[i])
			if err != nil {
				return physical, count, first, last, err
			}
		}
		if err := consume(row); err != nil {
			return physical, count, first, last, err
		}
		count++
		now = time.Now()
		last = &now
	}
	return physical, count, first, last, rows.Err()
}

func executeUnit(ctx context.Context, conn *sql.Conn, q Query, path string, limit int64) (cap Capture, retErr error) {
	cap.Version = formatVersion
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return cap, err
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, f.Close())
		}
	}()
	h := sha256.New()
	lw := &limitedWriter{io.MultiWriter(f, h), limit}
	buffer := bufio.NewWriterSize(lw, 64<<10)
	encoder := json.NewEncoder(buffer)
	start := time.Now()
	var first, last *time.Time
	for i, s := range q.Statements {
		if i == q.ResultStatement {
			cap.Columns, cap.Rows, first, last, err = drain(ctx, conn, s, q.Columns, func(row Row) error { return encoder.Encode(row) })
		} else {
			_, err = conn.ExecContext(ctx, s)
		}
		if err != nil {
			break
		}
	}
	cleanupStart := time.Now()
	if err != nil && q.Cleanup != "" {
		cleanup, cancel := context.WithTimeoutCause(context.Background(), 5*time.Second, errors.New("Q15 view cleanup deadline"))
		_, cleanErr := conn.ExecContext(cleanup, q.Cleanup)
		cancel()
		err = errors.Join(err, cleanErr)
	}
	err = errors.Join(err, buffer.Flush())
	err = errors.Join(err, f.Close())
	closed = true
	end := time.Now()
	cap.ElapsedNS = end.Sub(start).Nanoseconds()
	cap.CleanupNS = end.Sub(cleanupStart).Nanoseconds()
	if first != nil {
		v := first.Sub(start).Nanoseconds()
		cap.FirstRowNS = &v
	}
	if last != nil {
		v := last.Sub(start).Nanoseconds()
		cap.LastRowNS = &v
	}
	cap.File = FileID{Path: filepath.Base(path), SHA256: hex.EncodeToString(h.Sum(nil)), Bytes: limit - lw.Remaining}
	return cap, err
}

type rowReader struct {
	f       *os.File
	scanner *bufio.Scanner
}

func openRows(path string) (*rowReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 64<<10), 1<<20)
	return &rowReader{f, s}, nil
}
func (r *rowReader) next() (Row, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	var row Row
	if err := json.Unmarshal(r.scanner.Bytes(), &row); err != nil {
		return nil, err
	}
	return row, nil
}
func (r *rowReader) close() error { return r.f.Close() }

func cmpCell(a, b Cell, c Column) int {
	if a.Null {
		if b.Null {
			return 0
		}
		return -1
	}
	if b.Null {
		return 1
	}
	if c.Kind == "int" || c.Kind == "decimal" || c.Kind == "approximate" {
		x, _ := number(a.Value)
		y, _ := number(b.Value)
		return x.Cmp(y)
	}
	return strings.Compare(a.Value, b.Value)
}
func compareKey(a, b Row, q Query) int {
	for _, k := range q.Order {
		v := cmpCell(a[k.Column], b[k.Column], q.Columns[k.Column])
		if k.Desc {
			v = -v
		}
		if v != 0 {
			return v
		}
	}
	return 0
}

// A pair of monotonically ordered outputs can differ only within peer groups.
// Each group is bounded; oversized groups fail explicitly instead of growing.
func compareCaptures(rootA, rootB string, a, b Capture, q Query) (retErr error) {
	if a.Version != formatVersion || b.Version != formatVersion || a.Rows < 0 || a.Rows != b.Rows || len(a.Columns) != len(q.Columns) || len(b.Columns) != len(q.Columns) {
		return errors.New("capture count/schema mismatch")
	}
	if q.Limit > 0 && a.Rows > int64(q.Limit) {
		return fmt.Errorf("query Q%d result exceeds qualification LIMIT %d", q.Number, q.Limit)
	}
	if err := a.File.verify(rootA); err != nil {
		return err
	}
	if err := b.File.verify(rootB); err != nil {
		return err
	}
	pa, err := openRows(filepath.Join(rootA, a.File.Path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, pa.close()) }()
	pb, err := openRows(filepath.Join(rootB, b.File.Path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, pb.close()) }()
	ga, gb := &peerReader{reader: pa, q: q}, &peerReader{reader: pb, q: q}
	var count int64
	for {
		x, ea := ga.group()
		y, eb := gb.group()
		if errors.Is(ea, io.EOF) && errors.Is(eb, io.EOF) {
			break
		}
		if ea != nil || eb != nil {
			return errors.Join(ea, eb)
		}
		if len(x) != len(y) {
			return fmt.Errorf("query Q%d peer-group row count differs at row %d", q.Number, count)
		}
		less := func(a, b Row) int {
			for _, approx := range []bool{false, true} {
				for i, c := range q.Columns {
					if (c.Kind == "approximate") == approx {
						if v := cmpCell(a[i], b[i], c); v != 0 {
							return v
						}
					}
				}
			}
			return 0
		}
		slices.SortFunc(x, less)
		slices.SortFunc(y, less)
		for i := range x {
			for j, c := range q.Columns {
				ok, err := equalCell(x[i][j], y[i][j], c, a.Columns[j], b.Columns[j])
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("query Q%d mismatch at row %d column %d: %q / %q", q.Number, count+int64(i), j+1, x[i][j].Value, y[i][j].Value)
				}
			}
		}
		count += int64(len(x))
	}
	if count != a.Rows {
		return errors.New("capture receipt row count differs from complete contents")
	}
	return nil
}

type peerReader struct {
	reader   *rowReader
	q        Query
	pending  Row
	previous Row
}

func (p *peerReader) next() (Row, error) {
	row := p.pending
	p.pending = nil
	if row == nil {
		var err error
		row, err = p.reader.next()
		if err != nil {
			return nil, err
		}
	}
	if len(row) != len(p.q.Columns) {
		return nil, errors.New("invalid captured row width")
	}
	for i, c := range row {
		if !c.Null {
			if _, err := normalize(c.Value, p.q.Columns[i]); err != nil {
				return nil, err
			}
		}
	}
	if p.previous != nil && compareKey(p.previous, row, p.q) > 0 {
		return nil, errors.New("query output violates required ordering")
	}
	p.previous = row
	return row, nil
}
func (p *peerReader) group() ([]Row, error) {
	first, err := p.next()
	if err != nil {
		return nil, err
	}
	out := []Row{first}
	size := int64(0)
	for {
		row, err := p.next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if compareKey(first, row, p.q) != 0 {
			p.pending = row
			return out, nil
		}
		for _, c := range row {
			size += int64(len(c.Value)) + 32
		}
		if size > captureMemory {
			return nil, errors.New("peer group exceeds bounded comparison memory")
		}
		out = append(out, row)
	}
}

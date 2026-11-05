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

package vectorindex

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const ignoredRowsPerSql = 1024

// Float16Overflow is the smallest float32 magnitude that rounds to infinity as float16: the
// midpoint above the largest finite value, which rounds to even, away from it.
const Float16Overflow = float32(65520)

// UnstorableCodes are the error codes of the reasons an index cannot store a row.
var UnstorableCodes = []uint16{moerr.ErrIndexVectorDimension, moerr.ErrIndexVectorNonFinite, moerr.ErrIndexVectorOutOfRange,
	moerr.ErrIndexInvalidJSON, moerr.ErrIndexDatalink}

// MaxIgnoredReason is the number of bytes of a message an ignored row records.
const MaxIgnoredReason = 1024

// IgnoredReason returns the message an ignored row of err records: the message of err when its
// code needs the row's own detail, "" when the message of the code is enough. A message longer
// than MaxIgnoredReason bytes is cut at a character boundary and ends with "...".
func IgnoredReason(err error) string {
	code, _ := moerr.GetMoErrCode(err)
	switch code {
	case moerr.ErrIndexInvalidJSON, moerr.ErrIndexDatalink:
		msg := strings.ToValidUTF8(err.Error(), "�")
		if len(msg) <= MaxIgnoredReason {
			return msg
		}
		cut := MaxIgnoredReason - len("...")
		for cut > 0 && !utf8.RuneStart(msg[cut]) {
			cut--
		}
		return msg[:cut] + "..."
	}
	return ""
}

// EnumPkLiteral returns the SQL literal of the ENUM key e whose members are members. It holds the
// stored number, so it writes a number that names no member, as INSERT IGNORE stores, in strict mode.
func EnumPkLiteral(e types.Enum, members string) string {
	p := types.NewPacker()
	defer p.Close()
	p.EncodeEnum(e)
	quoted := strings.Split(members, ",")
	for i, m := range quoted {
		quoted[i] = sqlquote.String(m)
	}
	return fmt.Sprintf("SERIAL_EXTRACT(X'%s', 0 AS ENUM(%s))", hex.EncodeToString(p.GetBuf()), strings.Join(quoted, ","))
}

// SerialPkString returns the composite primary key whose serial is b as the hex literal X'...', in
// the digits HEX writes.
func SerialPkString(b []byte) string {
	return "X'" + strings.ToUpper(hex.EncodeToString(b)) + "'"
}

// UnstorableCode returns the code of err when it is a reason an index cannot store a row.
func UnstorableCode(err error) (uint16, bool) {
	code, ok := moerr.GetMoErrCode(err)
	if !ok || !slices.Contains(UnstorableCodes, code) {
		return 0, false
	}
	return code, true
}

// IgnoredRow is a source row an index cannot store: its primary key as a SQL literal, the
// error code of the reason, its message ("" for the message of the code), and the commit time
// (physical) of the change.
type IgnoredRow struct {
	Pk     string
	Code   uint16
	Reason string
	Ts     int64
}

// IgnoredColumns names the ignored table's columns. The caller passes the catalog's names:
// this package cannot import catalog, which reaches it again through cuvs in a gpu build.
type IgnoredColumns struct {
	Pk, Code, Reason, Ts string
}

// IgnoredRowsSql returns the statements that record rows in the ignored table db.table.
func IgnoredRowsSql(db, table string, cols IgnoredColumns, rows []IgnoredRow) []string {
	var sqls []string
	for start := 0; start < len(rows); start += ignoredRowsPerSql {
		end := min(start+ignoredRowsPerSql, len(rows))
		var b strings.Builder
		fmt.Fprintf(&b, "REPLACE INTO %s (%s, %s, %s, %s) VALUES ", sqlquote.QualifiedIdent(db, table),
			sqlquote.Ident(cols.Pk), sqlquote.Ident(cols.Code), sqlquote.Ident(cols.Reason), sqlquote.Ident(cols.Ts))
		for i, r := range rows[start:end] {
			if i > 0 {
				b.WriteString(", ")
			}
			reason := "NULL"
			if r.Reason != "" {
				reason = sqlquote.String(r.Reason)
			}
			fmt.Fprintf(&b, "(%s, %d, %s, %d)", r.Pk, r.Code, reason, r.Ts)
		}
		sqls = append(sqls, b.String())
	}
	return sqls
}

// IgnoredSpill holds the rows a build leaves out in a local file until it writes them, so the
// build's memory does not grow with their number.
type IgnoredSpill struct {
	dir string
	f   *os.File
	w   *bufio.Writer
	n   int64
}

// NewIgnoredSpill returns a spill whose file, created by the first row, is in dir ("" is $TMPDIR).
func NewIgnoredSpill(dir string) *IgnoredSpill {
	return &IgnoredSpill{dir: dir}
}

// Len returns the number of rows added.
func (s *IgnoredSpill) Len() int64 {
	return s.n
}

// Add appends a row: its primary key as a SQL literal, its error code and its message.
func (s *IgnoredSpill) Add(pk string, code uint16, reason string) error {
	if s.f == nil {
		f, err := os.CreateTemp(s.dir, "ignored")
		if err != nil {
			return err
		}
		s.f, s.w = f, bufio.NewWriter(f)
	}
	var hdr [10]byte
	binary.LittleEndian.PutUint16(hdr[:2], code)
	binary.LittleEndian.PutUint32(hdr[2:6], uint32(len(pk)))
	binary.LittleEndian.PutUint32(hdr[6:], uint32(len(reason)))
	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.w.WriteString(pk); err != nil {
		return err
	}
	if _, err := s.w.WriteString(reason); err != nil {
		return err
	}
	s.n++
	return nil
}

// Sqls passes to exec, in order, the statements that record the rows in the ignored table
// db.table with commit time ts, at most ignoredRowsPerSql rows each.
func (s *IgnoredSpill) Sqls(db, table string, cols IgnoredColumns, ts int64, exec func(string) error) error {
	if s.f == nil {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(s.f)
	rows := make([]IgnoredRow, 0, min(s.n, ignoredRowsPerSql))
	var hdr [10]byte
	for read := int64(0); read < s.n; read++ {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return err
		}
		pk := make([]byte, binary.LittleEndian.Uint32(hdr[2:6]))
		if _, err := io.ReadFull(r, pk); err != nil {
			return err
		}
		reason := make([]byte, binary.LittleEndian.Uint32(hdr[6:]))
		if _, err := io.ReadFull(r, reason); err != nil {
			return err
		}
		rows = append(rows, IgnoredRow{Pk: string(pk), Code: binary.LittleEndian.Uint16(hdr[:2]), Reason: string(reason), Ts: ts})
		if len(rows) == ignoredRowsPerSql || read == s.n-1 {
			for _, sql := range IgnoredRowsSql(db, table, cols, rows) {
				if err := exec(sql); err != nil {
					return err
				}
			}
			rows = rows[:0]
		}
	}
	return nil
}

// Close removes the file.
func (s *IgnoredSpill) Close() error {
	if s.f == nil {
		return nil
	}
	name := s.f.Name()
	err := s.f.Close()
	s.f, s.w = nil, nil
	if rerr := os.Remove(name); err == nil {
		err = rerr
	}
	return err
}

// CentroidxArgs returns the arguments of the CENTROIDX join that assigns the rows of an IVF-FLAT
// index build or sync: the metric and, for an index with an ignored table, the index's algo
// params and its table config cfg, by which the join leaves out and records the rows the index
// cannot store.
func CentroidxArgs(opType, params string, cfg IndexTableConfig) (string, error) {
	if cfg.IgnoredTable == "" {
		return sqlquote.String(opType), nil
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	return sqlquote.String(opType) + ", " + sqlquote.String(params) + ", " + sqlquote.String(string(b)), nil
}

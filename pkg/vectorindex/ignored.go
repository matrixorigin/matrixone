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
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
)

const ignoredRowsPerSql = 1024

// UnstorableCodes are the error codes of the reasons an index cannot store a row.
var UnstorableCodes = []uint16{moerr.ErrIndexVectorDimension, moerr.ErrIndexVectorNonFinite, moerr.ErrIndexVectorOutOfRange}

// UnstorableCode returns the code of err when it is a reason an index cannot store a row.
func UnstorableCode(err error) (uint16, bool) {
	code, ok := moerr.GetMoErrCode(err)
	if !ok || !slices.Contains(UnstorableCodes, code) {
		return 0, false
	}
	return code, true
}

// IgnoredRow is a source row an index cannot store: its primary key as a SQL literal, the
// error code of the reason, and the commit time (physical) of the change.
type IgnoredRow struct {
	Pk   string
	Code uint16
	Ts   int64
}

// IgnoredColumns names the ignored table's columns. The caller passes the catalog's names:
// this package cannot import catalog, which reaches it again through cuvs in a gpu build.
type IgnoredColumns struct {
	Pk, Code, Ts string
}

// IgnoredRowsSql returns the statements that record rows in the ignored table db.table.
func IgnoredRowsSql(db, table string, cols IgnoredColumns, rows []IgnoredRow) []string {
	var sqls []string
	for start := 0; start < len(rows); start += ignoredRowsPerSql {
		end := min(start+ignoredRowsPerSql, len(rows))
		var b strings.Builder
		fmt.Fprintf(&b, "REPLACE INTO %s (%s, %s, %s) VALUES ", sqlquote.QualifiedIdent(db, table),
			sqlquote.Ident(cols.Pk), sqlquote.Ident(cols.Code), sqlquote.Ident(cols.Ts))
		for i, r := range rows[start:end] {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "(%s, %d, %d)", r.Pk, r.Code, r.Ts)
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

// Add appends a row: its primary key as a SQL literal and its error code.
func (s *IgnoredSpill) Add(pk string, code uint16) error {
	if s.f == nil {
		f, err := os.CreateTemp(s.dir, "ignored")
		if err != nil {
			return err
		}
		s.f, s.w = f, bufio.NewWriter(f)
	}
	var hdr [6]byte
	binary.LittleEndian.PutUint16(hdr[:2], code)
	binary.LittleEndian.PutUint32(hdr[2:], uint32(len(pk)))
	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.w.WriteString(pk); err != nil {
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
	var hdr [6]byte
	for read := int64(0); read < s.n; read++ {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return err
		}
		pk := make([]byte, binary.LittleEndian.Uint32(hdr[2:]))
		if _, err := io.ReadFull(r, pk); err != nil {
			return err
		}
		rows = append(rows, IgnoredRow{Pk: string(pk), Code: binary.LittleEndian.Uint16(hdr[:2]), Ts: ts})
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

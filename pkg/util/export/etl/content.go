// Copyright 2024 Matrix Origin
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

package etl

import (
	"bytes"
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var _ table.RowWriter = (*ContentWriter)(nil)
var _ table.ContentSettable = (*ContentWriter)(nil)

// ContentWriter NO do gen op, just do the flush op.
type ContentWriter struct {
	ctx        context.Context
	tbl        *table.Table
	buf        *bytes.Buffer
	sqlFlusher table.Flusher
	csvFlusher table.Flusher
}

func NewContentWriter(ctx context.Context, tbl *table.Table, writer table.RowWriter) *ContentWriter {
	flusher, ok := writer.(table.Flusher)
	if !ok {
		panic("target writer NEED implements table.Flusher")
	}

	return &ContentWriter{
		ctx: ctx,
		tbl: tbl,
		// buf dependent on SetContent to set.

		sqlFlusher: NewSQLFlusher(tbl),
		csvFlusher: NewFileContentFlusher(flusher),
	}
}

func (c *ContentWriter) SetContent(buf *bytes.Buffer) {
	c.buf = buf
}

func (c *ContentWriter) WriteRow(_ *table.Row) error { return nil }

func (c *ContentWriter) GetContent() string {
	if c.buf == nil {
		return ""
	}
	return util.UnsafeBytesToString(c.buf.Bytes())
}

func (c *ContentWriter) GetContentLength() int {
	if c.buf == nil {
		return 0
	}
	return c.buf.Len()
}

func (c *ContentWriter) FlushAndClose() (int, error) {
	if c.buf == nil {
		return 0, nil
	}
	n, err := c.sqlFlusher.FlushBuffer(c.buf)
	if err != nil {
		n, err = c.csvFlusher.FlushBuffer(c.buf)
		if err != nil {
			v2.TraceMOLoggerErrorFlushCounter.Inc()
			return 0, err
		}
	}
	c.sqlFlusher = nil
	c.csvFlusher = nil
	return n, nil
}

var _ table.Flusher = (*SQLFlusher)(nil)

type SQLFlusher struct {
	database string
	table    string
}

func NewSQLFlusher(tbl *table.Table) *SQLFlusher {
	return &SQLFlusher{
		database: tbl.GetDatabase(),
		table:    tbl.GetName(),
	}
}

func (f *SQLFlusher) FlushBuffer(buf *bytes.Buffer) (int, error) {
	// FIXME: if error sometime, pls back-off
	conn, err := db_holder.GetOrInitDBConn(false, true)
	if err != nil {
		v2.TraceMOLoggerErrorConnDBCounter.Inc()
		return 0, err
	}

	sqlCsv := bytes.NewBuffer(make([]byte, 0, buf.Len()+mpool.KB))
	for _, c := range buf.Bytes() {
		// case \: mo sql NEED quote '\'
		// case ': sql (load data inline ... DATA='' ...) will quote {sqlCsv} with "'"
		if c == '\\' || c == '\'' {
			sqlCsv.WriteByte(c)
			sqlCsv.WriteByte(c)
		}
	}

	loadSQL := fmt.Sprintf("LOAD DATA INLINE FORMAT='csv', DATA='%s' INTO TABLE %s.%s FIELDS TERMINATED BY ','",
		sqlCsv.Bytes(), f.database, f.table)
	v2.TraceMOLoggerExportSqlHistogram.Observe(float64(len(loadSQL)))

	_, err = conn.Exec(loadSQL)
	return 0, err
}

var _ table.Flusher = (*FileFlusher)(nil)

type FileFlusher struct {
	writer table.Flusher
}

func NewFileContentFlusher(writer table.Flusher) *FileFlusher {
	return &FileFlusher{
		writer: writer,
	}
}

func (f *FileFlusher) FlushBuffer(buf *bytes.Buffer) (int, error) {
	return f.writer.FlushBuffer(buf)
}

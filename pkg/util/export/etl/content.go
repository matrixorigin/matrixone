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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var errBackOff = moerr.NewInternalErrorNoCtx("BackOff trigger")

var _ table.RowWriter = (*ContentWriter)(nil)
var _ table.BufferSettable = (*ContentWriter)(nil)
var _ table.BackOffSettable = (*ContentWriter)(nil)

// ContentWriter NO do gen op, just do the flush op.
type ContentWriter struct {
	ctx context.Context
	tbl *table.Table

	// formatter used in 'mode 1', more can see table.BufferSettable
	formatter Formatter

	// mode 1 & 2
	buf         *bytes.Buffer
	bufCallback func(*bytes.Buffer)

	// main flow
	sqlFlusher table.Flusher
	csvFlusher table.Flusher

	backoff table.BackOff
}

func NewContentWriter(ctx context.Context, tbl *table.Table, fileFlusher table.Flusher) *ContentWriter {
	return &ContentWriter{
		ctx: ctx,
		tbl: tbl,
		// buf dependent on SetContent to set.

		sqlFlusher: NewSQLFlusher(tbl),
		csvFlusher: NewFileContentFlusher(fileFlusher),
	}
}

// SetBuffer implements table.BufferSettable
func (c *ContentWriter) SetBuffer(buf *bytes.Buffer, callback func(buffer *bytes.Buffer)) {
	c.buf = buf
	c.bufCallback = callback
}

// NeedBuffer implements table.BufferSettable
func (c *ContentWriter) NeedBuffer() bool { return true }

func (c *ContentWriter) SetupBackOff(backoff table.BackOff) {
	c.backoff = backoff
}

// WriteRow serialize the row into buffer
// It new a formatter to serialize the row.
func (c *ContentWriter) WriteRow(row *table.Row) error {
	if c.formatter == nil {
		c.formatter = NewContentFormatter(c.ctx, c.buf)
	}
	return c.formatter.WriteRow(row)
}

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

func (c *ContentWriter) FlushAndClose() (n int, err error) {
	if c.buf == nil {
		return 0, nil
	}
	// mode 1 of table.BufferSettable
	if c.formatter != nil {
		c.formatter.Flush()
	}
	if c.bufCallback != nil {
		// release the buf.
		defer c.bufCallback(c.buf)
	}

	// main flow
	// Step 1/2: do sql flush.
	if c.backoff == nil || c.backoff.Count() {
		v2.TraceMOLoggerBufferLoopWriteSQL.Inc()
		n, err = c.sqlFlusher.FlushBuffer(c.buf)
	} else {
		// what situation wil run this loop
		// 1. metric collector has too much req in queue, ref metric_collector.go/mfsetETL.Count
		v2.TraceMOLoggerBufferLoopBackOff.Inc()
		// trigger csv flusher
		err = errBackOff
	}
	// Step 2/2: do csv flush, if sql failed.
	if err != nil {
		n, err = c.csvFlusher.FlushBuffer(c.buf)
		if err != nil {
			v2.TraceMOLoggerBufferWriteFailed.Inc()
			v2.TraceMOLoggerErrorFlushCounter.Inc()
			return 0, err
		} else {
			v2.TraceMOLoggerBufferWriteCSV.Inc()
		}
	} else {
		v2.TraceMOLoggerBufferWriteSQL.Inc()
	}

	// nil all
	if c.bufCallback == nil {
		v2.TraceMOLoggerBufferNoCallback.Inc()
	}
	c.sqlFlusher = nil
	c.csvFlusher = nil
	c.formatter = nil
	c.bufCallback = nil
	c.buf = nil
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

	sqlCsv := bytes.NewBuffer(make([]byte, 0, table.DefaultWriterBufferSize))
	for i := 0; i < buf.Len(); i++ {
		c := buf.Bytes()[i]
		if sqlCsv.Cap() == sqlCsv.Len() {
			sqlCsv.Grow(mpool.MB)
		}
		// case \: mo sql NEED quote '\'
		// case ': sql (load data inline ... DATA='' ...) will quote {sqlCsv} with "'"
		if c == '\\' || c == '\'' {
			// \ -> \\
			// ' -> ''
			sqlCsv.WriteByte(c)
		}
		sqlCsv.WriteByte(c)
	}

	loadSQL := fmt.Sprintf("LOAD DATA INLINE FORMAT='csv', DATA='%s' INTO TABLE %s.%s FIELDS TERMINATED BY ','",
		sqlCsv.Bytes(), f.database, f.table)
	v2.TraceMOLoggerExportSqlHistogram.Observe(float64(len(loadSQL)))

	_, err = conn.Exec(loadSQL)
	if len(loadSQL) > 10*mpool.MB {
		logutil.Info("generate req sql", zap.String("type", f.table), zap.Int("csv", buf.Len()), zap.Int("sql", len(loadSQL)))
	}

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

type Formatter interface {
	WriteRow(*table.Row) error
	Flush()
}

// ContentFormatter common File Format
type ContentFormatter struct {
	Formatter
}

func NewContentFormatter(ctx context.Context, buf *bytes.Buffer) Formatter {
	w := db_holder.NewCSVWriterWithBuffer(ctx, buf)
	return &ContentFormatter{
		Formatter: w,
	}
}

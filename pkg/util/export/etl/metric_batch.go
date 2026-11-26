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
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"errors"
	"io"
	"net"
	"strings"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	dbholder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

const metricReplaySQLTarget = 4 * mpool.MB

func ReplayMetricBatch(ctx context.Context, tbl *table.Table, data []byte, id uuid.UUID) error {
	return NewSQLFlusher(tbl).flushMetricBatch(ctx, data, id, false)
}

func (f *SQLFlusher) flushMetricBatch(parent context.Context, data []byte, id uuid.UUID, firstWrite bool) error {
	ctx := parent
	if firstWrite {
		// Keep the original total budget for synchronous collection.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, dbholder.MetricBatchTimeout)
		defer cancel()
	}
	var db *sql.DB
	err := dbholder.WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
		var err error
		db, err = dbholder.GetOrInitMetricDBConn(opCtx)
		return err
	})
	if err != nil {
		return err
	}
	err = dbholder.CommitMetricBatch(ctx, db, id, func(conn *sql.Conn) error {
		if firstWrite {
			return f.loadMetricCSV(ctx, conn, data)
		}
		return f.replayMetricLoads(ctx, conn, data)
	})
	if err != nil {
		if parent.Err() == nil && shouldReselectMetricCN(err) {
			dbholder.MarkMetricDBConnFailed(db)
		}
		return err
	}
	if firstWrite {
		// Only this writer can publish the CSV. A confirmed first write
		// never publishes one, so no importer can need this receipt.
		if err := dbholder.DeleteMetricBatchReceipt(ctx, db, id); err != nil {
			logutil.Warn("retain confirmed metric batch receipt", zap.String("batch_id", id.String()), zap.Error(err))
		}
	}
	return nil
}

func shouldReselectMetricCN(err error) bool {
	var netErr net.Error
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &netErr) || errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		(errors.As(err, &mysqlErr) && (mysqlErr.Number == 2006 || mysqlErr.Number == 2013 ||
			mysqlErr.Number == moerr.ER_SERVER_SHUTDOWN ||
			mysqlErr.Number == moerr.ErrNewTxnInCNRollingRestart || mysqlErr.Number == moerr.ErrRetryForCNRollingRestart))
}

func (f *SQLFlusher) loadMetricCSV(ctx context.Context, conn *sql.Conn, data []byte) error {
	query := f.loadSQL(data)
	return dbholder.WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
		_, err := conn.ExecContext(opCtx, query)
		return err
	})
}

func (f *SQLFlusher) replayMetricLoads(ctx context.Context, conn *sql.Conn, data []byte) error {
	var packetLimit int
	if err := dbholder.WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
		return conn.QueryRowContext(opCtx, "SELECT @@session.max_allowed_packet").Scan(&packetLimit)
	}); err != nil {
		return err
	}
	// The driver reserves one byte from the negotiated limit, and
	// COM_QUERY uses one more byte besides the SQL text.
	overhead := len(loadInlineCSVPrefix) + len(f.loadSuffix())
	limit := packetLimit - 2
	target := min(limit, metricReplaySQLTarget)
	reader := csv.NewReader(bytes.NewReader(data))
	reader.ReuseRecord = true
	reader.FieldsPerRecord = f.columnCount
	start, end, size := 0, 0, overhead
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		next := int(reader.InputOffset())
		rowSize := next - end + countInlineSQLEscapes(data[end:next])
		// Slice the original bytes at CSV record boundaries, including
		// records with embedded newlines. Keep all LOADs in this transaction.
		if start != end && size+rowSize > target {
			if err := f.loadMetricCSV(ctx, conn, data[start:end]); err != nil {
				return err
			}
			start, size = end, overhead
		}
		if overhead+rowSize > limit {
			if err := f.insertMetricRecord(ctx, conn, data[end:next], packetLimit); err != nil {
				return err
			}
			start, end, size = next, next, overhead
			continue
		}
		end, size = next, size+rowSize
	}
	if start != end {
		return f.loadMetricCSV(ctx, conn, data[start:end])
	}
	return nil
}

// insertMetricRecord binds the rare record that does not fit in an inline
// LOAD. Decode it with LOAD's CSV dialect, including its escapes and NULLs.
func (f *SQLFlusher) insertMetricRecord(ctx context.Context, conn *sql.Conn, record []byte, packetLimit int) error {
	cfg := csvparser.CSVConfig{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   tree.DefaultFieldsEnclosedBy,
		FieldsEscapedBy:    tree.DefaultFieldsEscapedBy,
		LinesTerminatedBy:  "\n",
		Null:               []string{`\N`},
		UnescapedQuote:     true,
		Context:            ctx,
	}
	parser, err := csvparser.NewCSVParser(&cfg, bytes.NewReader(record), csvparser.ReadBlockSize, false)
	if err != nil {
		return err
	}
	fields, err := parser.Read(nil)
	if err != nil {
		return err
	}
	if len(fields) != f.columnCount || parser.Pos() != int64(len(record)) {
		return moerr.NewInvalidInputNoCtx("invalid metric CSV record for parameterized insert")
	}

	args := make([]any, 0, len(fields))
	values := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.IsNull {
			values = append(values, "?")
			args = append(args, nil)
			continue
		}
		value := field.Val
		if len(value) <= packetLimit {
			values = append(values, "?")
			args = append(args, value)
			continue
		}

		// MO enforces max_allowed_packet on the total length of each
		// prepared parameter, even if the driver sends several packets.
		// Reassemble longer character values on the server.
		var placeholders []string
		for len(value) > 0 {
			end := min(len(value), packetLimit)
			for end < len(value) && !utf8.RuneStart(value[end]) {
				end--
			}
			if end <= 0 {
				return moerr.NewInvalidInputNoCtx("metric CSV value cannot fit in a prepared parameter")
			}
			placeholders = append(placeholders, "?")
			args = append(args, value[:end])
			value = value[end:]
		}
		values = append(values, "CONCAT("+strings.Join(placeholders, ",")+")")
	}

	query := "INSERT INTO " + f.database + "." + f.table + " VALUES (" + strings.Join(values, ",") + ")"
	return dbholder.WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
		_, err := conn.ExecContext(opCtx, query, args...)
		return err
	})
}

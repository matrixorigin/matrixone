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
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	dbholder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

const metricBatchTimeout = 10 * time.Second
const metricReplaySQLTarget = 4 * mpool.MB

func ReplayMetricBatch(ctx context.Context, tbl *table.Table, data []byte, id uuid.UUID) error {
	return NewSQLFlusher(tbl).flushMetricBatch(ctx, data, id, false, metricBatchTimeout)
}

func (f *SQLFlusher) flushMetricBatch(parent context.Context, data []byte, id uuid.UUID, firstWrite bool, timeout time.Duration) error {
	ctx, cancel := context.WithTimeoutCause(parent, timeout, moerr.CauseWriteRowRecords)
	defer cancel()
	db, err := dbholder.GetOrInitMetricDBConn(ctx)
	if err != nil {
		return err
	}
	err = dbholder.CommitMetricBatch(ctx, db, id, func(conn *sql.Conn) error {
		if firstWrite {
			_, err := conn.ExecContext(ctx, f.loadSQL(data))
			return err
		}
		return f.replayMetricLoads(ctx, conn, data)
	})
	if err != nil {
		if parent.Err() == nil && (ctx.Err() == context.DeadlineExceeded || isMetricConnectionError(err)) {
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

func isMetricConnectionError(err error) bool {
	var netErr net.Error
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &netErr) || errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		(errors.As(err, &mysqlErr) && (mysqlErr.Number == 2006 || mysqlErr.Number == 2013))
}

func (f *SQLFlusher) replayMetricLoads(ctx context.Context, conn *sql.Conn, data []byte) error {
	var packetLimit int
	if err := conn.QueryRowContext(ctx, "SELECT @@session.max_allowed_packet").Scan(&packetLimit); err != nil {
		return err
	}
	// The driver reserves one byte from the negotiated limit, and
	// COM_QUERY uses one more byte besides the SQL text.
	overhead := len(loadInlineCSVPrefix) + len(f.loadSuffix())
	limit := packetLimit - 2
	target := min(limit, metricReplaySQLTarget)
	reader := csv.NewReader(bytes.NewReader(data))
	reader.ReuseRecord = true
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
		if overhead+rowSize > limit {
			return moerr.NewInvalidInputf(ctx, "metric CSV record needs %d SQL bytes, exceeding the %d byte limit", overhead+rowSize, limit)
		}
		// Slice the original bytes at CSV record boundaries, including
		// records with embedded newlines. Keep all LOADs in this transaction.
		if start != end && size+rowSize > target {
			if _, err := conn.ExecContext(ctx, f.loadSQL(data[start:end])); err != nil {
				return err
			}
			start, size = end, overhead
		}
		end, size = next, size+rowSize
	}
	if start != end {
		_, err := conn.ExecContext(ctx, f.loadSQL(data[start:end]))
		return err
	}
	return nil
}

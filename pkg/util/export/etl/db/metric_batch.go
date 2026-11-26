// Copyright 2022 Matrix Origin
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

package db_holder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const MetricBatchTimeout = 10 * time.Second

const MetricBatchReceiptTable = "metric_batch_receipt"
const metricBatchReceiptRelation = "system_metrics." + MetricBatchReceiptTable

// Catalog DDL shared by fresh bootstrap and the upgrade entry. Keep this
// definition frozen once the upgrade is released.
const CreateMetricBatchReceipt = "CREATE TABLE IF NOT EXISTS " + metricBatchReceiptRelation +
	" (batch_id varchar(36) primary key, created_at datetime(6) not null default current_timestamp(6))"

// WithMetricBatchTimeout bounds a single database operation. A replay
// with many LOAD statements can continue while each operation makes progress.
func WithMetricBatchTimeout(parent context.Context, run func(context.Context) error) error {
	ctx, cancel := context.WithTimeoutCause(parent, MetricBatchTimeout, moerr.CauseWriteRowRecords)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	err := run(ctx)
	if ctxErr := ctx.Err(); err != nil && ctxErr != nil && !errors.Is(err, ctxErr) {
		return errors.Join(err, ctxErr)
	}
	return err
}

func execMetricBatch(ctx context.Context, conn *sql.Conn, query string, args ...any) error {
	return WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
		_, err := conn.ExecContext(opCtx, query, args...)
		return err
	})
}

func CommitMetricBatch(ctx context.Context, db *sql.DB, id uuid.UUID, load func(*sql.Conn) error) error {
	err := insertMetricBatch(ctx, db, id.String(), load)
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) ||
		(mysqlErr.Number != moerr.ER_DUP_ENTRY && mysqlErr.Number != moerr.ErrPrimaryKeyDuplicated) {
		return err
	}

	// The attempt has released its transaction and connection. Use a new
	// statement to observe a committed receipt, including commit-time conflicts.
	var committed bool
	checkErr := WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
		return db.QueryRowContext(opCtx, "select exists(select 1 from "+metricBatchReceiptRelation+
			" where batch_id = ?)", id.String()).Scan(&committed)
	})
	if checkErr != nil {
		return errors.Join(err, checkErr)
	}
	if committed {
		return nil
	}
	return err
}

func insertMetricBatch(ctx context.Context, db *sql.DB, id string, load func(*sql.Conn) error) error {
	var conn *sql.Conn
	err := WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
		var err error
		conn, err = db.Conn(opCtx)
		return err
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	discard := func() {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	// Pin one connection and use context-aware commands through COMMIT.
	// ExecContext on sql.Conn never retries a COMMIT on another connection.
	if err = execMetricBatch(ctx, conn, "START TRANSACTION"); err != nil {
		discard()
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rollbackErr := execMetricBatch(ctx, conn, "ROLLBACK"); rollbackErr == nil {
			return
		}
		// Canceled or unclean transactions cannot return to the shared pool.
		discard()
	}()

	if err = execMetricBatch(ctx, conn, "insert into "+metricBatchReceiptRelation+" (batch_id) values (?)", id); err != nil {
		return err
	}
	if err = load(conn); err != nil {
		return err
	}
	err = execMetricBatch(ctx, conn, "COMMIT")
	committed = err == nil
	return err
}

func DeleteMetricBatchReceipt(ctx context.Context, db *sql.DB, id uuid.UUID) error {
	return WithMetricBatchTimeout(ctx, func(opCtx context.Context) error {
		_, err := db.ExecContext(opCtx, "delete from "+metricBatchReceiptRelation+" where batch_id = ?", id.String())
		return err
	})
}

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

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const MetricBatchReceiptTable = "metric_batch_receipt"
const metricBatchReceiptRelation = "system_metrics." + MetricBatchReceiptTable

// Catalog DDL shared by fresh bootstrap and the upgrade entry. Keep this
// definition frozen once the upgrade is released.
const CreateMetricBatchReceipt = "CREATE TABLE IF NOT EXISTS " + metricBatchReceiptRelation +
	" (batch_id varchar(36) primary key, created_at datetime(6) not null default current_timestamp(6))"

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
	checkErr := db.QueryRowContext(ctx, "select exists(select 1 from "+metricBatchReceiptRelation+
		" where batch_id = ?)", id.String()).Scan(&committed)
	if checkErr != nil {
		return errors.Join(err, checkErr)
	}
	if committed {
		return nil
	}
	return err
}

func insertMetricBatch(ctx context.Context, db *sql.DB, id string, load func(*sql.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	discard := func() {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	// Pin one connection and use context-aware commands through COMMIT.
	// ExecContext on sql.Conn never retries a COMMIT on another connection.
	if _, err = conn.ExecContext(ctx, "START TRANSACTION"); err != nil {
		discard()
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if ctx.Err() == nil {
			if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr == nil {
				return
			}
		}
		// Canceled or unclean transactions cannot return to the shared pool.
		discard()
	}()

	if _, err = conn.ExecContext(ctx, "insert into "+metricBatchReceiptRelation+" (batch_id) values (?)", id); err != nil {
		return err
	}
	if err = load(conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	committed = err == nil
	return err
}

func DeleteMetricBatchReceipt(ctx context.Context, db *sql.DB, id uuid.UUID) error {
	_, err := db.ExecContext(ctx, "delete from "+metricBatchReceiptRelation+" where batch_id = ?", id.String())
	return err
}

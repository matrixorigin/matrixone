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

func CommitMetricBatch(ctx context.Context, db *sql.DB, id uuid.UUID, loadSQL string) error {
	err := insertMetricBatch(ctx, db, id.String(), loadSQL)
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

func insertMetricBatch(ctx context.Context, db *sql.DB, id string, loadSQL string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, "insert into "+metricBatchReceiptRelation+" (batch_id) values (?)", id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, loadSQL); err != nil {
		return err
	}
	return tx.Commit()
}

func DeleteMetricBatchReceipt(ctx context.Context, db *sql.DB, id uuid.UUID) error {
	_, err := db.ExecContext(ctx, "delete from "+metricBatchReceiptRelation+" where batch_id = ?", id.String())
	return err
}

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
	"context"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	dbholder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

func ReplayMetricBatch(ctx context.Context, tbl *table.Table, data []byte, id uuid.UUID) error {
	return NewSQLFlusher(tbl).flushMetricBatch(ctx, data, id, false)
}

func (f *SQLFlusher) flushMetricBatch(ctx context.Context, data []byte, id uuid.UUID, firstWrite bool) error {
	forceNew := !dbholder.DBConnErrCount.Check()
	conn, err := dbholder.GetOrInitDBConn(forceNew, firstWrite || forceNew)
	if err == nil {
		err = dbholder.CommitMetricBatch(ctx, conn, id, f.loadSQL(data))
	}
	if err != nil {
		if ctx.Err() == nil {
			_ = dbholder.DBConnErrCount.Count()
		}
		return err
	}
	dbholder.DBConnErrCount.Reset()
	if firstWrite {
		// Only this writer can publish the CSV. A confirmed first write
		// never publishes one, so no importer can need this receipt.
		if err := dbholder.DeleteMetricBatchReceipt(ctx, conn, id); err != nil {
			logutil.Warn("retain confirmed metric batch receipt", zap.String("batch_id", id.String()), zap.Error(err))
		}
	}
	return nil
}

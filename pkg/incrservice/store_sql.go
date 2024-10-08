// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	database      = "mo_catalog"
	incrTableName = "mo_increment_columns"
)

func (c AutoColumn) getInsertSQL() string {
	return fmt.Sprintf(`insert into %s(table_id, col_name, col_index, offset, step) 
		values(%d, '%s', %d, %d, %d)`,
		incrTableName,
		c.TableID,
		c.ColName,
		c.ColIndex,
		c.Offset,
		c.Step)
}

type sqlStore struct {
	ls   lockservice.LockService
	exec executor.SQLExecutor
}

func NewSQLStore(
	exec executor.SQLExecutor,
	ls lockservice.LockService,
) (IncrValueStore, error) {
	return &sqlStore{
		exec: exec,
		ls:   ls,
	}, nil
}

func (s *sqlStore) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn,
	txnOp client.TxnOperator) error {
	opts := executor.Options{}.
		WithDatabase(database).
		WithTxn(txnOp).
		WithWaitCommittedLogApplied()
	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
	} else {
		opts = opts.WithEnableTrace()
	}

	return s.exec.ExecTxn(
		ctx,
		func(te executor.TxnExecutor) error {
			for _, col := range cols {
				res, err := te.Exec(col.getInsertSQL(), executor.StatementOption{})
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		},
		opts)
}

func (s *sqlStore) Allocate(
	ctx context.Context,
	tableID uint64,
	colName string,
	count int,
	txnOp client.TxnOperator,
) (uint64, uint64, timestamp.Timestamp, error) {
	var current, next, step uint64
	ok := false

	fetchSQL := fmt.Sprintf(`select offset, step from %s where table_id = %d and col_name = '%s' for update`,
		incrTableName,
		tableID,
		colName)
	opts := executor.Options{}.
		WithDatabase(database).
		WithTxn(txnOp).
		WithWaitCommittedLogApplied() // make sure the update is visible to the subsequence txn, wait log tail applied
	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
	} else {
		opts = opts.WithEnableTrace()
	}

	ctxDone := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}
	retry := false
	for {
		err := s.exec.ExecTxn(
			ctx,
			func(te executor.TxnExecutor) error {
				txnOp = te.Txn()
				start := time.Now()
				res, err := te.Exec(fetchSQL, executor.StatementOption{})
				if err != nil {
					return err
				}
				rows := 0
				res.ReadRows(func(_ int, cols []*vector.Vector) bool {
					current = executor.GetFixedRows[uint64](cols[0])[0]
					step = executor.GetFixedRows[uint64](cols[1])[0]
					rows++
					return true
				})
				res.Close()

				if rows != 1 {
					accountID, err := defines.GetAccountId(ctx)
					if err != nil {
						return err
					}
					trace.GetService(s.ls.GetConfig().ServiceID).Sync()
					getLogger(s.ls.GetConfig().ServiceID).Fatal("BUG: read incr record invalid",
						zap.String("fetch-sql", fetchSQL),
						zap.Any("account", accountID),
						zap.Uint64("table", tableID),
						zap.String("col", colName),
						zap.Int("rows", rows),
						zap.Duration("cost", time.Since(start)),
						zap.Bool("ctx-done", ctxDone()))
				}

				next = getNext(current, count, int(step))
				sql := fmt.Sprintf(`update %s set offset = %d 
				where table_id = %d and col_name = '%s' and offset = %d`,
					incrTableName,
					next,
					tableID,
					colName,
					current)
				start = time.Now()
				res, err = te.Exec(sql, executor.StatementOption{})
				if err != nil {
					return err
				}

				if res.AffectedRows == 1 {
					ok = true
				} else if ctxDone() {
					return ctx.Err()
				} else {
					ctx2, cancel := context.WithTimeout(context.Background(), time.Second*30)
					defer cancel()
					ok, err := s.ls.IsOrphanTxn(ctx2, txnOp.Txn().ID)
					if ok || err != nil {
						retry = true
						return moerr.NewTxnNeedRetryNoCtx()
					}

					accountID, err := defines.GetAccountId(ctx)
					if err != nil {
						return err
					}
					trace.GetService(s.ls.GetConfig().ServiceID).Sync()
					getLogger(s.ls.GetConfig().ServiceID).Error("pre lock released by lock table changed",
						zap.String("update-sql", sql),
						zap.Any("account", accountID),
						zap.Uint64("table", tableID),
						zap.String("col", colName),
						zap.Uint64("affected-rows", res.AffectedRows),
						zap.Duration("cost", time.Since(start)),
						zap.Bool("ctx-done", ctxDone()))
					retry = true
					return moerr.NewTxnNeedRetryNoCtx()
				}
				res.Close()
				return nil
			},
			opts)
		if err != nil {
			// retry ww conflict if the txn is not pessimistic
			if retry ||
				(txnOp != nil && !txnOp.Txn().IsPessimistic() &&
					moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict)) {
				continue
			}

			return 0, 0, timestamp.Timestamp{}, err
		}
		if ok {
			break
		}
	}

	from, to := getNextRange(current, next, int(step))
	commitTs := txnOp.GetOverview().Meta.CommitTS
	return from, to, commitTs, nil
}

func (s *sqlStore) UpdateMinValue(
	ctx context.Context,
	tableID uint64,
	col string,
	minValue uint64,
	txnOp client.TxnOperator) error {
	opts := executor.Options{}.
		WithDatabase(database).
		WithTxn(txnOp)

	// txnOp is nil means the auto increment metadata is already insert into catalog.MOAutoIncrTable and committed.
	// So updateMinValue will use a new txn to update the min value. To avoid w-w conflict, we need to wait this
	// committed log tail applied to ensure subsequence txn must get a snapshot ts which is large than this commit.
	if txnOp == nil {
		opts = opts.WithWaitCommittedLogApplied().
			WithEnableTrace()
	} else {
		opts = opts.WithDisableIncrStatement()
	}
	res, err := s.exec.Exec(
		ctx,
		fmt.Sprintf("update %s set offset = %d where table_id = %d and col_name = '%s' and offset < %d",
			incrTableName,
			minValue,
			tableID,
			col,
			minValue),
		opts)
	if err != nil {
		return err
	}
	defer res.Close()
	return nil
}

func (s *sqlStore) Delete(ctx context.Context, tableID uint64) error {
	opts := executor.Options{}.
		WithDatabase(database).
		WithEnableTrace().
		WithWaitCommittedLogApplied()

	return s.exec.ExecTxn(ctx,
		func(txn executor.TxnExecutor) error {
			var tenantId uint32
			tenantId, err := defines.GetAccountId(ctx)
			if err != nil {
				return err
			}

			sql := fmt.Sprintf("select account_name from mo_catalog.mo_account where account_id = %d", tenantId)
			newCtx := defines.AttachAccountId(ctx, catalog.System_Account)
			res, err := s.exec.Exec(newCtx, sql, opts)
			if err != nil {
				return err
			}
			var rowCount int
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				rowCount += rows
				return true
			})
			if rowCount == 0 {
				res.Close()
				return nil
			}
			res, err = s.exec.Exec(
				ctx,
				fmt.Sprintf("delete from %s where table_id = %d",
					incrTableName, tableID),
				opts)
			res.Close()
			return err
		},
		opts)
}

func (s *sqlStore) GetColumns(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator) ([]AutoColumn, error) {
	fetchSQL := fmt.Sprintf(`select col_name, col_index, offset, step from %s where table_id = %d order by col_index`,
		incrTableName,
		tableID)
	opts := executor.Options{}.
		WithDatabase(database).
		WithTxn(txnOp)

	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
	} else {
		opts = opts.WithEnableTrace()
	}

	res, err := s.exec.Exec(ctx, fetchSQL, opts)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var colNames []string
	var indexes []int32
	var offsets []uint64
	var steps []uint64
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		colNames = append(colNames, executor.GetStringRows(cols[0])...)
		indexes = append(indexes, executor.GetFixedRows[int32](cols[1])...)
		offsets = append(offsets, executor.GetFixedRows[uint64](cols[2])...)
		steps = append(steps, executor.GetFixedRows[uint64](cols[3])...)
		return true
	})

	cols := make([]AutoColumn, len(colNames))
	for idx, colName := range colNames {
		cols[idx] = AutoColumn{
			TableID:  tableID,
			ColName:  colName,
			ColIndex: int(indexes[idx]),
			Offset:   offsets[idx],
			Step:     steps[idx],
		}
	}
	return cols, nil
}

func (s *sqlStore) Close() {

}

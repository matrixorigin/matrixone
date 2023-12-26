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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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
	exec executor.SQLExecutor
}

func NewSQLStore(exec executor.SQLExecutor) (IncrValueStore, error) {
	return &sqlStore{exec: exec}, nil
}

func (s *sqlStore) NewTxnOperator(ctx context.Context) client.TxnOperator {
	return s.exec.NewTxnOperator(ctx)
}

// only use for debug
func (s *sqlStore) SelectAll(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator) string {
	fetchSQL := fmt.Sprintf(`select col_name, table_id from %s`, incrTableName)
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
	}
	res, err := s.exec.Exec(ctx, fetchSQL, opts)
	if err != nil {
		return ""
	}
	defer res.Close()

	str := fmt.Sprintf("Cannot find tableID %d in table %s, accountid %d, txn: %s", tableID, incrTableName,
		ctx.Value(defines.TenantIDKey{}), txnOp.Txn().DebugString())
	res.ReadRows(func(cols []*vector.Vector) bool {
		str += fmt.Sprintf("\tcol_name: %s, table_id: %d\n",
			executor.GetStringRows(cols[0])[0],
			executor.GetFixedRows[uint64](cols[1])[0])
		return true
	})
	return str
}

func (s *sqlStore) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn,
	txnOp client.TxnOperator) error {
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp).WithWaitCommittedLogApplied()
	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
	}

	return s.exec.ExecTxn(
		ctx,
		func(te executor.TxnExecutor) error {
			for _, col := range cols {
				res, err := te.Exec(col.getInsertSQL())
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
	txnOp client.TxnOperator) (uint64, uint64, error) {
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
	}
	ctxDone := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}
	for {
		err := s.exec.ExecTxn(
			ctx,
			func(te executor.TxnExecutor) error {
				start := time.Now()
				res, err := te.Exec(fetchSQL)
				if err != nil {
					return err
				}
				rows := 0
				res.ReadRows(func(cols []*vector.Vector) bool {
					current = executor.GetFixedRows[uint64](cols[0])[0]
					step = executor.GetFixedRows[uint64](cols[1])[0]
					rows++
					return true
				})
				res.Close()

				if rows != 1 {
					getLogger().Fatal("BUG: read incr record invalid",
						zap.String("fetch-sql", fetchSQL),
						zap.Any("account", ctx.Value(defines.TenantIDKey{})),
						zap.Uint64("table", tableID),
						zap.String("col", colName),
						zap.Int("rows", rows),
						zap.Duration("cost", time.Since(start)),
						zap.String("select-all", s.SelectAll(ctx, tableID, txnOp)),
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
				res, err = te.Exec(sql)
				if err != nil {
					return err
				}

				if res.AffectedRows == 1 {
					ok = true
				} else {
					getLogger().Fatal("BUG: update incr record returns invalid affected rows",
						zap.String("update-sql", sql),
						zap.Any("account", ctx.Value(defines.TenantIDKey{})),
						zap.Uint64("table", tableID),
						zap.String("col", colName),
						zap.Uint64("affected-rows", res.AffectedRows),
						zap.String("select-all", s.SelectAll(ctx, tableID, txnOp)),
						zap.Duration("cost", time.Since(start)),
						zap.Bool("ctx-done", ctxDone()))
				}
				res.Close()
				return nil
			},
			opts)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			break
		}
	}

	from, to := getNextRange(current, next, int(step))
	return from, to, nil
}

func (s *sqlStore) UpdateMinValue(
	ctx context.Context,
	tableID uint64,
	col string,
	minValue uint64,
	txnOp client.TxnOperator) error {
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
	// txnOp is nil means the auto increment metadata is already insert into catalog.MOAutoIncrTable and committed.
	// So updateMinValue will use a new txn to update the min value. To avoid w-w conflict, we need to wait this
	// committed log tail applied to ensure subsequence txn must get a snapshot ts which is large than this commit.
	if txnOp == nil {
		opts = opts.WithWaitCommittedLogApplied()
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

func (s *sqlStore) Delete(
	ctx context.Context,
	tableID uint64) error {
	opts := executor.Options{}.
		WithDatabase(database).
		WithWaitCommittedLogApplied()
	res, err := s.exec.Exec(
		ctx,
		fmt.Sprintf("delete from %s where table_id = %d",
			incrTableName, tableID),
		opts)
	if err != nil {
		return err
	}
	defer res.Close()
	return nil
}

func (s *sqlStore) GetColumns(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator) ([]AutoColumn, error) {
	fetchSQL := fmt.Sprintf(`select col_name, col_index, offset, step from %s where table_id = %d order by col_index`,
		incrTableName,
		tableID)
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
	if txnOp != nil {
		opts = opts.WithDisableIncrStatement()
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
	res.ReadRows(func(cols []*vector.Vector) bool {
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

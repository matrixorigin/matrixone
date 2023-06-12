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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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

func (s *sqlStore) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn,
	txnOp client.TxnOperator) error {
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
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
	var curr, next, step uint64
	ok := false

	fetchSQL := fmt.Sprintf(`select offset, step from %s where table_id = %d and col_name = '%s'`,
		incrTableName,
		tableID,
		colName)
	opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
	for {
		err := s.exec.ExecTxn(
			ctx,
			func(te executor.TxnExecutor) error {
				res, err := te.Exec(fetchSQL)
				if err != nil {
					return err
				}
				res.ReadRows(func(cols []*vector.Vector) bool {
					curr = executor.GetFixedRows[uint64](cols[0])[0]
					step = executor.GetFixedRows[uint64](cols[1])[0]
					return true
				})
				res.Close()

				next = getNext(curr, count, int(step))
				res, err = te.Exec(fmt.Sprintf(`update %s set offset = %d 
					where table_id = %d and col_name = '%s' and offset = %d`,
					incrTableName,
					next,
					tableID,
					colName,
					curr))
				if err != nil {
					return err
				}

				if res.AffectedRows == 1 {
					ok = true
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

	from, to := getNextRange(curr, next, int(step))
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

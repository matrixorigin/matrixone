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

package executor

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type memExecutor struct {
	mocker func(sql string) (Result, error)
}

// NewMemExecutor used to testing
func NewMemExecutor(mocker func(sql string) (Result, error)) SQLExecutor {
	return &memExecutor{mocker: mocker}
}

func (e *memExecutor) NewTxnOperator(_ context.Context) client.TxnOperator {
	return nil
}

func (e *memExecutor) Exec(
	ctx context.Context,
	sql string,
	opts Options) (Result, error) {
	return e.mocker(sql)
}

func (e *memExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(TxnExecutor) error,
	opts Options) error {
	te := &memTxnExecutor{mocker: e.mocker}
	return execFunc(te)
}

type memTxnExecutor struct {
	mocker func(sql string) (Result, error)
}

func (te *memTxnExecutor) Exec(sql string, _ StatementOption) (Result, error) {
	return te.mocker(sql)
}

func (te *memTxnExecutor) Use(db string) {

}

func (te *memTxnExecutor) LockTable(table string) error {
	return nil
}

func (te *memTxnExecutor) Txn() client.TxnOperator {
	return nil
}

// MemResult used to test. Construct a Result from memory.
type MemResult struct {
	cols  int
	types []types.Type
	res   Result
}

func NewMemResult(
	types []types.Type,
	mp *mpool.MPool) *MemResult {
	return &MemResult{res: Result{mp: mp}, types: types, cols: len(types)}
}

func (m *MemResult) NewBatch() {
	m.res.Batches = append(m.res.Batches, newBatch(m.cols))
}

func (m *MemResult) GetResult() Result {
	return m.res
}

func AppendStringRows(m *MemResult, col int, values []string) error {
	bat := m.res.Batches[len(m.res.Batches)-1]
	return appendStringCols(bat, col, m.types[col], values, m.res.mp)
}

func AppendBytesRows(m *MemResult, col int, values [][]byte) error {
	bat := m.res.Batches[len(m.res.Batches)-1]
	return appendBytesCols(bat, col, m.types[col], values, m.res.mp)
}

func AppendFixedRows[T any](m *MemResult, col int, values []T) error {
	bat := m.res.Batches[len(m.res.Batches)-1]
	return appendCols(bat, col, m.types[col], values, m.res.mp)
}

func newBatch(cols int) *batch.Batch {
	bat := batch.NewWithSize(cols)
	bat.SetRowCount(cols)
	return bat
}

func appendCols[T any](
	bat *batch.Batch,
	colIndex int,
	tp types.Type,
	values []T,
	mp *mpool.MPool) error {

	col := vector.NewVec(tp)
	if err := vector.AppendFixedList(col, values, nil, mp); err != nil {
		return err
	}
	bat.Vecs[colIndex] = col
	return nil
}

func appendBytesCols(
	bat *batch.Batch,
	colIndex int,
	tp types.Type,
	values [][]byte,
	mp *mpool.MPool) error {

	col := vector.NewVec(tp)
	for _, v := range values {
		if err := vector.AppendBytes(col, v[:], false, mp); err != nil {
			return err
		}
	}
	bat.Vecs[colIndex] = col
	return nil
}

func appendStringCols(
	bat *batch.Batch,
	colIndex int,
	tp types.Type,
	values []string,
	mp *mpool.MPool) error {

	col := vector.NewVec(tp)
	for _, v := range values {
		if err := vector.AppendBytes(col, []byte(v), false, mp); err != nil {
			return err
		}
	}
	bat.Vecs[colIndex] = col
	return nil
}

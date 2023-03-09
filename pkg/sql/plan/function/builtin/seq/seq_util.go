// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-3.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package seq

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

var Sequence_cols_name = []string{"last_seq_num", "min_value", "max_value", "start_value", "increment_value", "cycle", "is_called", catalog.Row_ID}
var setEdge = true

func getValues[T constraints.Integer](vecs []*vector.Vector) (T, T, T, T, int64, bool, bool) {
	return vector.MustTCols[T](vecs[0])[0], vector.MustTCols[T](vecs[1])[0], vector.MustTCols[T](vecs[2])[0],
		vector.MustTCols[T](vecs[3])[0], vector.MustTCols[int64](vecs[4])[0], vector.MustTCols[bool](vecs[5])[0],
		vector.MustTCols[bool](vecs[6])[0]
}

func simpleUpdate(proc *process.Process,
	bat *batch.Batch, rel engine.Relation) error {
	var delBatch, updateBatch *batch.Batch
	var err error
	defer func() {
		if delBatch != nil {
			delBatch.Clean(proc.Mp())
		}
		// Updatebatch here is just bat.
		// It will be cleaned in other place.
	}()

	// attrs := getAttrsFromTableDefs(tableDefs)

	// Make delBatch and updateBatch.
	// The update batch is made based on the input param bat.
	delBatch, updateBatch, err = makeDeleteAndUpdateBatch(proc, bat)
	updateBatch.Cnt = 0
	updateBatch.Ro = true

	if err != nil {
		return err
	}

	if delBatch.Length() > 0 {
		// delete old rows
		err = rel.Delete(proc.Ctx, delBatch, catalog.Row_ID)
		if err != nil {
			return err
		}

		// No check for new rows not null
		// Because sequence table will never got nulls or something.

		// write table
		err = rel.Write(proc.Ctx, updateBatch)
		if err != nil {
			return err
		}
	}
	return nil
}

func makeDeleteAndUpdateBatch(proc *process.Process, bat *batch.Batch) (*batch.Batch, *batch.Batch, error) {
	// get delete batch
	delVec := vector.New(types.T_Rowid.ToType())
	err := delVec.Append(vector.MustTCols[types.Rowid](bat.Vecs[7])[0], false, proc.Mp())
	if err != nil {
		return nil, nil, err
	}
	delBatch := batch.New(true, []string{catalog.Row_ID})
	delBatch.SetVector(0, delVec)
	delBatch.SetZs(1, proc.Mp())

	// get update batch
	bat.Attrs = bat.Attrs[:7]
	bat.Vecs = bat.Vecs[:7]

	return delBatch, bat, nil
}

func NewTxn(eg engine.Engine, proc *process.Process, ctx context.Context) (txn client.TxnOperator, err error) {
	if proc.TxnClient == nil {
		return nil, moerr.NewInternalError(ctx, "must set txn client")
	}
	txn, err = proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, moerr.NewInternalError(ctx, "context should not be nil")
	}
	if err = eg.New(ctx, txn); err != nil {
		return nil, err
	}
	return txn, nil
}

func CommitTxn(eg engine.Engine, txn client.TxnOperator, ctx context.Context) error {
	if txn == nil {
		return nil
	}
	if ctx == nil {
		return moerr.NewInternalError(ctx, "context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		eg.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err := eg.Commit(ctx, txn); err != nil {
		if err2 := RolllbackTxn(eg, txn, ctx); err2 != nil {
			logutil.Errorf("CommitTxn: txn operator rollback failed. error:%v", err2)
		}
		return err
	}
	err := txn.Commit(ctx)
	txn = nil
	return err
}

func RolllbackTxn(eg engine.Engine, txn client.TxnOperator, ctx context.Context) error {
	if txn == nil {
		return nil
	}
	if ctx == nil {
		return moerr.NewInternalError(ctx, "context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		eg.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err := eg.Rollback(ctx, txn); err != nil {
		return err
	}
	err := txn.Rollback(ctx)
	txn = nil
	return err
}

// Copyright 2021 - 2022 Matrix Origin
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

package seq

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var Sequence_cols_name = []string{"last_seq_num", "min_value", "max_value", "start_value", "increment_value", "cycle", "is_called", catalog.Row_ID}
var setEdge = true

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
		if err2 := RollbackTxn(eg, txn, ctx); err2 != nil {
			logutil.Errorf("CommitTxn: txn operator rollback failed. error:%v", err2)
		}
		return err
	}
	err := txn.Commit(ctx)
	txn = nil
	return err
}

func RollbackTxn(eg engine.Engine, txn client.TxnOperator, ctx context.Context) error {
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

// func makeBatch(values []interface{}, proc *process.Process) (*batch.Batch, error) {
// 	var bat batch.Batch
// 	var typ types.Type
// 	bat.Ro = true
// 	bat.Cnt = 0
// 	bat.Zs = make([]int64, 1)
// 	bat.Zs[0] = 1
// 	attrs := make([]string, len(Sequence_cols_name))
// 	for i := range attrs {
// 		attrs[i] = Sequence_cols_name[i]
// 	}
// 	bat.Attrs = attrs
// 	sequence_cols_num := 7
// 	vecs := make([]*vector.Vector, sequence_cols_num)
// 	switch values[0].(type) {
// 	case int16:
// 		typ = types.T_int16.ToType()
// 		err := makeVecs(vecs, typ, proc, values[0].(int16), values[1].(int16), values[2].(int16), values[3].(int16), values[4].(int64), values[5].(bool), values[6].(bool))
// 		if err != nil {
// 			return nil, err
// 		}
// 	case int32:
// 		typ = types.T_int32.ToType()
// 		err := makeVecs(vecs, typ, proc, values[0].(int32), values[1].(int32), values[2].(int32), values[3].(int32), values[4].(int64), values[5].(bool), values[6].(bool))
// 		if err != nil {
// 			return nil, err
// 		}
// 	case int64:
// 		typ = types.T_int64.ToType()
// 		err := makeVecs(vecs, typ, proc, values[0].(int64), values[1].(int64), values[2].(int64), values[3].(int64), values[4].(int64), values[5].(bool), values[6].(bool))
// 		if err != nil {
// 			return nil, err
// 		}
// 	case uint16:
// 		typ = types.T_uint16.ToType()
// 		err := makeVecs(vecs, typ, proc, values[0].(uint16), values[1].(uint16), values[2].(uint16), values[3].(uint16), values[4].(int64), values[5].(bool), values[6].(bool))
// 		if err != nil {
// 			return nil, err
// 		}
// 	case uint32:
// 		typ = types.T_uint32.ToType()
// 		err := makeVecs(vecs, typ, proc, values[0].(uint32), values[1].(uint32), values[2].(uint32), values[3].(uint32), values[4].(int64), values[5].(bool), values[6].(bool))
// 		if err != nil {
// 			return nil, err
// 		}
// 	case uint64:
// 		typ = types.T_uint64.ToType()
// 		err := makeVecs(vecs, typ, proc, values[0].(uint64), values[1].(uint64), values[2].(uint64), values[3].(uint64), values[4].(int64), values[5].(bool), values[6].(bool))
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	bat.Vecs = vecs
// 	return &bat, nil
// }
//
// func makeVecs[T constraints.Integer](vecs []*vector.Vector, typ types.Type, proc *process.Process, seqN, minV, maxV, startN T, incr int64, cycle, iscalled bool) error {
// 	for i := 0; i < 4; i++ {
// 		vecs[i] = vector.NewVec(typ)
// 	}
// 	err := vector.AppendAny(vecs[0], seqN, false, proc.Mp())
// 	if err != nil {
// 		return err
// 	}
// 	err = vector.AppendAny(vecs[1], minV, false, proc.Mp())
// 	if err != nil {
// 		return err
// 	}
// 	err = vector.AppendAny(vecs[2], maxV, false, proc.Mp())
// 	if err != nil {
// 		return err
// 	}
// 	err = vector.AppendAny(vecs[3], startN, false, proc.Mp())
// 	if err != nil {
// 		return err
// 	}
// 	vecs[4] = vector.NewVec(types.T_int64.ToType())
// 	for i := 5; i <= 6; i++ {
// 		vecs[i] = vector.NewVec(types.T_bool.ToType())
// 	}
// 	err = vector.AppendAny(vecs[4], incr, false, proc.Mp())
// 	if err != nil {
// 		return err
// 	}
// 	err = vector.AppendAny(vecs[5], cycle, false, proc.Mp())
// 	if err != nil {
// 		return err
// 	}
// 	err = vector.AppendAny(vecs[6], iscalled, false, proc.Mp())
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
//

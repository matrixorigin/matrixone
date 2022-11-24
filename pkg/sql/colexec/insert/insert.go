// Copyright 2021 Matrix Origin
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

package insert

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/client"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct {
	Ts            uint64
	TargetTable   engine.Relation
	TargetColDefs []*plan.ColDef
	Affected      uint64
	Engine        engine.Engine
	DB            engine.Database
	TableID       string
	CPkeyColDef   *plan.ColDef
	DBName        string
	TableName     string
	IndexTables   []engine.Relation
	IndexInfos    []*plan.IndexInfo
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
}

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert select")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func handleWrite(n *Argument, proc *process.Process, ctx context.Context, bat *batch.Batch) error {
	defer bat.Clean(proc.Mp())
	// XXX The original logic was buggy and I had to temporarily circumvent it
	if bat.Length() == 0 {
		bat.SetZs(bat.GetVector(0).Length(), proc.Mp())
	}
	for idx, info := range n.IndexInfos {
		b, rowNum := util.BuildUniqueKeyBatch(bat.Vecs, bat.Attrs, info.Cols, proc)
		if rowNum != 0 {
			err := n.IndexTables[idx].Write(ctx, b)
			if err != nil {
				return err
			}
		}
		b.Clean(proc.Mp())
	}
	if err := n.TargetTable.Write(ctx, bat); err != nil {
		return err
	}
	atomic.AddUint64(&n.Affected, uint64(bat.Vecs[0].Length()))
	return nil
}

func NewTxn(n *Argument, proc *process.Process, ctx context.Context) (txn client.TxnOperator, err error) {
	if proc.TxnClient == nil {
		return nil, moerr.NewInternalError("must set txn client")
	}
	txn, err = proc.TxnClient.New()
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, moerr.NewInternalError("context should not be nil")
	}
	if err = n.Engine.New(ctx, txn); err != nil {
		return nil, err
	}
	return txn, nil
}

func CommitTxn(n *Argument, txn client.TxnOperator, ctx context.Context) error {
	if txn == nil {
		return nil
	}
	if ctx == nil {
		return moerr.NewInternalError("context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		n.Engine.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err := n.Engine.Commit(ctx, txn); err != nil {
		if err2 := RolllbackTxn(n, txn, ctx); err2 != nil {
			logutil.Errorf("CommitTxn: txn operator rollback failed. error:%v", err2)
		}
		return err
	}
	err := txn.Commit(ctx)
	txn = nil
	return err
}

func RolllbackTxn(n *Argument, txn client.TxnOperator, ctx context.Context) error {
	if txn == nil {
		return nil
	}
	if ctx == nil {
		return moerr.NewInternalError("context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		n.Engine.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err := n.Engine.Rollback(ctx, txn); err != nil {
		return err
	}
	err := txn.Rollback(ctx)
	txn = nil
	return err
}

func GetNewRelation(n *Argument, txn client.TxnOperator, proc *process.Process, ctx context.Context) (engine.Relation, error) {
	dbHandler, err := n.Engine.Database(ctx, n.DBName, txn)
	if err != nil {
		return nil, err
	}
	tableHandler, err := dbHandler.Relation(ctx, n.TableName)
	if err != nil {
		return nil, err
	}
	return tableHandler, nil
}

func handleLoadWrite(n *Argument, proc *process.Process, ctx context.Context, bat *batch.Batch) (bool, error) {
	var err error
	proc.TxnOperator, err = NewTxn(n, proc, ctx)
	if err != nil {
		return false, err
	}

	n.TargetTable, err = GetNewRelation(n, proc.TxnOperator, proc, ctx)
	if err != nil {
		return false, err
	}
	if err = handleWrite(n, proc, ctx, bat); err != nil {
		if err2 := RolllbackTxn(n, proc.TxnOperator, ctx); err2 != nil {
			return false, err2
		}
		return false, err
	}

	n.Affected += uint64(len(bat.Zs))
	if err = CommitTxn(n, proc.TxnOperator, ctx); err != nil {
		return false, err
	}
	return false, nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	n := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	defer bat.Clean(proc.Mp())
	{
		for i := range bat.Vecs {
			// Not-null check, for more information, please refer to the comments in func InsertValues
			if (n.TargetColDefs[i].Primary && !n.TargetColDefs[i].Typ.AutoIncr) || (n.TargetColDefs[i].Default != nil && !n.TargetColDefs[i].Default.NullAbility && !n.TargetColDefs[i].Typ.AutoIncr) {
				if nulls.Any(bat.Vecs[i].Nsp) {
					return false, moerr.NewConstraintViolation(fmt.Sprintf("Column '%s' cannot be null", n.TargetColDefs[i].GetName()))
				}
			}
		}
	}
	{
		bat.Ro = false
		bat.Attrs = make([]string, len(bat.Vecs))
		// scalar vector's extension
		for i := range bat.Vecs {
			bat.Attrs[i] = n.TargetColDefs[i].GetName()
			bat.Vecs[i] = bat.Vecs[i].ConstExpand(proc.Mp())
		}
	}
	if err := colexec.UpdateInsertBatch(n.Engine, proc.Ctx, proc, n.TargetColDefs, bat, n.TableID, n.DBName, n.TableName); err != nil {
		return false, err
	}
	if n.CPkeyColDef != nil {
		err := util.FillCompositePKeyBatch(bat, n.CPkeyColDef, proc)
		if err != nil {
			names := util.SplitCompositePrimaryKeyColumnName(n.CPkeyColDef.Name)
			for _, name := range names {
				for i := range bat.Vecs {
					if n.TargetColDefs[i].Name == name {
						if nulls.Any(bat.Vecs[i].Nsp) {
							return false, moerr.NewConstraintViolation(fmt.Sprintf("Column '%s' cannot be null", n.TargetColDefs[i].GetName()))
						}
					}
				}
			}

		}
	}
	// set null value's data
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.CheckInsertVector(bat.Vecs[i], proc.Mp())
	}
	if !proc.LoadTag {
		return false, handleWrite(n, proc, proc.Ctx, bat)
	}
	return handleLoadWrite(n, proc, proc.Ctx, bat)
}

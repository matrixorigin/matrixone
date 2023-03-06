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

package inside

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func getCurrentAutoIncrement(e engine.Engine, proc *process.Process, colName string, dbName, tblName string) (uint64, error) {
	autoIncrCaches := proc.SessionInfo.AutoIncrCaches
	if autoIncrCaches.AutoIncrCaches != nil {
		autoIncrCaches.Mu.Lock()
		defer autoIncrCaches.Mu.Unlock()
		if autoincrcache, ok := autoIncrCaches.AutoIncrCaches[colName]; ok {
			return autoincrcache.CurNum, nil
		} else {
			// Not cached yet or the cache is ran out.
			// Need new txn for read from the table.
			return getCurrAutoIncrWithTxn(dbName, tblName, colName, e, proc)
		}
	} else {
		return getCurrAutoIncrWithTxn(dbName, tblName, colName, e, proc)
	}
}

func getCurrAutoIncrWithTxn(dbName, tblName, colName string, eg engine.Engine, proc *process.Process) (uint64, error) {
	var err error
	loopCnt := 0
loop:
	loopCnt += 1
	if loopCnt >= 100 {
		return 0, err
	}
	txn, err := newTxn(eg, proc, proc.Ctx)
	if err != nil {
		goto loop
	}
	var autoIncrValue uint64
	if autoIncrValue, err = getTableAutoIncrValue(dbName, colName, eg, txn, proc); err != nil {
		rolllbackTxn(eg, txn, proc.Ctx)
		goto loop
	}
	if err = commitTxn(eg, txn, proc.Ctx); err != nil {
		goto loop
	}
	return autoIncrValue, nil
}

func getTableAutoIncrValue(dbName string, colName string, eg engine.Engine, txn client.TxnOperator, proc *process.Process) (uint64, error) {
	var rds []engine.Reader

	dbHandler, err := eg.Database(proc.Ctx, dbName, txn)
	if err != nil {
		return 0, err
	}
	rel, err := dbHandler.Relation(proc.Ctx, catalog.AutoIncrTableName)
	if err != nil {
		return 0, err
	}
	expr := getRangeExpr(colName)
	ret, err := rel.Ranges(proc.Ctx, expr)
	if err != nil {
		return 0, err
	}
	switch {
	case len(ret) == 0:
		if rds, err = rel.NewReader(proc.Ctx, 1, expr, nil); err != nil {
			return 0, err
		}
	case len(ret) == 1 && len(ret[0]) == 0:
		if rds, err = rel.NewReader(proc.Ctx, 1, expr, nil); err != nil {
			return 0, err
		}
	case len(ret[0]) == 0:
		rds0, err := rel.NewReader(proc.Ctx, 1, expr, nil)
		if err != nil {
			return 0, err
		}
		rds1, err := rel.NewReader(proc.Ctx, 1, expr, ret[1:])
		if err != nil {
			return 0, err
		}
		rds = append(rds, rds0...)
		rds = append(rds, rds1...)
	default:
		rds, _ = rel.NewReader(proc.Ctx, 1, expr, ret)
	}
	for len(rds) > 0 {
		bat, err := rds[0].Read(proc.Ctx, catalog.AutoIncrColumnNames, expr, proc.Mp())
		if err != nil {
			return 0, moerr.NewInvalidInput(proc.Ctx, "can not find the auto col")
		}
		if bat == nil {
			rds[0].Close()
			rds = rds[1:]
			continue
		}

		if len(bat.Vecs) < 4 {
			return 0, moerr.NewInternalError(proc.Ctx, "the mo_increment_columns col num is not four")
		}

		vs2 := vector.MustTCols[uint64](bat.Vecs[2])
		for i := 0; i < bat.Length(); i++ {
			str := bat.Vecs[1].GetString(int64(i))
			if str == colName {
				bat.Clean(proc.Mp())
				return vs2[i], nil
			}
		}
		bat.Clean(proc.Mp())
	}
	return 0, nil
}

// If the table contains auto_increment column, return true and get auto_increment column definition of the table.
// If there is no auto_increment column, return false
func getTableAutoIncrCol(engineDefs []engine.TableDef, tableName string) (bool, *plan.ColDef) {
	if engineDefs != nil {
		for _, def := range engineDefs {
			if attr, ok := def.(*engine.AttributeDef); ok && attr.Attr.AutoIncrement {
				autoIncrCol := &plan.ColDef{
					ColId: attr.Attr.ID,
					Name:  attr.Attr.Name,
					Typ: &plan.Type{
						Id:          int32(attr.Attr.Type.Oid),
						Width:       attr.Attr.Type.Width,
						Scale:       attr.Attr.Type.Scale,
						AutoIncr:    attr.Attr.AutoIncrement,
						Table:       tableName,
						NotNullable: attr.Attr.Default != nil && !attr.Attr.Default.NullAbility,
					},
					Primary:   attr.Attr.Primary,
					Default:   attr.Attr.Default,
					OnUpdate:  attr.Attr.OnUpdate,
					Comment:   attr.Attr.Comment,
					ClusterBy: attr.Attr.ClusterBy,
				}
				return true, autoIncrCol
			}
		}
	}
	return false, nil
}

func newTxn(eg engine.Engine, proc *process.Process, ctx context.Context) (txn client.TxnOperator, err error) {
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

func rolllbackTxn(eg engine.Engine, txn client.TxnOperator, ctx context.Context) error {
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

func commitTxn(eg engine.Engine, txn client.TxnOperator, ctx context.Context) error {
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
		if err2 := rolllbackTxn(eg, txn, ctx); err2 != nil {
			logutil.Errorf("CommitTxn: txn operator rollback failed. error:%v", err2)
		}
		return err
	}
	err := txn.Commit(ctx)
	txn = nil
	return err
}

// build equal expression for auto_increment column
func getRangeExpr(colName string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     10,
					ObjName: "=",
				},
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								Name: catalog.AutoIncrColumnNames[1],
							},
						},
					},
					{
						Expr: &plan.Expr_C{
							C: &plan.Const{
								Value: &plan.Const_Sval{
									Sval: colName,
								},
							},
						},
					},
				},
			},
		},
	}
}

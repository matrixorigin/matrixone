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

package compile

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	y "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *Scope) Delete(c *Compile) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*deletion.Argument)
	var err error
	var dbSource engine.Database
	var rel engine.Relation
	var isTemp bool

	// If the first table (original table) in the deletion context can be truncated,
	// subsequent index tables also need to be truncated
	if arg.DeleteCtxs[0].CanTruncate {
		var affectRows int64
		for _, deleteCtx := range arg.DeleteCtxs {
			if deleteCtx.CanTruncate {
				dbSource, err = c.e.Database(c.ctx, deleteCtx.DbName, c.proc.TxnOperator)
				if err != nil {
					return 0, err
				}

				if rel, err = dbSource.Relation(c.ctx, deleteCtx.TableName); err != nil {
					var e error // avoid contamination of error messages
					dbSource, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
					if e != nil {
						return 0, e
					}
					rel, e = dbSource.Relation(c.ctx, engine.GetTempTableName(deleteCtx.DbName, deleteCtx.TableName))
					if e != nil {
						return 0, err
					}
					isTemp = true
				}

				_, err = rel.Ranges(c.ctx, nil)
				if err != nil {
					return 0, err
				}
				affectRow, err := rel.Rows(s.Proc.Ctx)
				if err != nil {
					return 0, err
				}

				tableID := rel.GetTableID(c.ctx)
				var newId uint64

				if isTemp {
					newId, err = dbSource.Truncate(c.ctx, engine.GetTempTableName(deleteCtx.DbName, deleteCtx.TableName))
					if err != nil {
						return 0, err
					}
					err = colexec.MoveAutoIncrCol(c.e, c.ctx, engine.GetTempTableName(deleteCtx.DbName, deleteCtx.TableName), dbSource, c.proc, tableID, newId, defines.TEMPORARY_DBNAME)
				} else {
					newId, err = dbSource.Truncate(c.ctx, deleteCtx.TableName)
					if err != nil {
						return 0, err
					}
					err = colexec.MoveAutoIncrCol(c.e, c.ctx, deleteCtx.TableName, dbSource, c.proc, tableID, newId, deleteCtx.DbName)
				}

				if err != nil {
					return 0, err
				}

				if deleteCtx.IsIndexTableDelete {
					continue
				} else {
					affectRows += affectRow
				}

			}
		}
		return uint64(affectRows), nil
	}

	if err := s.MergeRun(c); err != nil {
		return 0, err
	}
	return arg.AffectedRows, nil
}

func (s *Scope) Insert(c *Compile) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*insert.Argument)
	if err := s.MergeRun(c); err != nil {
		return 0, err
	}
	return arg.Affected, nil
}

func (s *Scope) Update(c *Compile) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*update.Argument)
	if err := s.MergeRun(c); err != nil {
		return 0, err
	}
	return arg.AffectedRows, nil
}

func (s *Scope) InsertValues(c *Compile, stmt *tree.Insert) (uint64, error) {

	var err error
	var dbSource engine.Database
	var relation engine.Relation
	var isTemp bool
	p := s.Plan.GetIns()

	ctx := c.ctx
	clusterTable := p.GetClusterTable()
	if clusterTable.GetIsClusterTable() {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}
	dbSource, err = c.e.Database(c.ctx, p.DbName, c.proc.TxnOperator)
	if err != nil {
		return 0, err
	}
	relation, err = dbSource.Relation(ctx, p.TblName)
	if err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
		if e != nil {
			return 0, e
		}
		relation, e = dbSource.Relation(c.ctx, engine.GetTempTableName(p.DbName, p.TblName))
		if e != nil {
			return 0, err
		}
		isTemp = true
	}

	bat := makeInsertBatch(p)
	defer bat.Clean(c.proc.Mp())

	if p.OtherCols != nil {
		p.ExplicitCols = append(p.ExplicitCols, p.OtherCols...)
	}

	insertRows := stmt.Rows.Select.(*tree.ValuesClause).Rows
	if err = fillBatch(ctx, bat, p, insertRows, c.proc); err != nil {
		return 0, err
	}

	if clusterTable.GetIsClusterTable() {
		columns := p.GetColumns()
		accountIdColumnDef := p.ExplicitCols[clusterTable.GetColumnIndexOfAccountId()]
		accountIdRows := columns[clusterTable.GetColumnIndexOfAccountId()].GetColumn()
		accountIdExpr := accountIdRows[0]
		accountIdConst := accountIdExpr.GetC()
		accountIdVec := bat.Vecs[clusterTable.GetColumnIndexOfAccountId()]
		tmpBat := batch.NewWithSize(0)
		tmpBat.Zs = []int64{1}
		//save auto_increment column if necessary
		savedAutoIncrVectors := make([]*vector.Vector, 0)
		defer func() {
			for _, vec := range savedAutoIncrVectors {
				vector.Clean(vec, c.proc.Mp())
			}
		}()
		for i, colDef := range p.GetExplicitCols() {
			if colDef.GetTyp().GetAutoIncr() {
				vec2, err := vector.Dup(bat.Vecs[i], c.proc.Mp())
				if err != nil {
					return 0, err
				}
				savedAutoIncrVectors = append(savedAutoIncrVectors, vec2)
			}
		}
		for idx, accountId := range clusterTable.GetAccountIDs() {
			//update accountId in the accountIdExpr
			accountIdConst.Value = &plan.Const_U32Val{U32Val: accountId}
			//clean vector before fill it
			vector.Clean(accountIdVec, c.proc.Mp())
			//the j th row
			for j := 0; j < len(accountIdRows); j++ {
				err = fillRow(ctx, tmpBat,
					accountIdColumnDef,
					insertRows,
					int(clusterTable.GetColumnIndexOfAccountId()), j,
					accountIdExpr,
					accountIdVec,
					c.proc)
				if err != nil {
					return 0, err
				}
			}

			if idx != 0 { //refill the auto_increment column vector
				j := 0
				vecLen := vector.Length(bat.Vecs[0])
				for colIdx, colDef := range p.GetExplicitCols() {
					if colDef.GetTyp().GetAutoIncr() {
						targetVec := bat.Vecs[colIdx]
						vector.Clean(targetVec, c.proc.Mp())
						for k := int64(0); k < int64(vecLen); k++ {
							err = vector.UnionOne(targetVec, savedAutoIncrVectors[j], k, c.proc.Mp())
							if err != nil {
								return 0, err
							}
						}
						j++
					}
				}
			}

			if err = writeBatch(ctx, dbSource, relation, c, p, bat, isTemp); err != nil {
				return 0, err
			}
		}
		//the count of insert rows x the count of accounts
		return uint64(len(p.Columns[0].Column)) * uint64(len(clusterTable.GetAccountIDs())), nil
	} else {
		if err = writeBatch(ctx, dbSource, relation, c, p, bat, isTemp); err != nil {
			return 0, err
		}
		return uint64(len(p.Columns[0].Column)), nil
	}
}

// writeBatch saves the data into the storage
// and updates the auto increment table, index table.
func writeBatch(ctx context.Context,
	dbSource engine.Database,
	relation engine.Relation,
	c *Compile,
	p *plan.InsertValues,
	bat *batch.Batch,
	isTemp bool) error {
	var err error
	var oldDbName string

	/**
	Null value check:
	There are two cases to validate for not null
	1. Primary key
	2. Not null

	For auto_increment, follow the Mysql way instead of pg. That is, null values are allowed to be inserted.
	Assume that there is no case like 'create table t (a int auto_increment not null)', if exists, ignore not null constraint
	*/
	for i := range bat.Vecs {
		// check for case 1 and case 2
		if (p.ExplicitCols[i].Primary && !p.ExplicitCols[i].Typ.AutoIncr) || (p.ExplicitCols[i].Default != nil && !p.ExplicitCols[i].Default.NullAbility && !p.ExplicitCols[i].Typ.AutoIncr) {
			if nulls.Any(bat.Vecs[i].Nsp) {
				return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", p.ExplicitCols[i].Name))
			}
		}
	}
	batch.Reorder(bat, p.OrderAttrs)

	if isTemp {
		oldDbName = p.DbName
		p.TblName = engine.GetTempTableName(p.DbName, p.TblName)
		p.DbName = defines.TEMPORARY_DBNAME
	}

	//update the auto increment table
	if err = colexec.UpdateInsertValueBatch(c.e, ctx, c.proc, p, bat, p.DbName, p.TblName); err != nil {
		return err
	}

	//complement composite primary key
	if p.CompositePkey != nil {
		err := util.FillCompositePKeyBatch(bat, p.CompositePkey, c.proc)
		if err != nil {
			names := util.SplitCompositePrimaryKeyColumnName(p.CompositePkey.Name)
			for i := range bat.Vecs {
				for _, name := range names {
					if p.OrderAttrs[i] == name {
						if nulls.Any(bat.Vecs[i].Nsp) {
							return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", p.OrderAttrs[i]))
						}
					}
				}
			}
		}
	} else if p.Cb != nil && util.JudgeIsCompositeClusterByColumn(p.Cb.Name) {
		util.FillCompositeClusterByBatch(bat, p.Cb.Name, c.proc)
	}

	//update unique index table
	if p.UniqueIndexDef != nil {
		primaryKeyName := update.GetTablePriKeyName(p.ExplicitCols, p.CompositePkey)
		for i := range p.UniqueIndexDef.IndexNames {
			var indexRelation engine.Relation
			if p.UniqueIndexDef.TableExists[i] {
				if isTemp {
					indexRelation, err = dbSource.Relation(ctx, engine.GetTempTableName(oldDbName, p.UniqueIndexDef.TableNames[i]))
				} else {
					indexRelation, err = dbSource.Relation(ctx, p.UniqueIndexDef.TableNames[i])
				}
				if err != nil {
					return err
				}
				indexBatch, rowNum := util.BuildUniqueKeyBatch(bat.Vecs, bat.Attrs, p.UniqueIndexDef.Fields[i].Parts, primaryKeyName, c.proc)
				if rowNum != 0 {
					indexBatch.SetZs(rowNum, c.proc.Mp())
					if err = indexRelation.Write(ctx, indexBatch); err != nil {
						return err
					}
				}
				indexBatch.Clean(c.proc.Mp())
			}
		}
	}

	return relation.Write(ctx, bat)
}

/*
fillRow evaluates the expression and put the result into the targetVec.
tmpBat: store temporal vector
colDef: the definition meta of the column
rows: data rows of the insert
colIdx,rowIdx: which column, which row
expr: the expression to be evaluated at the position (colIdx,rowIdx)
targetVec: the destination where the evaluated result of expr saved into
*/
func fillRow(ctx context.Context,
	tmpBat *batch.Batch,
	colDef *plan.ColDef,
	rows []tree.Exprs, colIdx, rowIdx int,
	expr *plan.Expr,
	targetVec *vector.Vector,
	proc *process.Process) error {
	vec, err := colexec.EvalExpr(tmpBat, proc, expr)
	if err != nil {
		return y.MakeInsertError(ctx, targetVec.Typ.Oid, colDef, rows, colIdx, rowIdx, err)
	}
	if vec.Size() == 0 {
		vec = vec.ConstExpand(false, proc.Mp())
	}
	if err := vector.UnionOne(targetVec, vec, 0, proc.Mp()); err != nil {
		vec.Free(proc.Mp())
		return err
	}
	vec.Free(proc.Mp())
	return err
}

// XXX: is this just fill batch with first vec.Col[0]?
func fillBatch(ctx context.Context, bat *batch.Batch, p *plan.InsertValues, rows []tree.Exprs, proc *process.Process) error {
	tmpBat := batch.NewWithSize(0)
	tmpBat.Zs = []int64{1}

	clusterTable := p.GetClusterTable()
	//the i th column
	for i, v := range bat.Vecs {
		//skip fill vector of the account_id in the cluster table here
		if clusterTable.GetIsClusterTable() && int32(i) == clusterTable.GetColumnIndexOfAccountId() {
			continue
		}
		//the j th row
		for j, expr := range p.Columns[i].Column {
			//evaluate the expr at position (i,j) and
			//save into
			err := fillRow(ctx, tmpBat, p.ExplicitCols[i],
				rows, i, j,
				expr,
				v,
				proc)
			if err != nil {
				return err
			}
		}
	}
	bat.SetZs(len(rows), proc.Mp())
	return nil
}

func makeInsertBatch(p *plan.InsertValues) *batch.Batch {
	attrs := make([]string, 0, len(p.OrderAttrs))

	for _, col := range p.ExplicitCols {
		attrs = append(attrs, col.Name)
	}
	for _, col := range p.OtherCols {
		attrs = append(attrs, col.Name)
	}

	bat := batch.NewWithSize(len(attrs))
	bat.SetAttributes(attrs)
	idx := 0
	for _, col := range p.ExplicitCols {
		bat.Vecs[idx] = vector.New(types.Type{Oid: types.T(col.Typ.GetId()), Scale: col.Typ.Scale, Width: col.Typ.Width})
		idx++
	}
	for _, col := range p.OtherCols {
		bat.Vecs[idx] = vector.New(types.Type{Oid: types.T(col.Typ.GetId()), Scale: col.Typ.Scale, Width: col.Typ.Width})
		idx++
	}

	return bat
}

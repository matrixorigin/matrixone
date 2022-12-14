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
	"fmt"

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

	// If the first table (original table) in the deletion context can be truncated,
	// subsequent index tables also need to be truncated
	if arg.DeleteCtxs[0].CanTruncate {
		var affectRows int64
		for _, deleteCtx := range arg.DeleteCtxs {
			if deleteCtx.CanTruncate {
				dbSource, err := c.e.Database(c.ctx, deleteCtx.DbName, c.proc.TxnOperator)
				if err != nil {
					return 0, err
				}

				var rel engine.Relation
				if rel, err = dbSource.Relation(c.ctx, deleteCtx.TableName); err != nil {
					return 0, err
				}
				tableID := rel.GetTableID(c.ctx)

				err = dbSource.Truncate(c.ctx, deleteCtx.TableName)
				if err != nil {
					return 0, err
				}

				err = colexec.MoveAutoIncrCol(c.e, c.ctx, deleteCtx.TableName, dbSource, c.proc, tableID, deleteCtx.DbName)
				if err != nil {
					return 0, err
				}

				if deleteCtx.IsIndexTableDelete {
					continue
				} else {
					_, affectRow, err := rel.Stats(s.Proc.Ctx)
					if err != nil {
						return uint64(affectRow), err
					}
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
	p := s.Plan.GetIns()

	dbSource, err := c.e.Database(c.ctx, p.DbName, c.proc.TxnOperator)
	if err != nil {
		return 0, err
	}
	relation, err := dbSource.Relation(c.ctx, p.TblName)
	if err != nil {
		return 0, err
	}

	bat := makeInsertBatch(p)
	defer bat.Clean(c.proc.Mp())

	if p.OtherCols != nil {
		p.ExplicitCols = append(p.ExplicitCols, p.OtherCols...)
	}

	if err := fillBatch(bat, p, stmt.Rows.Select.(*tree.ValuesClause).Rows, c.proc); err != nil {
		return 0, err
	}
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
				return 0, moerr.NewConstraintViolation(c.ctx, fmt.Sprintf("Column '%s' cannot be null", p.ExplicitCols[i].Name))
			}
		}
	}
	batch.Reorder(bat, p.OrderAttrs)

	if err = colexec.UpdateInsertValueBatch(c.e, c.ctx, c.proc, p, bat, p.DbName, p.TblName); err != nil {
		return 0, err
	}
	if p.CompositePkey != nil {
		err := util.FillCompositePKeyBatch(bat, p.CompositePkey, c.proc)
		if err != nil {
			names := util.SplitCompositePrimaryKeyColumnName(p.CompositePkey.Name)
			for i := range bat.Vecs {
				for _, name := range names {
					if p.OrderAttrs[i] == name {
						if nulls.Any(bat.Vecs[i].Nsp) {
							return 0, moerr.NewConstraintViolation(c.ctx, fmt.Sprintf("Column '%s' cannot be null", p.OrderAttrs[i]))
						}
					}
				}
			}
		}
	}
	if p.UniqueIndexDef != nil {
		primaryKeyName := ""
		for _, col := range p.ExplicitCols {
			if col.Primary {
				primaryKeyName = col.Name
			}
		}
		if p.CompositePkey != nil {
			primaryKeyName = p.CompositePkey.Name
		}
		for i := range p.UniqueIndexDef.IndexNames {
			if p.UniqueIndexDef.TableExists[i] {
				indexRelation, err := dbSource.Relation(c.ctx, p.UniqueIndexDef.TableNames[i])
				if err != nil {
					return 0, err
				}
				indexBatch, rowNum := util.BuildUniqueKeyBatch(bat.Vecs, bat.Attrs, p.UniqueIndexDef.Fields[i], primaryKeyName, c.proc)
				if rowNum != 0 {
					if err := indexRelation.Write(c.ctx, indexBatch); err != nil {
						return 0, err
					}
				}
				indexBatch.Clean(c.proc.Mp())
			}
		}
	}

	if err := relation.Write(c.ctx, bat); err != nil {
		return 0, err
	}

	return uint64(len(p.Columns[0].Column)), nil
}

// XXX: is this just fill batch with first vec.Col[0]?
func fillBatch(bat *batch.Batch, p *plan.InsertValues, rows []tree.Exprs, proc *process.Process) error {
	tmpBat := batch.NewWithSize(0)
	tmpBat.Zs = []int64{1}

	for i, v := range bat.Vecs {
		for j, expr := range p.Columns[i].Column {
			vec, err := colexec.EvalExpr(tmpBat, proc, expr)
			if err != nil {
				return y.MakeInsertError(proc.Ctx, v.Typ.Oid, p.ExplicitCols[i], rows, i, j, err)
			}
			if vec.Size() == 0 {
				vec = vec.ConstExpand(proc.Mp())
			}
			if err := vector.UnionOne(v, vec, 0, proc.Mp()); err != nil {
				vec.Free(proc.Mp())
				return err
			}
			vec.Free(proc.Mp())
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

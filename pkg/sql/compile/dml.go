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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *Scope) Delete(c *Compile) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*deletion.Argument)

	if arg.DeleteCtxs[0].CanTruncate {
		return arg.DeleteCtxs[0].TableSource.Truncate(c.ctx)
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
	snapshot := engine.Snapshot(c.proc.Snapshot)
	p := s.Plan.GetIns()

	dbSource, err := c.e.Database(c.ctx, p.DbName, snapshot)
	if err != nil {
		return 0, err
	}
	relation, err := dbSource.Relation(c.ctx, p.TblName)
	if err != nil {
		return 0, err
	}

	bat := makeInsertBatch(p)

	if p.OtherCols != nil {
		p.ExplicitCols = append(p.ExplicitCols, p.OtherCols...)
	}

	if err := fillBatch(bat, p, stmt.Rows.Select.(*tree.ValuesClause).Rows, c.proc); err != nil {
		return 0, err
	}
	batch.Reorder(bat, p.OrderAttrs)
	if err := relation.Write(c.ctx, bat); err != nil {
		return 0, err
	}

	return uint64(len(p.Columns[0].Column)), nil
}

func fillBatch(bat *batch.Batch, p *plan.InsertValues, rows []tree.Exprs, proc *process.Process) error {
	rowCount := len(p.Columns[0].Column)

	tmpBat := batch.NewWithSize(0)
	tmpBat.Zs = []int64{1}

	for i, v := range bat.Vecs {
		switch v.Typ.Oid {
		case types.T_json:
			vs := make([][]byte, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.(*types.Bytes).Data
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_bool:
			vs := make([]bool, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]bool)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_int8:
			vs := make([]int8, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]int8)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_int16:
			vs := make([]int16, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]int16)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_int32:
			vs := make([]int32, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]int32)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_int64:
			vs := make([]int64, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]int64)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_uint8:
			vs := make([]uint8, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]uint8)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_uint16:
			vs := make([]uint16, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]uint16)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_uint32:
			vs := make([]uint32, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]uint32)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_uint64:
			vs := make([]uint64, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]uint64)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_float32:
			vs := make([]float32, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]float32)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_float64:
			vs := make([]float64, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]float64)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_char, types.T_varchar, types.T_blob:
			vs := make([][]byte, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.(*types.Bytes).Data
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_date:
			vs := make([]types.Date, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Date)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_datetime:
			vs := make([]types.Datetime, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Datetime)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_timestamp:
			vs := make([]types.Timestamp, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Timestamp)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_decimal64:
			vs := make([]types.Decimal64, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Decimal64)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		case types.T_decimal128:
			vs := make([]types.Decimal128, rowCount)
			{
				for j, expr := range p.Columns[i].Column {
					vec, err := colexec.EvalExpr(tmpBat, proc, expr)
					if err != nil {
						return y.MakeInsertError(v.Typ.Oid, p.ExplicitCols[i], rows, i, j)
					}
					if nulls.Any(vec.Nsp) {
						nulls.Add(v.Nsp, uint64(j))
					} else {
						vs[j] = vec.Col.([]types.Decimal128)[0]
					}
				}
			}
			if err := vector.Append(v, vs); err != nil {
				return err
			}
		default:
			return fmt.Errorf("data truncation: type of '%v' doesn't implement", v.Typ)
		}
	}
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

	bat := batch.New(true, attrs)
	idx := 0
	for _, col := range p.ExplicitCols {
		bat.Vecs[idx] = vector.New(types.Type{Oid: types.T(col.Typ.GetId())})
		idx++
	}
	for _, col := range p.OtherCols {
		bat.Vecs[idx] = vector.New(types.Type{Oid: types.T(col.Typ.GetId())})
		idx++
	}

	return bat
}

// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func newBasePKFilter(
	expr *plan.Expr,
	tblDef *plan.TableDef,
	proc *process.Process,
) (filter basePKFilter, err error) {
	if expr == nil {
		return
	}

	defer func() {
		if tblDef.Pkey.CompPkeyCol != nil {
			filter.oid = types.T_varchar
		}
	}()

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch name := exprImpl.F.Func.ObjName; name {
		case "and":
			var filters []basePKFilter
			for idx := range exprImpl.F.Args {
				ff, err := newBasePKFilter(exprImpl.F.Args[idx], tblDef, proc)
				if err != nil {
					return basePKFilter{}, err
				}
				if ff.valid {
					filters = append(filters, ff)
				}
			}

			if len(filters) == 0 {
				return basePKFilter{}, nil
			}

			for idx := 0; idx < len(filters)-1; {
				f1 := &filters[idx]
				f2 := &filters[idx+1]
				ff, err := mergeFilters(f1, f2, function.AND, proc)
				if err != nil {
					return basePKFilter{}, err
				}

				if !ff.valid {
					return basePKFilter{}, nil
				}

				idx++
				filters[idx] = ff
			}

			for idx := 0; idx < len(filters)-1; idx++ {
				if filters[idx].vec != nil {
					filters[idx].vec.Free(proc.Mp())
				}
			}

			ret := filters[len(filters)-1]
			return ret, nil

		case "or":
			var filters []basePKFilter
			for idx := range exprImpl.F.Args {
				ff, err := newBasePKFilter(exprImpl.F.Args[idx], tblDef, proc)
				if err != nil {
					return basePKFilter{}, err
				}
				if !ff.valid {
					return basePKFilter{}, err
				}

				filters = append(filters, ff)
			}

			if len(filters) == 0 {
				return basePKFilter{}, nil
			}

			for idx := 0; idx < len(filters)-1; {
				f1 := &filters[idx]
				f2 := &filters[idx+1]
				ff, err := mergeFilters(f1, f2, function.OR, proc)
				if err != nil {
					return basePKFilter{}, nil
				}

				if !ff.valid {
					return basePKFilter{}, nil
				}

				idx++
				filters[idx] = ff
			}

			for idx := 0; idx < len(filters)-1; idx++ {
				if filters[idx].vec != nil {
					filters[idx].vec.Free(proc.Mp())
				}
			}

			ret := filters[len(filters)-1]
			return ret, nil

		case ">=":
			//a >= ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.GREAT_EQUAL
			filter.lb = vals[0]
			filter.oid = oid

		case "<=":
			//a <= ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.LESS_EQUAL
			filter.lb = vals[0]
			filter.oid = oid

		case ">":
			//a > ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.GREAT_THAN
			filter.lb = vals[0]
			filter.oid = oid

		case "<":
			//a < ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.LESS_THAN
			filter.lb = vals[0]
			filter.oid = oid

		case "=":
			// a = ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.EQUAL
			filter.lb = vals[0]
			filter.oid = oid

		case "prefix_eq":
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.PREFIX_EQ
			filter.lb = vals[0]
			filter.oid = oid

		case "in":
			ok, oid, vals := evalValue(exprImpl, tblDef, true, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.IN
			{
				vec := vector.NewVec(types.T_any.ToType())
				if err = vec.UnmarshalBinary(vals[0]); err != nil {
					return basePKFilter{}, err
				}
				filter.vec = vec
			}
			filter.oid = oid

		case "prefix_in":
			ok, oid, vals := evalValue(exprImpl, tblDef, true, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.PREFIX_IN
			{
				vec := vector.NewVec(types.T_any.ToType())
				if err = vec.UnmarshalBinary(vals[0]); err != nil {
					return basePKFilter{}, err
				}
				filter.vec = vec
			}
			filter.oid = oid

		case "between":
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.BETWEEN
			filter.lb = vals[0]
			filter.ub = vals[1]
			filter.oid = oid

		case "prefix_between":
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.valid = true
			filter.op = function.PREFIX_BETWEEN
			filter.lb = vals[0]
			filter.ub = vals[1]
			filter.oid = oid

		default:
			//panic(name)
		}
	default:
		//panic(plan2.FormatExpr(expr))
	}

	return
}

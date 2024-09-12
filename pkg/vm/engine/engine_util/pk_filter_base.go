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

package engine_util

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	RangeLeftOpen = iota + function.FUNCTION_END_NUMBER + 1
	RangeRightOpen
	RangeBothOpen
)

type BasePKFilter struct {
	Valid bool
	Op    int
	LB    []byte
	UB    []byte
	Vec   *vector.Vector
	Oid   types.T
}

func (b *BasePKFilter) String() string {
	name := map[int]string{
		function.LESS_EQUAL:     "less_eq",
		function.LESS_THAN:      "less_than",
		function.GREAT_THAN:     "great_than",
		function.GREAT_EQUAL:    "great_eq",
		RangeLeftOpen:           "range_left_open",
		RangeRightOpen:          "range_right_open",
		RangeBothOpen:           "range_both_open",
		function.EQUAL:          "equal",
		function.IN:             "in",
		function.BETWEEN:        "between",
		function.PREFIX_EQ:      "prefix_in",
		function.PREFIX_IN:      "prefix_in",
		function.PREFIX_BETWEEN: "prefix_between",
	}
	return fmt.Sprintf("valid = %v, op = %s, lb = %v, ub = %v, vec.type=%T, oid = %s",
		b.Valid, name[b.Op], b.LB, b.UB, b.Vec, b.Oid.String())
}

func ConstructBasePKFilter(
	expr *plan.Expr,
	tblDef *plan.TableDef,
	proc *process.Process,
) (filter BasePKFilter, err error) {
	if expr == nil {
		return
	}

	defer func() {
		if tblDef.Pkey.CompPkeyCol != nil {
			filter.Oid = types.T_varchar
		}
	}()

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch name := exprImpl.F.Func.ObjName; name {
		case "and":
			var filters []BasePKFilter
			for idx := range exprImpl.F.Args {
				ff, err := ConstructBasePKFilter(exprImpl.F.Args[idx], tblDef, proc)
				if err != nil {
					return BasePKFilter{}, err
				}
				if ff.Valid {
					filters = append(filters, ff)
				}
			}

			if len(filters) == 0 {
				return BasePKFilter{}, nil
			}

			for idx := 0; idx < len(filters)-1; {
				f1 := &filters[idx]
				f2 := &filters[idx+1]
				ff, err := mergeFilters(f1, f2, function.AND, proc)
				if err != nil {
					return BasePKFilter{}, err
				}

				if !ff.Valid {
					return BasePKFilter{}, nil
				}

				idx++
				filters[idx] = ff
			}

			for idx := 0; idx < len(filters)-1; idx++ {
				if filters[idx].Vec != nil {
					filters[idx].Vec.Free(proc.Mp())
				}
			}

			ret := filters[len(filters)-1]
			return ret, nil

		case "or":
			var filters []BasePKFilter
			for idx := range exprImpl.F.Args {
				ff, err := ConstructBasePKFilter(exprImpl.F.Args[idx], tblDef, proc)
				if err != nil {
					return BasePKFilter{}, err
				}
				if !ff.Valid {
					return BasePKFilter{}, err
				}

				filters = append(filters, ff)
			}

			if len(filters) == 0 {
				return BasePKFilter{}, nil
			}

			for idx := 0; idx < len(filters)-1; {
				f1 := &filters[idx]
				f2 := &filters[idx+1]
				ff, err := mergeFilters(f1, f2, function.OR, proc)
				if err != nil {
					return BasePKFilter{}, nil
				}

				if !ff.Valid {
					return BasePKFilter{}, nil
				}

				idx++
				filters[idx] = ff
			}

			for idx := 0; idx < len(filters)-1; idx++ {
				if filters[idx].Vec != nil {
					filters[idx].Vec.Free(proc.Mp())
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
			filter.Valid = true
			filter.Op = function.GREAT_EQUAL
			filter.LB = vals[0]
			filter.Oid = oid

		case "<=":
			//a <= ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.LESS_EQUAL
			filter.LB = vals[0]
			filter.Oid = oid

		case ">":
			//a > ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.GREAT_THAN
			filter.LB = vals[0]
			filter.Oid = oid

		case "<":
			//a < ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.LESS_THAN
			filter.LB = vals[0]
			filter.Oid = oid

		case "=":
			// a = ?
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.EQUAL
			filter.LB = vals[0]
			filter.Oid = oid

		case "prefix_eq":
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.PREFIX_EQ
			filter.LB = vals[0]
			filter.Oid = oid

		case "in":
			ok, oid, vals := evalValue(exprImpl, tblDef, true, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.IN
			{
				vec := vector.NewVec(types.T_any.ToType())
				if err = vec.UnmarshalBinary(vals[0]); err != nil {
					return BasePKFilter{}, err
				}
				filter.Vec = vec
			}
			filter.Oid = oid

		case "prefix_in":
			ok, oid, vals := evalValue(exprImpl, tblDef, true, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.PREFIX_IN
			{
				vec := vector.NewVec(types.T_any.ToType())
				if err = vec.UnmarshalBinary(vals[0]); err != nil {
					return BasePKFilter{}, err
				}
				filter.Vec = vec
			}
			filter.Oid = oid

		case "between":
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.BETWEEN
			filter.LB = vals[0]
			filter.UB = vals[1]
			filter.Oid = oid

		case "prefix_between":
			ok, oid, vals := evalValue(exprImpl, tblDef, false, tblDef.Pkey.PkeyColName, proc)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.PREFIX_BETWEEN
			filter.LB = vals[0]
			filter.UB = vals[1]
			filter.Oid = oid

		default:
			//panic(name)
		}
	default:
		//panic(plan2.FormatExpr(expr))
	}

	return
}

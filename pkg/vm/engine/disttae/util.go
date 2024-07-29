// Copyright 2022 Matrix Origin
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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func compPkCol(colName string, pkName string) bool {
	dotIdx := strings.Index(colName, ".")
	colName = colName[dotIdx+1:]
	return colName == pkName
}

func getColDefByName(name string, tableDef *plan.TableDef) *plan.ColDef {
	idx := strings.Index(name, ".")
	var pos int32
	if idx >= 0 {
		subName := name[idx+1:]
		pos = tableDef.Name2ColIndex[subName]
	} else {
		pos = tableDef.Name2ColIndex[name]
	}
	return tableDef.Cols[pos]
}

const (
	rangeLeftOpen = iota + function.FUNCTION_END_NUMBER + 1
	rangeRightOpen
	rangeBothOpen

	//emptySet
)

type basePKFilter struct {
	valid bool
	op    int
	lb    []byte
	ub    []byte
	vec   any
	oid   types.T
}

func (b *basePKFilter) String() string {
	name := map[int]string{
		function.LESS_EQUAL:     "less_eq",
		function.LESS_THAN:      "less_than",
		function.GREAT_THAN:     "great_than",
		function.GREAT_EQUAL:    "great_eq",
		rangeLeftOpen:           "range_left_open",
		rangeRightOpen:          "range_right_open",
		rangeBothOpen:           "range_both_open",
		function.EQUAL:          "equal",
		function.IN:             "in",
		function.BETWEEN:        "between",
		function.PREFIX_EQ:      "prefix_in",
		function.PREFIX_IN:      "prefix_in",
		function.PREFIX_BETWEEN: "prefix_between",
	}
	return fmt.Sprintf("valid = %v, op = %s, lb = %v, ub = %v, vec.type=%T, oid = %s",
		b.valid, name[b.op], b.lb, b.ub, b.vec, b.oid.String())
}

func evalValue(exprImpl *plan.Expr_F, tblDef *plan.TableDef, isVec bool, pkName string, proc *process.Process) (
	ok bool, oid types.T, vals [][]byte) {
	var val []byte
	var col *plan.Expr_Col

	if !isVec {
		col, vals, ok = mustColConstValueFromBinaryFuncExpr(exprImpl, tblDef, proc)
	} else {
		col, val, ok = mustColVecValueFromBinaryFuncExpr(proc, exprImpl)
	}

	if !ok {
		return false, 0, nil
	}
	if !compPkCol(col.Col.Name, pkName) {
		return false, 0, nil
	}

	var colPos int32
	if col.Col.Name == "" {
		colPos = col.Col.ColPos
		logutil.Warnf("colExpr.Col.Name is empty")
	} else {
		idx := strings.Index(col.Col.Name, ".")
		if idx == -1 {
			colPos = tblDef.Name2ColIndex[col.Col.Name]
		} else {
			colPos = tblDef.Name2ColIndex[col.Col.Name[idx+1:]]
		}
	}

	if isVec {
		return true, types.T(tblDef.Cols[colPos].Typ.Id), [][]byte{val}
	}
	return true, types.T(tblDef.Cols[colPos].Typ.Id), vals
}

func mergeBaseFilterInKind(left, right basePKFilter, isOR bool, proc *process.Process) (ret basePKFilter) {
	var ok bool
	var va, vb *vector.Vector
	ret.vec = vector.NewVec(left.oid.ToType())

	if va, ok = left.vec.(*vector.Vector); !ok {
		va = vector.NewVec(types.T_any.ToType())
		va.UnmarshalBinary(left.vec.([]byte))
	}

	if vb, ok = right.vec.(*vector.Vector); !ok {
		vb = vector.NewVec(types.T_any.ToType())
		vb.UnmarshalBinary(right.vec.([]byte))
	}

	switch va.GetType().Oid {
	case types.T_int8:
		a := vector.MustFixedCol[int8](va)
		b := vector.MustFixedCol[int8](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y int8) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y int8) int { return int(x - y) })
		}
	case types.T_int16:
		a := vector.MustFixedCol[int16](va)
		b := vector.MustFixedCol[int16](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y int16) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y int16) int { return int(x - y) })
		}
	case types.T_int32:
		a := vector.MustFixedCol[int32](va)
		b := vector.MustFixedCol[int32](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y int32) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y int32) int { return int(x - y) })
		}
	case types.T_int64:
		a := vector.MustFixedCol[int64](va)
		b := vector.MustFixedCol[int64](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y int64) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y int64) int { return int(x - y) })
		}
	case types.T_float32:
		a := vector.MustFixedCol[float32](va)
		b := vector.MustFixedCol[float32](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y float32) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y float32) int { return int(x - y) })
		}
	case types.T_float64:
		a := vector.MustFixedCol[float64](va)
		b := vector.MustFixedCol[float64](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y float64) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y float64) int { return int(x - y) })
		}
	case types.T_uint8:
		a := vector.MustFixedCol[uint8](va)
		b := vector.MustFixedCol[uint8](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y uint8) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y uint8) int { return int(x - y) })
		}
	case types.T_uint16:
		a := vector.MustFixedCol[uint16](va)
		b := vector.MustFixedCol[uint16](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y uint16) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y uint16) int { return int(x - y) })
		}
	case types.T_uint32:
		a := vector.MustFixedCol[uint32](va)
		b := vector.MustFixedCol[uint32](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y uint32) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y uint32) int { return int(x - y) })
		}
	case types.T_uint64:
		a := vector.MustFixedCol[uint64](va)
		b := vector.MustFixedCol[uint64](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y uint64) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y uint64) int { return int(x - y) })
		}
	case types.T_date:
		a := vector.MustFixedCol[types.Date](va)
		b := vector.MustFixedCol[types.Date](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Date) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Date) int { return int(x - y) })
		}
	case types.T_time:
		a := vector.MustFixedCol[types.Time](va)
		b := vector.MustFixedCol[types.Time](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Time) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Time) int { return int(x - y) })
		}
	case types.T_datetime:
		a := vector.MustFixedCol[types.Datetime](va)
		b := vector.MustFixedCol[types.Datetime](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Datetime) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Datetime) int { return int(x - y) })
		}
	case types.T_timestamp:
		a := vector.MustFixedCol[types.Timestamp](va)
		b := vector.MustFixedCol[types.Timestamp](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Timestamp) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Timestamp) int { return int(x - y) })
		}
	case types.T_decimal64:
		a := vector.MustFixedCol[types.Decimal64](va)
		b := vector.MustFixedCol[types.Decimal64](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Decimal64) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Decimal64) int { return int(x - y) })
		}
	case types.T_decimal128:
		a := vector.MustFixedCol[types.Decimal128](va)
		b := vector.MustFixedCol[types.Decimal128](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Decimal128) int { return types.CompareDecimal128(x, y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Decimal128) int { return types.CompareDecimal128(x, y) })
		}

	case types.T_varchar, types.T_char, types.T_json, types.T_binary, types.T_text, types.T_datalink:
		if isOR {
			vector.Union2VectorValen(va, vb, ret.vec.(*vector.Vector), proc.Mp())
		} else {
			vector.Intersection2VectorVarlen(va, vb, ret.vec.(*vector.Vector), proc.Mp())
		}

	case types.T_enum:
		a := vector.MustFixedCol[types.Enum](va)
		b := vector.MustFixedCol[types.Enum](vb)
		if isOR {
			vector.Union2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Enum) int { return int(x - y) })
		} else {
			vector.Intersection2VectorOrdered(a, b, ret.vec.(*vector.Vector), proc.Mp(), func(x, y types.Enum) int { return int(x - y) })
		}
	default:
		return basePKFilter{}
		//panic(basePKFilter.oid.String())
	}

	ret.valid = true
	ret.op = left.op
	ret.oid = left.oid

	return ret
}

// left op in (">", ">=", "=", "<", "<="), right op in (">", ">=", "=", "<", "<=")
// left op AND right op
// left op OR right op
func mergeFilters(left, right basePKFilter, connector int, proc *process.Process) (finalFilter basePKFilter) {
	defer func() {
		finalFilter.oid = left.oid
	}()

	switch connector {
	case function.AND:
		switch left.op {
		case function.IN:
			switch right.op {
			case function.IN:
				// a in (...) and a in (...) and a in (...) and ...
				finalFilter = mergeBaseFilterInKind(left, right, false, proc)
			}

		case function.GREAT_EQUAL:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a >= x and a >= y --> a >= max(x, y)
				// a >= x and a > y  --> a > y or a >= x
				if bytes.Compare(left.lb, right.lb) >= 0 { // x >= y
					return left
				} else { // x < y
					return right
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a >= x and a <= y --> [x, y]
				// a >= x and a < y  --> [x, y)
				finalFilter.lb = left.lb
				finalFilter.ub = right.lb
				if right.op == function.LESS_THAN {
					finalFilter.op = rangeRightOpen
				} else {
					finalFilter.op = function.BETWEEN
				}
				finalFilter.valid = true

			case function.EQUAL:
				// a >= x and a = y --> a = y if y >= x
				if bytes.Compare(left.lb, right.lb) <= 0 {
					finalFilter.op = function.EQUAL
					finalFilter.lb = right.lb
					finalFilter.valid = true
				}
			}

		case function.GREAT_THAN:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a > x and a >= y
				// a > x and a > y
				if bytes.Compare(left.lb, right.lb) >= 0 { // x >= y
					return left
				} else { // x < y
					return right
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a > x and a <= y --> (x, y]
				// a > x and a < y  --> (x, y)
				finalFilter.lb = left.lb
				finalFilter.ub = right.lb
				if right.op == function.LESS_THAN {
					finalFilter.op = rangeBothOpen
				} else {
					finalFilter.op = rangeLeftOpen
				}
				finalFilter.valid = true

			case function.EQUAL:
				// a > x and a = y --> a = y if y > x
				if bytes.Compare(left.lb, right.lb) < 0 { // x < y
					finalFilter.op = function.EQUAL
					finalFilter.lb = right.lb
					finalFilter.valid = true
				}
			}

		case function.LESS_EQUAL:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a <= x and a >= y --> [y, x]
				// a <= x and a > y  --> (y, x]
				finalFilter.lb = right.lb
				finalFilter.ub = left.lb
				if right.op == function.GREAT_EQUAL {
					finalFilter.op = function.BETWEEN
				} else {
					finalFilter.op = rangeLeftOpen
				}
				finalFilter.valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a <= x and a <= y --> a <= min(x,y)
				// a <= x and a < y  --> a <= x if x < y | a < y if x >= y
				if bytes.Compare(left.lb, right.lb) < 0 { // x < y
					return left
				} else {
					return right
				}

			case function.EQUAL:
				// a <= x and a = y --> a = y if x >= y
				if bytes.Compare(left.lb, right.lb) >= 0 {
					finalFilter.op = function.EQUAL
					finalFilter.lb = right.lb
					finalFilter.valid = true
				}
			}

		case function.LESS_THAN:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a < x and a >= y --> [y, x)
				// a < x and a > y  --> (y, x)
				finalFilter.lb = right.lb
				finalFilter.ub = left.lb
				if right.op == function.GREAT_EQUAL {
					finalFilter.op = rangeRightOpen
				} else {
					finalFilter.op = rangeBothOpen
				}
				finalFilter.valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a < x and a <= y --> a < x if x <= y | a <= y if x > y
				// a < x and a < y  --> a < min(x,y)
				finalFilter.op = function.LESS_THAN
				if bytes.Compare(left.lb, right.lb) <= 0 {
					finalFilter.lb = left.lb
				} else {
					finalFilter.lb = right.lb
					if right.op == function.LESS_EQUAL {
						finalFilter.op = function.LESS_EQUAL
					}
				}
				finalFilter.valid = true

			case function.EQUAL:
				// a < x and a = y --> a = y if x > y
				if bytes.Compare(left.lb, right.lb) > 0 {
					finalFilter.op = function.EQUAL
					finalFilter.lb = right.lb
					finalFilter.valid = true
				}
			}

		case function.EQUAL:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a = x and a >= y --> a = x if x >= y
				// a = x and a > y  --> a = x if x > y
				if ret := bytes.Compare(left.lb, right.lb); ret > 0 {
					return left
				} else if ret == 0 && right.op == function.GREAT_EQUAL {
					return left
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a = x and a <= y --> a = x if x <= y
				// a = x and a < y  --> a = x if x < y
				if ret := bytes.Compare(left.lb, right.lb); ret < 0 {
					return left
				} else if ret == 0 && right.op == function.LESS_EQUAL {
					return left
				}

			case function.EQUAL:
				// a = x and a = y --> a = y if x = y
				if bytes.Equal(left.lb, right.lb) {
					return left
				}
			}
		}

	case function.OR:
		switch left.op {
		case function.IN:
			switch right.op {
			case function.IN:
				// a in (...) and a in (...)
				finalFilter = mergeBaseFilterInKind(left, right, true, proc)
			}

		case function.GREAT_EQUAL:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a >= x or a >= y --> a >= min(x, y)
				// a >= x or a > y  --> a >= x if x <= y | a > y if x > y
				if bytes.Compare(left.lb, right.lb) <= 0 { // x <= y
					return left
				} else { // x > y
					return right
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a >= x or a <= y --> all if x <= y | [] or []
				// a >= x or a < y  -->
				//finalFilter.lb = left.lb
				//finalFilter.ub = right.lb
				//if right.op == function.LESS_THAN {
				//	finalFilter.op = rangeRightOpen
				//} else {
				//	finalFilter.op = function.BETWEEN
				//}
				//finalFilter.valid = true

			case function.EQUAL:
				// a >= x or a = y --> a >= x if x <= y | [], x
				if bytes.Compare(left.lb, right.lb) <= 0 {
					finalFilter.op = function.GREAT_EQUAL
					finalFilter.lb = left.lb
					finalFilter.valid = true
				}
			}

		case function.GREAT_THAN:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a > x or a >= y --> a >= y if x >= y | a > x if x < y
				// a > x or a > y  --> a > y if x >= y | a > x if x < y
				if bytes.Compare(left.lb, right.lb) >= 0 { // x >= y
					return right
				} else { // x < y
					return left
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a > x or a <= y --> (x, y]
				// a > x or a < y  --> (x, y)
				//finalFilter.lb = left.lb
				//finalFilter.ub = right.lb
				//if right.op == function.LESS_THAN {
				//	finalFilter.op = rangeBothOpen
				//} else {
				//	finalFilter.op = rangeLeftOpen
				//}
				//finalFilter.valid = true

			case function.EQUAL:
				// a > x or a = y --> a > x if x < y | a >= x if x == y
				if ret := bytes.Compare(left.lb, right.lb); ret < 0 { // x < y
					return left
				} else if ret == 0 {
					finalFilter = left
					finalFilter.op = function.GREAT_EQUAL
				}
			}

		case function.LESS_EQUAL:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a <= x or a >= y -->
				// a <= x or a > y  -->
				//finalFilter.lb = right.lb
				//finalFilter.ub = left.lb
				//if right.op == function.GREAT_EQUAL {
				//	finalFilter.op = function.BETWEEN
				//} else {
				//	finalFilter.op = rangeLeftOpen
				//}
				//finalFilter.valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a <= x or a <= y --> a <= max(x,y)
				// a <= x or a < y  --> a <= x if x >= y | a < y if x < y
				if bytes.Compare(left.lb, right.lb) >= 0 { // x >= y
					return left
				} else {
					return right
				}

			case function.EQUAL:
				// a <= x or a = y --> a <= x if x >= y | [], x
				if bytes.Compare(left.lb, right.lb) >= 0 {
					return left
				}
			}

		case function.LESS_THAN:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a < x or a >= y
				// a < x or a > y
				//finalFilter.lb = right.lb
				//finalFilter.ub = left.lb
				//if right.op == function.GREAT_EQUAL {
				//	finalFilter.op = rangeRightOpen
				//} else {
				//	finalFilter.op = rangeBothOpen
				//}
				//finalFilter.valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a < x or a <= y --> a <= y if x <= y | a < x if x > y
				// a < x or a < y  --> a < y if x <= y | a < x if x > y
				if bytes.Compare(left.lb, right.lb) <= 0 { // a <= y
					return right
				} else {
					return left
				}

			case function.EQUAL:
				// a < x or a = y --> a < x if x > y | a <= x if x = y
				if ret := bytes.Compare(left.lb, right.lb); ret > 0 {
					return left
				} else if ret == 0 {
					finalFilter = left
					finalFilter.op = function.LESS_EQUAL
				}
			}

		case function.EQUAL:
			switch right.op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a = x or a >= y --> a >= y if x >= y
				// a = x or a > y  --> a > y if x > y | a >= y if x = y
				if ret := bytes.Compare(left.lb, right.lb); ret > 0 {
					return right
				} else if ret == 0 {
					finalFilter = right
					finalFilter.op = function.GREAT_EQUAL
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a = x or a <= y --> a <= y if x <= y
				// a = x or a < y  --> a < y if x < y | a <= y if x = y
				if ret := bytes.Compare(left.lb, right.lb); ret < 0 {
					return right
				} else if ret == 0 {
					finalFilter = right
					finalFilter.op = function.LESS_EQUAL
				}

			case function.EQUAL:
				// a = x or a = y --> a = x if x = y
				//                --> a in (x, y) if x != y
				if bytes.Equal(left.lb, right.lb) {
					return left
				}

			}
		}
	}
	return
}

func tryConstructPrimaryKeyIndexIter(
	ts timestamp.Timestamp,
	pkFilter memPKFilter,
	state *logtailreplay.PartitionState,
) (iter logtailreplay.RowsIter, delIterFactory func(blkId types.Blockid) logtailreplay.RowsIter) {
	if !pkFilter.isValid {
		return
	}

	switch pkFilter.op {
	case function.EQUAL, function.PREFIX_EQ:
		iter = state.NewPrimaryKeyIter(
			types.TimestampToTS(ts),
			logtailreplay.Prefix(pkFilter.packed[0]),
		)
		delIterFactory = func(blkId types.Blockid) logtailreplay.RowsIter {
			return state.NewPrimaryKeyDelIter(
				types.TimestampToTS(ts),
				logtailreplay.Prefix(pkFilter.packed[0]), blkId)
		}

	case function.IN, function.PREFIX_IN:
		// may be it's better to iterate rows instead.
		if len(pkFilter.packed) > 128 {
			return
		}

		iter = state.NewPrimaryKeyIter(
			types.TimestampToTS(ts),
			logtailreplay.InKind(pkFilter.packed, pkFilter.op),
		)
		delIterFactory = func(blkId types.Blockid) logtailreplay.RowsIter {
			return state.NewPrimaryKeyDelIter(
				types.TimestampToTS(ts),
				logtailreplay.InKind(pkFilter.packed, pkFilter.op), blkId)
		}

	case function.LESS_EQUAL, function.LESS_THAN:
		iter = state.NewPrimaryKeyIter(
			types.TimestampToTS(ts),
			logtailreplay.LessKind(pkFilter.packed[0], pkFilter.op == function.LESS_EQUAL),
		)
		delIterFactory = func(blkId types.Blockid) logtailreplay.RowsIter {
			return state.NewPrimaryKeyDelIter(
				types.TimestampToTS(ts),
				logtailreplay.LessKind(pkFilter.packed[0], pkFilter.op == function.LESS_EQUAL), blkId)
		}

	case function.GREAT_EQUAL, function.GREAT_THAN:
		iter = state.NewPrimaryKeyIter(
			types.TimestampToTS(ts),
			logtailreplay.GreatKind(pkFilter.packed[0], pkFilter.op == function.GREAT_EQUAL),
		)
		delIterFactory = func(blkId types.Blockid) logtailreplay.RowsIter {
			return state.NewPrimaryKeyDelIter(
				types.TimestampToTS(ts),
				logtailreplay.GreatKind(pkFilter.packed[0], pkFilter.op == function.GREAT_EQUAL), blkId)
		}

	case function.BETWEEN, rangeLeftOpen, rangeRightOpen, rangeBothOpen, function.PREFIX_BETWEEN:
		var kind int
		switch pkFilter.op {
		case function.BETWEEN:
			kind = 0
		case rangeLeftOpen:
			kind = 1
		case rangeRightOpen:
			kind = 2
		case rangeBothOpen:
			kind = 3
		case function.PREFIX_BETWEEN:
			kind = 4
		}
		iter = state.NewPrimaryKeyIter(
			types.TimestampToTS(ts),
			logtailreplay.BetweenKind(pkFilter.packed[0], pkFilter.packed[1], kind))

		delIterFactory = func(blkId types.Blockid) logtailreplay.RowsIter {
			return state.NewPrimaryKeyDelIter(
				types.TimestampToTS(ts),
				logtailreplay.BetweenKind(pkFilter.packed[0], pkFilter.packed[1], kind), blkId)
		}
	}

	return iter, delIterFactory
}

func getPkExpr(
	expr *plan.Expr, pkName string, proc *process.Process,
) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			leftPK := getPkExpr(exprImpl.F.Args[0], pkName, proc)
			if leftPK == nil {
				return nil
			}
			rightPK := getPkExpr(exprImpl.F.Args[1], pkName, proc)
			if rightPK == nil {
				return nil
			}
			if litExpr, ok := leftPK.Expr.(*plan.Expr_Lit); ok {
				if litExpr.Lit.Isnull {
					return rightPK
				}
			}
			if litExpr, ok := rightPK.Expr.(*plan.Expr_Lit); ok {
				if litExpr.Lit.Isnull {
					return leftPK
				}
			}
			return &plan.Expr{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{leftPK, rightPK},
					},
				},
				Typ: leftPK.Typ,
			}

		case "and":
			pkBytes := getPkExpr(exprImpl.F.Args[0], pkName, proc)
			if pkBytes != nil {
				return pkBytes
			}
			return getPkExpr(exprImpl.F.Args[1], pkName, proc)

		case "=":
			if col := exprImpl.F.Args[0].GetCol(); col != nil {
				if !compPkCol(col.Name, pkName) {
					return nil
				}
				constVal := getConstValueByExpr(exprImpl.F.Args[1], proc)
				if constVal == nil {
					return nil
				}
				return &plan.Expr{
					Typ: exprImpl.F.Args[1].Typ,
					Expr: &plan.Expr_Lit{
						Lit: constVal,
					},
				}
			}
			if col := exprImpl.F.Args[1].GetCol(); col != nil {
				if !compPkCol(col.Name, pkName) {
					return nil
				}
				constVal := getConstValueByExpr(exprImpl.F.Args[0], proc)
				if constVal == nil {
					return nil
				}
				return &plan.Expr{
					Typ: exprImpl.F.Args[0].Typ,
					Expr: &plan.Expr_Lit{
						Lit: constVal,
					},
				}
			}
			return nil

		case "in":
			if col := exprImpl.F.Args[0].GetCol(); col != nil {
				if !compPkCol(col.Name, pkName) {
					return nil
				}
				return exprImpl.F.Args[1]
			}

		case "prefix_eq", "prefix_between", "prefix_in", "between":
			if col := exprImpl.F.Args[0].GetCol(); col != nil {
				if !compPkCol(col.Name, pkName) {
					return nil
				}
				return expr
			}
		}
	}

	return nil
}

func LinearSearchOffsetByValFactory(pk *vector.Vector) func(*vector.Vector) []int32 {
	mp := make(map[any]bool)
	switch pk.GetType().Oid {
	case types.T_bool:
		vs := vector.MustFixedCol[bool](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_bit:
		vs := vector.MustFixedCol[uint64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int8:
		vs := vector.MustFixedCol[int8](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int16:
		vs := vector.MustFixedCol[int16](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int32:
		vs := vector.MustFixedCol[int32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int64:
		vs := vector.MustFixedCol[int64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint8:
		vs := vector.MustFixedCol[uint8](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint16:
		vs := vector.MustFixedCol[uint16](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint32:
		vs := vector.MustFixedCol[uint32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint64:
		vs := vector.MustFixedCol[uint64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_decimal64:
		vs := vector.MustFixedCol[types.Decimal64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_decimal128:
		vs := vector.MustFixedCol[types.Decimal128](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uuid:
		vs := vector.MustFixedCol[types.Uuid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_float32:
		vs := vector.MustFixedCol[float32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_float64:
		vs := vector.MustFixedCol[float64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_date:
		vs := vector.MustFixedCol[types.Date](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_timestamp:
		vs := vector.MustFixedCol[types.Timestamp](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_time:
		vs := vector.MustFixedCol[types.Time](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_datetime:
		vs := vector.MustFixedCol[types.Datetime](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_enum:
		vs := vector.MustFixedCol[types.Enum](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_TS:
		vs := vector.MustFixedCol[types.TS](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_Rowid:
		vs := vector.MustFixedCol[types.Rowid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_Blockid:
		vs := vector.MustFixedCol[types.Blockid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		if pk.IsConst() {
			for i := 0; i < pk.Length(); i++ {
				v := pk.UnsafeGetStringAt(i)
				mp[v] = true
			}
		} else {
			vs := vector.MustFixedCol[types.Varlena](pk)
			area := pk.GetArea()
			for i := 0; i < len(vs); i++ {
				v := vs[i].UnsafeGetString(area)
				mp[v] = true
			}
		}
	case types.T_array_float32:
		for i := 0; i < pk.Length(); i++ {
			v := types.ArrayToString[float32](vector.GetArrayAt[float32](pk, i))
			mp[v] = true
		}
	case types.T_array_float64:
		for i := 0; i < pk.Length(); i++ {
			v := types.ArrayToString[float64](vector.GetArrayAt[float64](pk, i))
			mp[v] = true
		}
	default:
		panic(moerr.NewInternalErrorNoCtx("%s not supported", pk.GetType().String()))
	}

	return func(vec *vector.Vector) []int32 {
		var sels []int32
		switch vec.GetType().Oid {
		case types.T_bool:
			vs := vector.MustFixedCol[bool](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_bit:
			vs := vector.MustFixedCol[uint64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_int8:
			vs := vector.MustFixedCol[int8](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_int16:
			vs := vector.MustFixedCol[int16](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_int32:
			vs := vector.MustFixedCol[int32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_int64:
			vs := vector.MustFixedCol[int64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uint8:
			vs := vector.MustFixedCol[uint8](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uint16:
			vs := vector.MustFixedCol[uint16](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uint32:
			vs := vector.MustFixedCol[uint32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uint64:
			vs := vector.MustFixedCol[uint64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_decimal64:
			vs := vector.MustFixedCol[types.Decimal64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_decimal128:
			vs := vector.MustFixedCol[types.Decimal128](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uuid:
			vs := vector.MustFixedCol[types.Uuid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_float32:
			vs := vector.MustFixedCol[float32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_float64:
			vs := vector.MustFixedCol[float64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_date:
			vs := vector.MustFixedCol[types.Date](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_timestamp:
			vs := vector.MustFixedCol[types.Timestamp](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_time:
			vs := vector.MustFixedCol[types.Time](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_datetime:
			vs := vector.MustFixedCol[types.Datetime](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_enum:
			vs := vector.MustFixedCol[types.Enum](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_TS:
			vs := vector.MustFixedCol[types.TS](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_Rowid:
			vs := vector.MustFixedCol[types.Rowid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_Blockid:
			vs := vector.MustFixedCol[types.Blockid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_char, types.T_varchar, types.T_json,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
			if pk.IsConst() {
				for i := 0; i < pk.Length(); i++ {
					v := pk.UnsafeGetStringAt(i)
					if mp[v] {
						sels = append(sels, int32(i))
					}
				}
			} else {
				vs := vector.MustFixedCol[types.Varlena](pk)
				area := pk.GetArea()
				for i := 0; i < len(vs); i++ {
					v := vs[i].UnsafeGetString(area)
					if mp[v] {
						sels = append(sels, int32(i))
					}
				}
			}
		case types.T_array_float32:
			for i := 0; i < vec.Length(); i++ {
				v := types.ArrayToString[float32](vector.GetArrayAt[float32](vec, i))
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_array_float64:
			for i := 0; i < vec.Length(); i++ {
				v := types.ArrayToString[float64](vector.GetArrayAt[float64](vec, i))
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		default:
			panic(moerr.NewInternalErrorNoCtx("%s not supported", vec.GetType().String()))
		}
		return sels
	}
}

func getNonSortedPKSearchFuncByPKVec(
	vec *vector.Vector,
) blockio.ReadFilterSearchFuncType {

	searchPKFunc := LinearSearchOffsetByValFactory(vec)

	if searchPKFunc != nil {
		return func(vecs []*vector.Vector) []int32 {
			return searchPKFunc(vecs[0])
		}
	}
	return nil
}

func evalLiteralExpr(expr *plan.Literal, oid types.T) (canEval bool, val any) {
	switch val := expr.Value.(type) {
	case *plan.Literal_I8Val:
		return transferIval(val.I8Val, oid)
	case *plan.Literal_I16Val:
		return transferIval(val.I16Val, oid)
	case *plan.Literal_I32Val:
		return transferIval(val.I32Val, oid)
	case *plan.Literal_I64Val:
		return transferIval(val.I64Val, oid)
	case *plan.Literal_Dval:
		return transferDval(val.Dval, oid)
	case *plan.Literal_Sval:
		return transferSval(val.Sval, oid)
	case *plan.Literal_Bval:
		return transferBval(val.Bval, oid)
	case *plan.Literal_U8Val:
		return transferUval(val.U8Val, oid)
	case *plan.Literal_U16Val:
		return transferUval(val.U16Val, oid)
	case *plan.Literal_U32Val:
		return transferUval(val.U32Val, oid)
	case *plan.Literal_U64Val:
		return transferUval(val.U64Val, oid)
	case *plan.Literal_Fval:
		return transferFval(val.Fval, oid)
	case *plan.Literal_Dateval:
		return transferDateval(val.Dateval, oid)
	case *plan.Literal_Timeval:
		return transferTimeval(val.Timeval, oid)
	case *plan.Literal_Datetimeval:
		return transferDatetimeval(val.Datetimeval, oid)
	case *plan.Literal_Decimal64Val:
		return transferDecimal64val(val.Decimal64Val.A, oid)
	case *plan.Literal_Decimal128Val:
		return transferDecimal128val(val.Decimal128Val.A, val.Decimal128Val.B, oid)
	case *plan.Literal_Timestampval:
		return transferTimestampval(val.Timestampval, oid)
	case *plan.Literal_Jsonval:
		return transferSval(val.Jsonval, oid)
	case *plan.Literal_EnumVal:
		return transferUval(val.EnumVal, oid)
	}
	return
}

// return canEval, isNull, isVec, evaledVal
func getPkValueByExpr(
	expr *plan.Expr,
	pkName string,
	oid types.T,
	mustOne bool,
	proc *process.Process,
) (bool, bool, bool, any) {
	valExpr := getPkExpr(expr, pkName, proc)
	if valExpr == nil {
		return false, false, false, nil
	}

	switch exprImpl := valExpr.Expr.(type) {
	case *plan.Expr_Lit:
		if exprImpl.Lit.Isnull {
			return false, true, false, nil
		}
		canEval, val := evalLiteralExpr(exprImpl.Lit, oid)
		if canEval {
			return true, false, false, val
		} else {
			return false, false, false, nil
		}

	case *plan.Expr_Vec:
		if mustOne {
			vec := vector.NewVec(types.T_any.ToType())
			vec.UnmarshalBinary(exprImpl.Vec.Data)
			if vec.Length() != 1 {
				return false, false, false, nil
			}
			exprLit := rule.GetConstantValue(vec, true, 0)
			if exprLit == nil {
				return false, false, false, nil
			}
			if exprLit.Isnull {
				return false, true, false, nil
			}
			canEval, val := evalLiteralExpr(exprLit, oid)
			if canEval {
				return true, false, false, val
			}
			return false, false, false, nil
		}
		return true, false, true, exprImpl.Vec.Data

	case *plan.Expr_List:
		if mustOne {
			return false, false, false, nil
		}
		canEval, vec, put := evalExprListToVec(oid, exprImpl, proc)
		if !canEval || vec == nil || vec.Length() == 0 {
			return false, false, false, nil
		}
		data, _ := vec.MarshalBinary()
		put()
		return true, false, true, data
	}

	return false, false, false, nil
}

func evalExprListToVec(
	oid types.T, expr *plan.Expr_List, proc *process.Process,
) (canEval bool, vec *vector.Vector, put func()) {
	if expr == nil {
		return false, nil, nil
	}
	canEval, vec = recurEvalExprList(oid, expr, nil, proc)
	if !canEval {
		if vec != nil {
			proc.PutVector(vec)
		}
		return false, nil, nil
	}
	put = func() {
		proc.PutVector(vec)
	}
	vec.InplaceSort()
	return
}

func recurEvalExprList(
	oid types.T, inputExpr *plan.Expr_List, inputVec *vector.Vector, proc *process.Process,
) (canEval bool, outputVec *vector.Vector) {
	outputVec = inputVec
	for _, expr := range inputExpr.List.List {
		switch expr2 := expr.Expr.(type) {
		case *plan.Expr_Lit:
			canEval, val := evalLiteralExpr(expr2.Lit, oid)
			if !canEval {
				return false, outputVec
			}
			if outputVec == nil {
				outputVec = proc.GetVector(oid.ToType())
			}
			// TODO: not use appendAny
			if err := vector.AppendAny(outputVec, val, false, proc.Mp()); err != nil {
				return false, outputVec
			}
		case *plan.Expr_Vec:
			vec := vector.NewVec(oid.ToType())
			if err := vec.UnmarshalBinary(expr2.Vec.Data); err != nil {
				return false, outputVec
			}
			if outputVec == nil {
				outputVec = proc.GetVector(oid.ToType())
			}
			sels := make([]int32, vec.Length())
			for i := 0; i < vec.Length(); i++ {
				sels[i] = int32(i)
			}
			union := vector.GetUnionAllFunction(*outputVec.GetType(), proc.Mp())
			if err := union(outputVec, vec); err != nil {
				return false, outputVec
			}
		case *plan.Expr_List:
			if canEval, outputVec = recurEvalExprList(oid, expr2, outputVec, proc); !canEval {
				return false, outputVec
			}
		default:
			return false, outputVec
		}
	}
	return true, outputVec
}

func logDebugf(txnMeta txn.TxnMeta, msg string, infos ...interface{}) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		infos = append(infos, txnMeta.DebugString())
		logutil.Debugf(msg+" %s", infos...)
	}
}

func getConstValueByExpr(
	expr *plan.Expr, proc *process.Process,
) *plan.Literal {
	exec, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return nil
	}
	defer exec.Free()
	vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil
	}
	return rule.GetConstantValue(vec, true, 0)
}

func getConstExpr(oid int32, c *plan.Literal) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: oid},
		Expr: &plan.Expr_Lit{Lit: c},
	}
}

// ListTnService gets all tn service in the cluster
func ListTnService(
	service string,
	appendFn func(service *metadata.TNService),
) {
	mc := clusterservice.GetMOCluster(service)
	mc.GetTNService(clusterservice.NewSelector(), func(tn metadata.TNService) bool {
		if appendFn != nil {
			appendFn(&tn)
		}
		return true
	})
}

// util function for object stats

// ForeachBlkInObjStatsList receives an object info list,
// and visits each blk of these object info by OnBlock,
// until the onBlock returns false or all blks have been enumerated.
// when onBlock returns a false,
// the next argument decides whether continue onBlock on the next stats or exit foreach completely.
func ForeachBlkInObjStatsList(
	next bool,
	dataMeta objectio.ObjectDataMeta,
	onBlock func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool,
	objects ...objectio.ObjectStats,
) {
	stop := false
	objCnt := len(objects)

	for idx := 0; idx < objCnt && !stop; idx++ {
		iter := NewStatsBlkIter(&objects[idx], dataMeta)
		pos := uint32(0)
		for iter.Next() {
			blk := iter.Entry()
			var meta objectio.BlockObject
			if !dataMeta.IsEmpty() {
				meta = dataMeta.GetBlockMeta(pos)
			}
			pos++
			if !onBlock(blk, meta) {
				stop = true
				break
			}
		}

		if stop && next {
			stop = false
		}
	}
}

type StatsBlkIter struct {
	name       objectio.ObjectName
	extent     objectio.Extent
	blkCnt     uint16
	totalRows  uint32
	cur        int
	accRows    uint32
	curBlkRows uint32
	meta       objectio.ObjectDataMeta
}

func NewStatsBlkIter(stats *objectio.ObjectStats, meta objectio.ObjectDataMeta) *StatsBlkIter {
	return &StatsBlkIter{
		name:       stats.ObjectName(),
		blkCnt:     uint16(stats.BlkCnt()),
		extent:     stats.Extent(),
		cur:        -1,
		accRows:    0,
		totalRows:  stats.Rows(),
		curBlkRows: options.DefaultBlockMaxRows,
		meta:       meta,
	}
}

func (i *StatsBlkIter) Next() bool {
	if i.cur >= 0 {
		i.accRows += i.curBlkRows
	}
	i.cur++
	return i.cur < int(i.blkCnt)
}

func (i *StatsBlkIter) Entry() objectio.BlockInfo {
	if i.cur == -1 {
		i.cur = 0
	}

	// assume that all blks have DefaultBlockMaxRows, except the last one
	if i.meta.IsEmpty() {
		if i.cur == int(i.blkCnt-1) {
			i.curBlkRows = i.totalRows - i.accRows
		}
	} else {
		i.curBlkRows = i.meta.GetBlockMeta(uint32(i.cur)).GetRows()
	}

	loc := objectio.BuildLocation(i.name, i.extent, i.curBlkRows, uint16(i.cur))
	blk := objectio.BlockInfo{
		BlockID:   *objectio.BuildObjectBlockid(i.name, uint16(i.cur)),
		SegmentID: i.name.SegmentId(),
		MetaLoc:   objectio.ObjectLocation(loc),
	}
	return blk
}

func ForeachCommittedObjects(
	createObjs map[objectio.ObjectNameShort]struct{},
	delObjs map[objectio.ObjectNameShort]struct{},
	p *logtailreplay.PartitionState,
	onObj func(info logtailreplay.ObjectInfo) error) (err error) {
	for obj := range createObjs {
		if objInfo, ok := p.GetObject(obj); ok {
			if err = onObj(objInfo); err != nil {
				return
			}
		}
	}
	for obj := range delObjs {
		if objInfo, ok := p.GetObject(obj); ok {
			if err = onObj(objInfo); err != nil {
				return
			}
		}
	}
	return nil

}

func ForeachSnapshotObjects(
	ts timestamp.Timestamp,
	onObject func(obj logtailreplay.ObjectInfo, isCommitted bool) error,
	tableSnapshot *logtailreplay.PartitionState,
	uncommitted ...objectio.ObjectStats,
) (err error) {
	// process all uncommitted objects first
	for _, obj := range uncommitted {
		info := logtailreplay.ObjectInfo{
			ObjectStats: obj,
		}
		if err = onObject(info, false); err != nil {
			return
		}
	}

	// process all committed objects
	if tableSnapshot == nil {
		return
	}

	iter, err := tableSnapshot.NewObjectsIter(types.TimestampToTS(ts), true)
	if err != nil {
		return
	}
	defer iter.Close()
	for iter.Next() {
		obj := iter.Entry()
		if err = onObject(obj.ObjectInfo, true); err != nil {
			return
		}
	}
	return
}

func ConstructObjStatsByLoadObjMeta(
	ctx context.Context, metaLoc objectio.Location,
	fs fileservice.FileService) (stats objectio.ObjectStats, dataMeta objectio.ObjectDataMeta, err error) {

	// 1. load object meta
	var meta objectio.ObjectMeta
	if meta, err = objectio.FastLoadObjectMeta(ctx, &metaLoc, false, fs); err != nil {
		logutil.Error("fast load object meta failed when split object stats. ", zap.Error(err))
		return
	}
	dataMeta = meta.MustDataMeta()

	// 2. construct an object stats
	objectio.SetObjectStatsObjectName(&stats, metaLoc.Name())
	objectio.SetObjectStatsExtent(&stats, metaLoc.Extent())
	objectio.SetObjectStatsBlkCnt(&stats, dataMeta.BlockCount())

	sortKeyIdx := dataMeta.BlockHeader().SortKey()
	objectio.SetObjectStatsSortKeyZoneMap(&stats, dataMeta.MustGetColumn(sortKeyIdx).ZoneMap())

	totalRows := uint32(0)
	for idx := uint32(0); idx < dataMeta.BlockCount(); idx++ {
		totalRows += dataMeta.GetBlockMeta(idx).GetRows()
	}

	objectio.SetObjectStatsRowCnt(&stats, totalRows)

	return
}

// getDatabasesExceptDeleted remove databases delete in the txn from the CatalogCache
func getDatabasesExceptDeleted(accountId uint32, cache *cache.CatalogCache, txn *Transaction) []string {
	//first get all delete tables
	deleteDatabases := make(map[string]any)
	txn.deletedDatabaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		if key.accountId == accountId {
			deleteDatabases[key.name] = nil
		}
		return true
	})

	dbs := cache.Databases(accountId, txn.op.SnapshotTS())
	dbs = removeIf[string](dbs, func(t string) bool {
		return find[string](deleteDatabases, t)
	})
	return dbs
}

// removeIf removes the elements that pred is true.
func removeIf[T any](data []T, pred func(t T) bool) []T {
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}

func find[T ~string | ~int, S any](data map[T]S, val T) bool {
	if len(data) == 0 {
		return false
	}
	if _, exists := data[val]; exists {
		return true
	}
	return false
}

// txnIsValid
// if the workspace is nil or txnOp is aborted, it returns error
func txnIsValid(txnOp client.TxnOperator) (*Transaction, error) {
	if txnOp == nil {
		return nil, moerr.NewInternalErrorNoCtx("txnOp is nil")
	}
	ws := txnOp.GetWorkspace()
	if ws == nil {
		return nil, moerr.NewInternalErrorNoCtx("txn workspace is nil")
	}
	var wsTxn *Transaction
	var ok bool
	if wsTxn, ok = ws.(*Transaction); ok {
		if wsTxn == nil {
			return nil, moerr.NewTxnClosedNoCtx(txnOp.Txn().ID)
		}
	}
	//if it is not the Transaction instance, only check the txnOp
	if txnOp.Status() == txn.TxnStatus_Aborted {
		return nil, moerr.NewTxnClosedNoCtx(txnOp.Txn().ID)
	}
	return wsTxn, nil
}

func CheckTxnIsValid(txnOp client.TxnOperator) (err error) {
	_, err = txnIsValid(txnOp)
	return err
}

// concurrentTask is the task that runs in the concurrent executor.
type concurrentTask func() error

// ConcurrentExecutor is an interface that runs tasks concurrently.
type ConcurrentExecutor interface {
	// AppendTask append the concurrent task to the exuecutor.
	AppendTask(concurrentTask)
	// Run starts receive task to execute.
	Run(context.Context)
	// GetConcurrency returns the concurrency of this executor.
	GetConcurrency() int
}

type concurrentExecutor struct {
	// concurrency is the concurrency to run the tasks at the same time.
	concurrency int
	// task contains all the tasks needed to run.
	tasks chan concurrentTask
}

func newConcurrentExecutor(concurrency int) ConcurrentExecutor {
	return &concurrentExecutor{
		concurrency: concurrency,
		tasks:       make(chan concurrentTask, 2048),
	}
}

// AppendTask implements the ConcurrentExecutor interface.
func (e *concurrentExecutor) AppendTask(t concurrentTask) {
	e.tasks <- t
}

// Run implements the ConcurrentExecutor interface.
func (e *concurrentExecutor) Run(ctx context.Context) {
	for i := 0; i < e.concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return

				case t := <-e.tasks:
					if err := t(); err != nil {
						logutil.Errorf("failed to execute task: %v", err)
					}
				}
			}
		}()
	}
}

// GetConcurrency implements the ConcurrentExecutor interface.
func (e *concurrentExecutor) GetConcurrency() int {
	return e.concurrency
}

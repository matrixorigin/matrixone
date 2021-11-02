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

package overload

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

const (
	// noRt signs there is no function and returnType for this operator.
	noRt = math.MaxUint8
	// maxt is max length of binOpsReturnType and unaryOpsReturnType
	maxt = math.MaxUint8
)

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = errors.New("division by zero")
	// ErrZeroModulus is reported when computing the rest of a division by zero.
	ErrModByZero = errors.New("zero modulus")

	// BinOps contains the binary operations indexed by operation type.
	BinOps = map[int][]*BinOp{}

	// binOpsReturnType contains returnType of a binary expr, likes
	// int + float32, bigint + double, and so on.
	binOpsReturnType [][][]types.T

	// binOpsWithTypeCast contains rules of type cast for binary operators.
	// which left type and right type can't be resolved directly.
	binOpsTypeCastRules [][][]castResult

	// variants only used to init and get items from binOpsReturnType and binOpsTypeCastRules
	// binOperators does not include comparison operators because their returns are always T_sel.
	binOperators = []int{Or, And, Plus, Minus, Mult, Div, Mod, Like, NotLike, Typecast, EQ, LT, LE, GT, GE, NE}
	firstBinaryOp = binOperators[0]
)

type castResult struct {
	has      bool
	leftCast types.Type
	rightCast types.Type
	newReturnType types.T
}

func init() {
	initSliceForBinaryOps()
	initCastRulesForBinaryOps()
}

func BinaryEval(op int, ltyp, rtyp types.T, lc, rc bool, lv, rv *vector.Vector, p *process.Process) (*vector.Vector, error) {
	// do type cast if it needs.
	if rule, ok := binaryOpsNeedCast(op, ltyp, rtyp); ok {
		var err error
		if !lv.Typ.Eq(rule.leftCast) {
			lv, err = BinaryEval(Typecast, ltyp, rule.leftCast.Oid, lc, false, lv, vector.New(rule.leftCast), p)
			if err != nil {
				return nil, err
			}
		}
		if !rv.Typ.Eq(rule.rightCast) {
			rv, err = BinaryEval(Typecast, rtyp, rule.rightCast.Oid, rc, false, rv, vector.New(rule.rightCast), p)
			if err != nil {
				return nil, err
			}
		}
		ltyp, rtyp = rule.leftCast.Oid, rule.rightCast.Oid
	}

	if os, ok := BinOps[op]; ok {
		for _, o := range os {
			if binaryCheck(op, o.LeftType, o.RightType, ltyp, rtyp) {
				return o.Fn(lv, rv, p, lc, rc)
			}
		}
	}
	return nil, fmt.Errorf("%s not yet implemented for %s, %s", OpName[op], ltyp, rtyp)
}

func binaryCheck(_ int, arg0, arg1 types.T, val0, val1 types.T) bool {
	return arg0 == val0 && arg1 == val1
}

// GetBinOpReturnType returns the returnType of binary op and its arg types.
func GetBinOpReturnType(op int, lt, rt types.T) types.T {
	var t types.T
	if c := binOpsTypeCastRules[op-firstBinaryOp][lt][rt]; c.has {
		t = c.newReturnType
	} else {
		t = binOpsReturnType[op-firstBinaryOp][lt][rt]
	}

	if t == noRt { // just ignore and return any type to make error message normal.
		t = lt
	}
	return t
}

func initSliceForBinaryOps() {
	binOpsReturnType = make([][][]types.T, len(binOperators))
	for i := range binOperators {
		binOpsReturnType[i] = make([][]types.T, maxt)
		for p := range binOpsReturnType[i] {
			binOpsReturnType[i][p] = make([]types.T, maxt)
		}

		for l := range binOpsReturnType[i] {
			for r := range binOpsReturnType[i][l] {
				binOpsReturnType[i][l][r] = noRt
			}
		}
	}
}

func initCastRulesForBinaryOps() {
	// init cast rule for binary operators.
	binOpsTypeCastRules = make([][][]castResult, len(binOperators))
	for i := range binOpsTypeCastRules {
		binOpsTypeCastRules[i] = make([][]castResult, maxt)
		for j := range binOpsTypeCastRules[i] {
			binOpsTypeCastRules[i][j] = make([]castResult, maxt)
			for k := range binOpsTypeCastRules {
				binOpsTypeCastRules[i][j][k] = castResult{has: false, newReturnType: noRt}
			}
		}
	}

	ints := []uint8{types.T_int8, types.T_int16, types.T_int32, types.T_int64}
	uints := []uint8{types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64}
	floats := []uint8{types.T_float32, types.T_float64}
	chars := []uint8{types.T_char, types.T_varchar}

	// PLUS cast rule / Minus cast rule / Mult cast rule
	{
		ops := []int{Plus, Minus, Mult}
		for _, op := range ops {
			index := op - firstBinaryOp

			// cast to int64 + int64
			nl, nr, nret := types.Type{Oid: types.T_int64, Size: 8}, types.Type{Oid: types.T_int64, Size: 8}, types.T_int64
			// plus between int and uint
			for _, l := range ints {
				for _, r := range uints {
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nr,
						rightCast:     nl,
						newReturnType: types.T(nret),
					}
				}
			}
			// cast to float64 + float64
			nl, nr, nret = types.Type{Oid: types.T_float64, Size: 8}, types.Type{Oid: types.T_float64, Size: 8}, types.T_float64
			// plus between int and float
			for _, l := range ints {
				for _, r := range floats {
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsTypeCastRules[index][r][l] = castResult{
						has:           true,
						leftCast:      nr,
						rightCast:     nl,
						newReturnType: types.T(nret),
					}
				}
			}
			// plus between uint and float
			for _, l := range uints {
				for _, r := range floats {
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsTypeCastRules[index][r][l] = castResult{
						has:           true,
						leftCast:      nr,
						rightCast:     nl,
						newReturnType: types.T(nret),
					}
				}
			}
		}
	}
	// Div cast rule
	{
		index := Div-firstBinaryOp

		// cast to float64 / float64 = float64
		nl, nr, nret := types.Type{Oid: types.T_float64, Size: 8}, types.Type{Oid: types.T_float64, Size: 8}, types.T_float64
		// div between int and int
		for _, l := range ints {
			for _, r := range ints {
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
			}
		}
		// div between uint and uint
		for _, l := range uints {
			for _, r := range uints {
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
			}
		}
		// div between int and uint
		for _, l := range ints {
			for _, r := range uints {
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
				binOpsTypeCastRules[index][r][l] = castResult{
					has:           true,
					leftCast:      nr,
					rightCast:     nl,
					newReturnType: types.T(nret),
				}
			}
		}
		// div between int and float
		for _, l := range ints {
			for _, r := range floats {
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
				binOpsTypeCastRules[index][r][l] = castResult{
					has:           true,
					leftCast:      nr,
					rightCast:     nl,
					newReturnType: types.T(nret),
				}
			}
		}
		// div between uint and float
		for _, l := range uints {
			for _, r := range floats {
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
				binOpsTypeCastRules[index][r][l] = castResult{
					has:           true,
					leftCast:      nr,
					rightCast:     nl,
					newReturnType: types.T(nret),
				}
			}
		}
		// div between float32 and float64
		binOpsTypeCastRules[index][types.T_float32][types.T_float64] = castResult{
			has:           true,
			leftCast:      nl,
			rightCast:     nr,
			newReturnType: types.T(nret),
		}
		binOpsTypeCastRules[index][types.T_float64][types.T_float32] = castResult{
			has:           true,
			leftCast:      nr,
			rightCast:     nl,
			newReturnType: types.T(nret),
		}
	}
	// Mod cast rule
	{
		index := Mod - firstBinaryOp

		// cast to int64 % int64
		nl, nr, nret := types.Type{Oid: types.T_int64, Size: 8}, types.Type{Oid: types.T_int64, Size: 8}, types.T_int64
		// mod between int and int
		for _, l := range ints {
			for _, r := range ints {
				if l == r {
					continue
				}
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
			}
		}
		// mod between int and uint
		for _, l := range ints {
			for _, r := range uints {
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
				binOpsTypeCastRules[index][r][l] = castResult{
					has:           true,
					leftCast:      nr,
					rightCast:     nl,
					newReturnType: types.T(nret),
				}
			}
		}
		// cast to uint64 % uint64
		nl, nr, nret = types.Type{Oid: types.T_uint64, Size: 8}, types.Type{Oid: types.T_uint64, Size: 8}, types.T_uint64
		// mod between uint and uint
		for _, l := range uints {
			for _, r := range uints {
				if l == r {
					continue
				}
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
			}
		}
		// cast to float64 % float64
		nl, nr, nret = types.Type{Oid: types.T_float64, Size: 8}, types.Type{Oid: types.T_float64, Size: 8}, types.T_float64
		// mod between int and float
		for _, l := range ints {
			for _, r := range floats {
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
				binOpsTypeCastRules[index][r][l] = castResult{
					has:           true,
					leftCast:      nr,
					rightCast:     nl,
					newReturnType: types.T(nret),
				}
			}
		}
		// mod between uint and float
		for _, l := range uints {
			for _, r := range floats {
				binOpsTypeCastRules[index][l][r] = castResult{
					has:           true,
					leftCast:      nl,
					rightCast:     nr,
					newReturnType: types.T(nret),
				}
				binOpsTypeCastRules[index][r][l] = castResult{
					has:           true,
					leftCast:      nr,
					rightCast:     nl,
					newReturnType: types.T(nret),
				}
			}
		}
		// mod between float32 and float64
		binOpsTypeCastRules[index][types.T_float32][types.T_float64] = castResult{
			has:           true,
			leftCast:      nl,
			rightCast:     nr,
			newReturnType: types.T(nret),
		}
		binOpsReturnType[index][types.T_float32][types.T_float64] = types.T(nret)
		binOpsTypeCastRules[index][types.T_float64][types.T_float32] = castResult{
			has:           true,
			leftCast:      nr,
			rightCast:     nl,
			newReturnType: types.T(nret),
		}
		binOpsReturnType[index][types.T_float64][types.T_float32] = types.T(nret)
	}

	// EQ / NE / GE / GT / LE / LT
	{
		ops := []int{EQ, NE, GT, GE, LT, LE}
		for _, op := range ops {
			index := op - firstBinaryOp

			// cast to int64 op int64
			nl, nr, nret := types.Type{Oid: types.T_int64, Size: 8}, types.Type{Oid: types.T_int64, Size: 8}, types.T_sel
			// int op int
			for _, l := range ints {
				for _, r := range ints {
					if l == r {
						continue
					}
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][l][r] = types.T(nret)
				}
			}
			// uint op int
			for _, l := range ints {
				for _, r := range uints {
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][l][r] = types.T(nret)
					binOpsTypeCastRules[index][r][l] = castResult{
						has:           true,
						leftCast:      nr,
						rightCast:     nl,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][r][l] = types.T(nret)
				}
			}
			// cast to uint64 op uint64
			nl, nr, nret = types.Type{Oid: types.T_int64, Size: 8}, types.Type{Oid: types.T_int64, Size: 8}, types.T_sel
			// uint op uint
			for _, l := range uints {
				for _, r := range uints {
					if l == r {
						continue
					}
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][l][r] = types.T(nret)
				}
			}
			// cast to float64 op float64
			nl, nr, nret = types.Type{Oid: types.T_float64, Size: 8}, types.Type{Oid: types.T_float64, Size: 8}, types.T_sel
			// int op float
			for _, l := range ints {
				for _, r := range floats {
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][l][r] = types.T(nret)
					binOpsTypeCastRules[index][r][l] = castResult{
						has:           true,
						leftCast:      nr,
						rightCast:     nl,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][r][l] = types.T(nret)
				}
			}
			// uint op float
			for _, l := range uints {
				for _, r := range floats {
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][l][r] = types.T(nret)
					binOpsTypeCastRules[index][r][l] = castResult{
						has:           true,
						leftCast:      nr,
						rightCast:     nl,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][r][l] = types.T(nret)
				}
			}
			// char op numeric
			for _, l := range chars {
				var numeric []uint8
				numeric = append(numeric, floats...)
				numeric = append(numeric, ints...)
				numeric = append(numeric, uints...)
				for _, r := range numeric {
					binOpsTypeCastRules[index][l][r] = castResult{
						has:           true,
						leftCast:      nl,
						rightCast:     nr,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][l][r] = types.T(nret)
					binOpsTypeCastRules[index][r][l] = castResult{
						has:           true,
						leftCast:      nr,
						rightCast:     nl,
						newReturnType: types.T(nret),
					}
					binOpsReturnType[index][r][l] = types.T(nret)
				}
			}
			// float32 op float64
			binOpsTypeCastRules[index][types.T_float32][types.T_float64] = castResult{
				has:           true,
				leftCast:      nl,
				rightCast:     nr,
				newReturnType: types.T(nret),
			}
			binOpsReturnType[index][types.T_float32][types.T_float64] = types.T(nret)
			binOpsTypeCastRules[index][types.T_float64][types.T_float32] = castResult{
				has:           true,
				leftCast:      nr,
				rightCast:     nl,
				newReturnType: types.T(nret),
			}
			binOpsReturnType[index][types.T_float64][types.T_float32] = types.T(nret)
		}
	}
}

// binaryOpsNeedCast returns true if a binary operator needs type-cast for its arguments.
func binaryOpsNeedCast(op int, ltyp types.T, rtyp types.T) (castResult, bool) {
	return binOpsTypeCastRules[op-firstBinaryOp][ltyp][rtyp], binOpsTypeCastRules[op-firstBinaryOp][ltyp][rtyp].has
}

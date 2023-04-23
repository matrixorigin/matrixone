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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"math"
)

var (
	// operater `CASE` supported return type.
	retOperatorCaseSupports = []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_bool,
		types.T_uuid,
		types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
		types.T_decimal64, types.T_decimal128,
		types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_json,
	}
)

func caseCheck(_ []overload, inputs []types.Type) checkResult {
	l := len(inputs)

	needCast := false
	if l >= 1 {
		// X should be bool or Int.
		for i := 0; i < l-1; i += 2 {
			if inputs[i].Oid != types.T_bool {
				if inputs[i].IsIntOrUint() {
					needCast = true
				} else {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
			}
		}

		// Y should be cast to a same type.
		allYSame := true
		t := inputs[1]
		// XXX if all decimal but with different scales. should cast ?
		if l%2 == 1 {
			if inputs[l-1].Oid == t.Oid {
				allYSame = false
			}
		}
		if !allYSame {
			for i := 1; i < l; i += 2 {
				if t.Oid != inputs[i].Oid {
					allYSame = false
					break
				}
			}
		}

		// XXX choose a supported Y type.
		var source []types.Type
		minCost := math.MaxInt32
		retType := types.Type{}
		if allYSame {
			source = []types.Type{inputs[1]}
		} else {
			source = make([]types.Type, 0, (l+1)/2)
			for j := 1; j < l; j += 2 {
				source = append(source, inputs[j])
			}
			if l%2 == 1 {
				source = append(source, inputs[l-1])
			}
		}

		target := make([]types.T, len(source))

		for _, rett := range retOperatorCaseSupports {
			for i := range target {
				target[i] = rett
			}
			c, cost := tryToMatch(source, target)
			if c == matchFailed {
				continue
			}
			if cost < minCost {
				minCost = cost
				retType = rett.ToType()
			}
		}
		if minCost == math.MaxInt32 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		if minCost == 0 && !needCast {
			return newCheckResultWithSuccess(0)
		}

		finalTypes := make([]types.Type, len(inputs))
		for i := range finalTypes {
			if i%2 == 0 {
				finalTypes[i] = types.T_bool.ToType()
			} else {
				finalTypes[i] = retType
			}
		}
		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func caseFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	t := result.GetResultVector().GetType()
	switch t.Oid {
	case types.T_int8:
		return generalCaseFn[int8](parameters, result, proc, length)
	case types.T_int16:
		return generalCaseFn[int16](parameters, result, proc, length)
	case types.T_int32:
		return generalCaseFn[int32](parameters, result, proc, length)
	case types.T_int64:
		return generalCaseFn[int64](parameters, result, proc, length)
	case types.T_uint8:
		return generalCaseFn[uint8](parameters, result, proc, length)
	case types.T_uint16:
		return generalCaseFn[uint16](parameters, result, proc, length)
	case types.T_uint32:
		return generalCaseFn[uint32](parameters, result, proc, length)
	case types.T_uint64:
		return generalCaseFn[uint64](parameters, result, proc, length)
	case types.T_float32:
		return generalCaseFn[float32](parameters, result, proc, length)
	case types.T_float64:
		return generalCaseFn[float64](parameters, result, proc, length)
	case types.T_date:
		return generalCaseFn[types.Date](parameters, result, proc, length)
	case types.T_time:
		return generalCaseFn[types.Time](parameters, result, proc, length)
	case types.T_datetime:
		return generalCaseFn[types.Datetime](parameters, result, proc, length)
	case types.T_timestamp:
		return generalCaseFn[types.Timestamp](parameters, result, proc, length)
	case types.T_uuid:
		return generalCaseFn[types.Uuid](parameters, result, proc, length)
	case types.T_bool:
		return generalCaseFn[bool](parameters, result, proc, length)
	case types.T_decimal64:
		return generalCaseFn[types.Decimal64](parameters, result, proc, length)
	case types.T_decimal128:
		return generalCaseFn[types.Decimal128](parameters, result, proc, length)

	case types.T_char:
		return strCaseFn(parameters, result, proc, length)
	case types.T_varchar:
		return strCaseFn(parameters, result, proc, length)
	case types.T_blob:
		return strCaseFn(parameters, result, proc, length)
	case types.T_text:
		return strCaseFn(parameters, result, proc, length)
	case types.T_json:
		return strCaseFn(parameters, result, proc, length)
	}
	panic("unreached code")
}

func generalCaseFn[T constraints.Integer | constraints.Float | bool | types.Date | types.Datetime |
	types.Decimal64 | types.Decimal128 | types.Timestamp | types.Uuid](vecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	// case Xn then Yn else Z
	xs := make([]vector.FunctionParameterWrapper[bool], 0, len(vecs)/2)
	ys := make([]vector.FunctionParameterWrapper[T], 0, len(vecs)/2)

	l := len(vecs)
	for i := 0; i < l-1; i += 2 {
		xs = append(xs, vector.GenerateFunctionFixedTypeParameter[bool](vecs[i]))
	}
	for j := 1; j < l; j += 2 {
		ys = append(ys, vector.GenerateFunctionFixedTypeParameter[T](vecs[j]))
	}

	rs := vector.MustFunctionResult[T](result)

	if len(vecs)%2 == 1 {
		z := vector.GenerateFunctionFixedTypeParameter[T](vecs[len(vecs)-1])
		for i := uint64(0); i < uint64(length); i++ {
			matchElse := true
			for j := range xs {
				if v, null := xs[j].GetValue(i); !null && v {
					if err := rs.Append(ys[j].GetValue(i)); err != nil {
						return err
					}
					matchElse = false
					break
				}
				if matchElse {
					if err := rs.Append(z.GetValue(i)); err != nil {
						return err
					}
				}
			}
		}
	} else {
		var dv T // default value

		for i := uint64(0); i < uint64(length); i++ {
			matchElse := true
			for j := range xs {
				if v, null := xs[j].GetValue(i); !null && v {
					if err := rs.Append(ys[j].GetValue(i)); err != nil {
						return err
					}
					matchElse = false
					break
				}
				if matchElse {
					if err := rs.Append(dv, true); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func strCaseFn(vecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	// case Xn then Yn else Z
	xs := make([]vector.FunctionParameterWrapper[bool], 0, len(vecs)/2)
	ys := make([]vector.FunctionParameterWrapper[types.Varlena], 0, len(vecs)/2)

	l := len(vecs)
	for i := 0; i < l-1; i += 2 {
		xs = append(xs, vector.GenerateFunctionFixedTypeParameter[bool](vecs[i]))
	}
	for j := 1; j < l; j += 2 {
		ys = append(ys, vector.GenerateFunctionFixedTypeParameter[types.Varlena](vecs[j]))
	}

	rs := vector.MustFunctionResult[types.Varlena](result)

	if len(vecs)%2 == 1 {
		z := vector.GenerateFunctionFixedTypeParameter[types.Varlena](vecs[len(vecs)-1])
		for i := uint64(0); i < uint64(length); i++ {
			matchElse := true
			for j := range xs {
				if v, null := xs[j].GetValue(i); !null && v {
					if err := rs.AppendBytes(ys[j].GetStrValue(i)); err != nil {
						return err
					}
					matchElse = false
					break
				}
				if matchElse {
					if err := rs.AppendBytes(z.GetStrValue(i)); err != nil {
						return err
					}
				}
			}
		}
	} else {
		for i := uint64(0); i < uint64(length); i++ {
			matchElse := true
			for j := range xs {
				if v, null := xs[j].GetValue(i); !null && v {
					if err := rs.AppendBytes(ys[j].GetStrValue(i)); err != nil {
						return err
					}
					matchElse = false
					break
				}
				if matchElse {
					if err := rs.AppendBytes(nil, true); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

var (
	retOperatorIffSupports = []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_uuid,
		types.T_bool, types.T_date, types.T_datetime,
		types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_json,
		types.T_decimal64, types.T_decimal128,
		types.T_timestamp, types.T_time,
	}
)

func iffCheck(_ []overload, inputs []types.Type) checkResult {
	// iff(x, y, z)
	if len(inputs) == 3 {
		needCast := false
		if inputs[0].Oid != types.T_bool {
			if !inputs[0].IsIntOrUint() {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			needCast = true
		}

		minCost := math.MaxInt32
		retType := types.Type{}

		source := []types.Type{inputs[1], inputs[2]}
		target := make([]types.T, 2)
		for _, rett := range retOperatorIffSupports {
			target[0], target[1] = rett, rett

			c, cost := tryToMatch(source, target)
			if c == matchFailed {
				continue
			}
			if cost < minCost {
				minCost = cost
				retType = rett.ToType()
			}
		}

		if minCost == math.MaxInt32 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		if minCost == 0 && !needCast {
			return newCheckResultWithSuccess(0)
		}
		return newCheckResultWithCast(0, []types.Type{types.T_bool.ToType(), retType, retType})
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func iffFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rett := result.GetResultVector().GetType()
	switch rett.Oid {
	case types.T_int8:
		return generalIffFn[int8](parameters, result, proc, length)
	case types.T_int16:
		return generalIffFn[int16](parameters, result, proc, length)
	case types.T_int32:
		return generalIffFn[int32](parameters, result, proc, length)
	case types.T_int64:
		return generalIffFn[int64](parameters, result, proc, length)
	case types.T_uint8:
		return generalIffFn[uint8](parameters, result, proc, length)
	case types.T_uint16:
		return generalIffFn[uint16](parameters, result, proc, length)
	case types.T_uint32:
		return generalIffFn[uint32](parameters, result, proc, length)
	case types.T_uint64:
		return generalIffFn[uint64](parameters, result, proc, length)
	case types.T_float32:
		return generalIffFn[float32](parameters, result, proc, length)
	case types.T_float64:
		return generalIffFn[float64](parameters, result, proc, length)
	case types.T_uuid:
		return generalIffFn[types.Uuid](parameters, result, proc, length)
	case types.T_bool:
		return generalIffFn[bool](parameters, result, proc, length)
	case types.T_date:
		return generalIffFn[types.Date](parameters, result, proc, length)
	case types.T_datetime:
		return generalIffFn[types.Datetime](parameters, result, proc, length)
	case types.T_decimal64:
		return generalIffFn[types.Decimal64](parameters, result, proc, length)
	case types.T_decimal128:
		return generalIffFn[types.Decimal128](parameters, result, proc, length)
	case types.T_time:
		return generalIffFn[types.Time](parameters, result, proc, length)
	case types.T_timestamp:
		return generalIffFn[types.Timestamp](parameters, result, proc, length)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_json:
		return strIffFn(parameters, result, proc, length)
	}
	panic("unreached code")
}

func generalIffFn[T constraints.Integer | constraints.Float | bool | types.Date | types.Datetime |
	types.Decimal64 | types.Decimal128 | types.Timestamp | types.Uuid](vecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](vecs[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](vecs[1])
	p3 := vector.GenerateFunctionFixedTypeParameter[T](vecs[2])

	rs := vector.MustFunctionResult[T](result)
	var dfv T
	for i := uint64(0); i < uint64(length); i++ {
		b, null := p1.GetValue(i)
		if null {
			if err := rs.Append(dfv, true); err != nil {
				return err
			}
		}
		if b {
			if err := rs.Append(p2.GetValue(i)); err != nil {
				return err
			}
		} else {
			if err := rs.Append(p3.GetValue(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func strIffFn(vecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](vecs[0])
	p2 := vector.GenerateFunctionStrParameter(vecs[1])
	p3 := vector.GenerateFunctionStrParameter(vecs[2])

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		b, null := p1.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		if b {
			if err := rs.AppendBytes(p2.GetStrValue(i)); err != nil {
				return err
			}
		} else {
			if err := rs.AppendBytes(p3.GetStrValue(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorUnaryPlus[T constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(p1.GetValue(i)); err != nil {
			return err
		}
	}
	return nil
}

func operatorUnaryMinus[T constraints.Signed | constraints.Float](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if err := rs.Append(-v, null); err != nil {
			return err
		}
	}
	return nil
}

func operatorUnaryMinusDecimal64(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	rs := vector.MustFunctionResult[types.Decimal64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(v, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Minus(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorUnaryMinusDecimal128(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
	rs := vector.MustFunctionResult[types.Decimal128](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(v, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Minus(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func funcBitInversion[T constraints.Integer](x T) uint64 {
	if x > 0 {
		n := uint64(x)
		return ^n
	} else {
		return uint64(^x)
	}
}

func operatorUnaryTilde[T constraints.Integer](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[uint64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(funcBitInversion(v), null); err != nil {
				return err
			}
		}

	}
	return nil
}

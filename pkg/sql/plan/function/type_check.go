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

package function

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// a fixed type cast rule for
// 1. Plus Sub Multi
// 2. Equal NotEqual
// 3. >= > < <=
// 4. Mod
func fixedTypeCastRule1(s1, s2 types.Type) (bool, types.Type, types.Type) {
	check := fixedBinaryCastRule1[s1.Oid][s2.Oid]
	if check.cast {
		t1, t2 := check.left.ToType(), check.right.ToType()

		// special case: null + type, null + null, type + null
		if s1.Oid == types.T_any && s2.Oid == types.T_any {
			return true, t1, t2
		}
		if s1.Oid == types.T_any {
			return true, s2, s2
		}
		if s2.Oid == types.T_any {
			return true, s1, s1
		}

		// too bad.
		// but how to make sure we can let `time = varchar` always right if we want to convert varchar to be time.
		if t1.Oid.IsDateRelate() {
			if t1.Oid == t2.Oid {
				if s1.Oid == t1.Oid {
					return true, s1, s1
				} else if s2.Oid == t2.Oid {
					return true, s2, s2
				}
			}
		}

		if t1.Oid.IsArrayRelate() {
			if t1.Oid == t2.Oid {
				if s1.Oid == t1.Oid {
					return true, s1, s1
				} else if s2.Oid == t2.Oid {
					return true, s2, s2
				}
			}
		}

		setTargetScaleFromSource(&s1, &t1)
		setTargetScaleFromSource(&s2, &t2)

		return true, t1, t2
	}
	return false, s1, s2
}

// a fixed type cast rule for
//  1. Div
//  2. IntegerDiv
func fixedTypeCastRule2(s1, s2 types.Type) (bool, types.Type, types.Type) {
	check := fixedBinaryCastRule2[s1.Oid][s2.Oid]
	if check.cast {
		t1, t2 := check.left.ToType(), check.right.ToType()

		// special case: null + type, null + null, type + null
		if s1.Oid == types.T_any && s2.Oid == types.T_any {
			return true, t1, t2
		}
		if s1.Oid == types.T_any {
			return true, s2, s2
		}
		if s2.Oid == types.T_any {
			return true, s1, s1
		}

		if t1.Oid.IsDateRelate() {
			if t1.Oid == t2.Oid {
				if s1.Oid == t1.Oid {
					return true, s1, s1
				} else if s2.Oid == t2.Oid {
					return true, s2, s2
				}
			}
		}

		setTargetScaleFromSource(&s1, &t1)
		setTargetScaleFromSource(&s2, &t2)

		return true, t1, t2
	}
	return false, s1, s2
}

type matchCheckStatus int

const (
	matchDirectly matchCheckStatus = 0
	matchByCast   matchCheckStatus = 1
	matchFailed   matchCheckStatus = 2
)

// a fixed type match method.
func fixedTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	minIndex := -1
	minCost := math.MaxInt
	for i, ov := range overloads {
		if len(ov.args) != len(inputs) {
			continue
		}

		sta, cos := tryToMatch(inputs, ov.args)
		if sta == matchFailed {
			continue
		} else if sta == matchDirectly {
			return newCheckResultWithSuccess(i)
		} else {
			if cos < minCost {
				minIndex = i
				minCost = cos
			}
		}
	}
	if minIndex == -1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	castType := make([]types.Type, len(inputs))
	ov := overloads[minIndex]
	for i := range castType {
		if ov.args[i] == inputs[i].Oid {
			castType[i] = inputs[i]
		} else {
			castType[i] = ov.args[i].ToType()
			setTargetScaleFromSource(&inputs[i], &castType[i])
		}
	}
	return newCheckResultWithCast(minIndex, castType)
}

// a fixed type match method without any type convert. (const null exception)
// if all parameters were `constant null`, match the first one whose number of parameters was same.
func fixedDirectlyTypeMatch(overload []overload, inputs []types.Type) checkResult {
	for i, o := range overload {
		if len(o.args) != len(inputs) {
			continue
		}
		allSame := true
		allNull := true
		for j := range o.args {
			if inputs[j].Oid == types.T_any {
				continue
			}
			allNull = false
			if o.args[j] != inputs[j].Oid {
				allSame = false
				break
			}
		}
		if allSame || allNull {
			return newCheckResultWithSuccess(i)
		}
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

// return whether `from` can match `to` implicitly, and match cost.
func tryToMatch(from []types.Type, to []types.T) (sta matchCheckStatus, cost int) {
	if len(from) != len(to) {
		return matchFailed, -1
	}
	l := len(from)
	for i, source := range from {
		if source.Oid == to[i] {
			cost += 0
		} else {
			can, c := fixedImplicitTypeCast(source, to[i])
			if !can {
				return matchFailed, -1
			}
			if c == 1 {
				cost += c
			} else {
				cost += c * l
			}
		}
	}
	if cost == 0 {
		return matchDirectly, cost
	}
	return matchByCast, cost
}

// a fixed type implicit cast rule
// return can cast or not and cast cost.
func fixedImplicitTypeCast(from types.Type, to types.T) (canCast bool, cost int) {
	if from.Oid == types.T_any {
		return true, 1
	}
	rule := fixedCanImplicitCastRule[from.Oid].toList[to]
	return rule.canCast, rule.preferLevel
}

// a fixed type check method for Agg(only one column).
// do not do any implicit type conversion.
func fixedUnaryAggTypeCheck(inputs []types.Type, supported []types.T) checkResult {
	if len(inputs) == 1 && len(supported) > 0 {
		t := inputs[0]
		// if select agg(null), just match the first one.
		if t.Oid == types.T_any {
			return newCheckResultWithCast(0, []types.Type{supported[0].ToType()})
		}
		for _, supportT := range supported {
			if t.Oid == supportT {
				return newCheckResultWithSuccess(0)
			}
		}
	}
	return newCheckResultWithFailure(failedAggParametersWrong)
}

var fixedBinaryCastRule1 [300][300]tarTypes
var fixedBinaryCastRule2 [300][300]tarTypes
var fixedCanImplicitCastRule [300]implicitTypeCastRule

func setTargetScaleFromSource(source, target *types.Type) {
	if source.Oid == target.Oid {
		target.Scale = source.Scale
		return
	}

	if target.IsFloat() {
		if source.IsInt() || source.IsUInt() {
			target.Scale = 0
		} else if source.IsFloat() || source.IsDecimal() {
			target.Scale = int32(math.Min(float64(source.Scale), 30))
		} else {
			target.Scale = -1
		}
	}

	if target.Oid == types.T_datetime {
		if source.Oid.IsMySQLString() {
			target.Scale = 6
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_time {
		if source.Oid.IsMySQLString() {
			target.Scale = 6
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_timestamp {
		if source.Oid.IsMySQLString() {
			target.Scale = 6
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_decimal64 {
		if source.Oid.IsInteger() {
			target.Scale = 0
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_decimal128 {
		if source.Oid == types.T_decimal64 {
			target.Scale = source.Scale
		} else if source.Oid.IsInteger() {
			target.Scale = 0
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}
}

func setMaxScaleFromSource(t *types.Type, source []types.Type) {
	for i := range source {
		if source[i].Oid == t.Oid {
			if source[i].Scale > t.Scale {
				t.Scale = source[i].Scale
			}
		}
	}
}

func setMaxWidthFromSource(t *types.Type, source []types.Type) {
	t.Width = -1
	for i := range source {
		if source[i].Oid == t.Oid || source[i].Oid.IsMySQLString() {
			if source[i].Width > t.Width {
				t.Width = source[i].Width
			}
		}
	}
	if t.Width == -1 {
		t.Width = types.MaxVarBinaryLen
	}
}

func setMaxScaleForAll(source []types.Type) {
	maxScale := int32(math.MinInt32)
	maxWidth := int32(math.MinInt32)
	for _, t := range source {
		if t.Scale > maxScale {
			maxScale = t.Scale
		}
		if t.Width > maxWidth {
			maxWidth = t.Width
		}
	}
	for k := range source {
		source[k].Scale = maxScale
		source[k].Width = maxWidth
	}
}

func initFixed1() {
	// cast [0] + [1] ==> [2] + [3]
	ru := [][4]types.T{
		{types.T_any, types.T_any, types.T_int64, types.T_int64},
		{types.T_any, types.T_bool, types.T_bool, types.T_bool},
		{types.T_any, types.T_int8, types.T_int8, types.T_int8},
		{types.T_any, types.T_int16, types.T_int16, types.T_int16},
		{types.T_any, types.T_int32, types.T_int32, types.T_int32},
		{types.T_any, types.T_int64, types.T_int64, types.T_int64},
		{types.T_any, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_any, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_any, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_any, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_any, types.T_float32, types.T_float32, types.T_float32},
		{types.T_any, types.T_float64, types.T_float64, types.T_float64},
		{types.T_any, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_any, types.T_date, types.T_date, types.T_date},
		{types.T_any, types.T_time, types.T_time, types.T_time},
		{types.T_any, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_any, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_any, types.T_char, types.T_char, types.T_char},
		{types.T_any, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_any, types.T_binary, types.T_binary, types.T_binary},
		{types.T_any, types.T_varbinary, types.T_varbinary, types.T_varbinary},
		{types.T_any, types.T_blob, types.T_blob, types.T_blob},
		{types.T_any, types.T_text, types.T_text, types.T_text},
		{types.T_any, types.T_json, types.T_json, types.T_json},
		{types.T_bool, types.T_any, types.T_bool, types.T_bool},
		{types.T_bool, types.T_int8, types.T_int8, types.T_int8},
		{types.T_bool, types.T_int16, types.T_int16, types.T_int16},
		{types.T_bool, types.T_int32, types.T_int32, types.T_int32},
		{types.T_bool, types.T_int64, types.T_int64, types.T_int64},
		{types.T_bool, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_bool, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_bool, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_bool, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_bool, types.T_char, types.T_bool, types.T_bool},
		{types.T_bool, types.T_varchar, types.T_bool, types.T_bool},
		{types.T_bool, types.T_binary, types.T_bool, types.T_bool},
		{types.T_bool, types.T_varbinary, types.T_bool, types.T_bool},
		{types.T_bool, types.T_blob, types.T_bool, types.T_bool},
		{types.T_bool, types.T_text, types.T_bool, types.T_bool},
		{types.T_int8, types.T_any, types.T_int8, types.T_int8},
		{types.T_int8, types.T_bool, types.T_int8, types.T_int8},
		{types.T_int8, types.T_int16, types.T_int16, types.T_int16},
		{types.T_int8, types.T_int32, types.T_int32, types.T_int32},
		{types.T_int8, types.T_int64, types.T_int64, types.T_int64},
		{types.T_int8, types.T_uint8, types.T_int16, types.T_int16},
		{types.T_int8, types.T_uint16, types.T_int16, types.T_int16},
		{types.T_int8, types.T_uint32, types.T_int16, types.T_int16},
		{types.T_int8, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_int8, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int8, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int8, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_date, types.T_int64, types.T_int64},
		{types.T_int8, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int8, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_int8, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_int8, types.T_char, types.T_int8, types.T_int8},
		{types.T_int8, types.T_varchar, types.T_int8, types.T_int8},
		{types.T_int8, types.T_binary, types.T_int8, types.T_int8},
		{types.T_int8, types.T_varbinary, types.T_int8, types.T_int8},
		{types.T_int8, types.T_blob, types.T_int8, types.T_int8},
		{types.T_int8, types.T_text, types.T_int8, types.T_int8},
		{types.T_int16, types.T_any, types.T_int16, types.T_int16},
		{types.T_int16, types.T_bool, types.T_int16, types.T_int16},
		{types.T_int16, types.T_int8, types.T_int16, types.T_int16},
		{types.T_int16, types.T_int32, types.T_int32, types.T_int32},
		{types.T_int16, types.T_int64, types.T_int64, types.T_int64},
		{types.T_int16, types.T_uint8, types.T_int32, types.T_int32},
		{types.T_int16, types.T_uint16, types.T_int32, types.T_int32},
		{types.T_int16, types.T_uint32, types.T_int32, types.T_int32},
		{types.T_int16, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_int16, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int16, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int16, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_date, types.T_int64, types.T_int64},
		{types.T_int16, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int16, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_int16, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_int16, types.T_char, types.T_int16, types.T_int16},
		{types.T_int16, types.T_varchar, types.T_int16, types.T_int16},
		{types.T_int16, types.T_binary, types.T_int16, types.T_int16},
		{types.T_int16, types.T_varbinary, types.T_int16, types.T_int16},
		{types.T_int16, types.T_blob, types.T_int16, types.T_int16},
		{types.T_int16, types.T_text, types.T_int16, types.T_int16},
		{types.T_int32, types.T_any, types.T_int32, types.T_int32},
		{types.T_int32, types.T_bool, types.T_int32, types.T_int32},
		{types.T_int32, types.T_int8, types.T_int32, types.T_int32},
		{types.T_int32, types.T_int16, types.T_int32, types.T_int32},
		{types.T_int32, types.T_int64, types.T_int64, types.T_int64},
		{types.T_int32, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_int32, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_int32, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_int32, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_int32, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int32, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_date, types.T_int64, types.T_int64},
		{types.T_int32, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int32, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_int32, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_int32, types.T_char, types.T_int32, types.T_int32},
		{types.T_int32, types.T_varchar, types.T_int32, types.T_int32},
		{types.T_int32, types.T_binary, types.T_int32, types.T_int32},
		{types.T_int32, types.T_varbinary, types.T_int32, types.T_int32},
		{types.T_int32, types.T_blob, types.T_int32, types.T_int32},
		{types.T_int32, types.T_text, types.T_int32, types.T_int32},
		{types.T_int64, types.T_any, types.T_int64, types.T_int64},
		{types.T_int64, types.T_bool, types.T_int64, types.T_int64},
		{types.T_int64, types.T_int8, types.T_int64, types.T_int64},
		{types.T_int64, types.T_int16, types.T_int64, types.T_int64},
		{types.T_int64, types.T_int32, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint64, types.T_int64, types.T_int64},
		{types.T_int64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int64, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_date, types.T_int64, types.T_int64},
		{types.T_int64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int64, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_int64, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_int64, types.T_char, types.T_int64, types.T_int64},
		{types.T_int64, types.T_varchar, types.T_int64, types.T_int64},
		{types.T_int64, types.T_binary, types.T_int64, types.T_int64},
		{types.T_int64, types.T_varbinary, types.T_int64, types.T_int64},
		{types.T_int64, types.T_blob, types.T_int64, types.T_int64},
		{types.T_int64, types.T_text, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_any, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_bool, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_int8, types.T_int16, types.T_int16},
		{types.T_uint8, types.T_int16, types.T_int32, types.T_int32},
		{types.T_uint8, types.T_int32, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_int64, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_uint8, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_uint8, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_uint8, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint8, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_uint8, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_uint8, types.T_char, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_varchar, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_binary, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_varbinary, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_blob, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_text, types.T_uint8, types.T_uint8},
		{types.T_uint16, types.T_any, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_bool, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_int8, types.T_int16, types.T_int16},
		{types.T_uint16, types.T_int16, types.T_int32, types.T_int32},
		{types.T_uint16, types.T_int32, types.T_int64, types.T_int64},
		{types.T_uint16, types.T_int64, types.T_int64, types.T_int64},
		{types.T_uint16, types.T_uint8, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_uint16, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_uint16, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint16, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint16, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_uint16, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_uint16, types.T_char, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_varchar, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_binary, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_varbinary, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_blob, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_text, types.T_uint16, types.T_uint16},
		{types.T_uint32, types.T_any, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_bool, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_int8, types.T_int16, types.T_int16},
		{types.T_uint32, types.T_int16, types.T_int32, types.T_int32},
		{types.T_uint32, types.T_int32, types.T_int64, types.T_int64},
		{types.T_uint32, types.T_int64, types.T_int64, types.T_int64},
		{types.T_uint32, types.T_uint8, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_uint16, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_uint32, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint32, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint32, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_uint32, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_uint32, types.T_char, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_varchar, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_binary, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_varbinary, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_blob, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_text, types.T_uint32, types.T_uint32},
		{types.T_uint64, types.T_any, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_bool, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int8, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int16, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int32, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int64, types.T_int64, types.T_int64},
		{types.T_uint64, types.T_uint8, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_uint16, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_uint32, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint64, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_uint64, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_uint64, types.T_char, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_varchar, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_binary, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_varbinary, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_blob, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_text, types.T_uint64, types.T_uint64},
		{types.T_float32, types.T_any, types.T_float32, types.T_float32},
		{types.T_float32, types.T_int8, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int16, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int32, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_decimal64, types.T_float32, types.T_float32},
		{types.T_float32, types.T_decimal128, types.T_float32, types.T_float32},
		{types.T_float32, types.T_char, types.T_float32, types.T_float32},
		{types.T_float32, types.T_varchar, types.T_float32, types.T_float32},
		{types.T_float32, types.T_binary, types.T_float32, types.T_float32},
		{types.T_float32, types.T_varbinary, types.T_float32, types.T_float32},
		{types.T_float32, types.T_blob, types.T_float32, types.T_float32},
		{types.T_float32, types.T_text, types.T_float32, types.T_float32},
		{types.T_float64, types.T_any, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int8, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int16, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_float64, types.T_char, types.T_float64, types.T_float64},
		{types.T_float64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_float64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_float64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_float64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_float64, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_float32, types.T_float32, types.T_float32},
		{types.T_decimal64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_date, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_char, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_any, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_float32, types.T_float32, types.T_float32},
		{types.T_decimal128, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_date, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_time, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_char, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_binary, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_blob, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_text, types.T_float64, types.T_float64},
		{types.T_date, types.T_any, types.T_date, types.T_date},
		{types.T_date, types.T_int8, types.T_int64, types.T_int64},
		{types.T_date, types.T_int16, types.T_int64, types.T_int64},
		{types.T_date, types.T_int32, types.T_int64, types.T_int64},
		{types.T_date, types.T_int64, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint64, types.T_int64, types.T_int64},
		{types.T_date, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_date, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_date, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_date, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_date, types.T_char, types.T_date, types.T_date},
		{types.T_date, types.T_varchar, types.T_date, types.T_date},
		{types.T_date, types.T_binary, types.T_date, types.T_date},
		{types.T_date, types.T_varbinary, types.T_date, types.T_date},
		{types.T_date, types.T_blob, types.T_date, types.T_date},
		{types.T_date, types.T_text, types.T_date, types.T_date},
		{types.T_time, types.T_any, types.T_time, types.T_time},
		{types.T_time, types.T_int8, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int16, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int32, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint8, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint16, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint32, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_time, types.T_char, types.T_time, types.T_time},
		{types.T_time, types.T_varchar, types.T_time, types.T_time},
		{types.T_time, types.T_binary, types.T_time, types.T_time},
		{types.T_time, types.T_varbinary, types.T_time, types.T_time},
		{types.T_time, types.T_blob, types.T_time, types.T_time},
		{types.T_time, types.T_text, types.T_time, types.T_time},
		{types.T_datetime, types.T_any, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_int8, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_int16, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_int32, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_int64, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_uint8, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_uint16, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_uint32, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_uint64, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_date, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_datetime, types.T_char, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_varchar, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_binary, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_varbinary, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_blob, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_text, types.T_datetime, types.T_datetime},
		{types.T_timestamp, types.T_any, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_int8, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_int16, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_int32, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_int64, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_uint8, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_uint16, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_uint32, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_uint64, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_date, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_datetime, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_char, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_varchar, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_binary, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_varbinary, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_blob, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_text, types.T_timestamp, types.T_timestamp},
		{types.T_char, types.T_any, types.T_char, types.T_char},
		{types.T_char, types.T_bool, types.T_bool, types.T_bool},
		{types.T_char, types.T_int8, types.T_int8, types.T_int8},
		{types.T_char, types.T_int16, types.T_int16, types.T_int16},
		{types.T_char, types.T_int32, types.T_int32, types.T_int32},
		{types.T_char, types.T_int64, types.T_int64, types.T_int64},
		{types.T_char, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_char, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_char, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_char, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_char, types.T_float32, types.T_float32, types.T_float32},
		{types.T_char, types.T_float64, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_char, types.T_date, types.T_date, types.T_date},
		{types.T_char, types.T_time, types.T_time, types.T_time},
		{types.T_char, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_char, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_char, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_char, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_char, types.T_binary, types.T_char, types.T_char},
		{types.T_char, types.T_varbinary, types.T_char, types.T_char},
		{types.T_char, types.T_blob, types.T_char, types.T_char},
		{types.T_char, types.T_text, types.T_char, types.T_char},
		{types.T_varchar, types.T_any, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_bool, types.T_bool, types.T_bool},
		{types.T_varchar, types.T_int8, types.T_int8, types.T_int8},
		{types.T_varchar, types.T_int16, types.T_int16, types.T_int16},
		{types.T_varchar, types.T_int32, types.T_int32, types.T_int32},
		{types.T_varchar, types.T_int64, types.T_int64, types.T_int64},
		{types.T_varchar, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_varchar, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_varchar, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_varchar, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_varchar, types.T_float32, types.T_float32, types.T_float32},
		{types.T_varchar, types.T_float64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_date, types.T_date, types.T_date},
		{types.T_varchar, types.T_time, types.T_time, types.T_time},
		{types.T_varchar, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_varchar, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_varchar, types.T_char, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_varchar, types.T_binary, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_varbinary, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_blob, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_text, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_array_float32, types.T_array_float32, types.T_array_float32},
		{types.T_varchar, types.T_array_float64, types.T_array_float64, types.T_array_float64},
		{types.T_json, types.T_any, types.T_json, types.T_json},
		{types.T_json, types.T_bool, types.T_bool, types.T_bool},
		{types.T_json, types.T_int8, types.T_int8, types.T_int8},
		{types.T_json, types.T_int16, types.T_int16, types.T_int16},
		{types.T_json, types.T_int32, types.T_int32, types.T_int32},
		{types.T_json, types.T_int64, types.T_int64, types.T_int64},
		{types.T_json, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_json, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_json, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_json, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_json, types.T_float32, types.T_float32, types.T_float32},
		{types.T_json, types.T_float64, types.T_float64, types.T_float64},
		{types.T_json, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_json, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_json, types.T_date, types.T_date, types.T_date},
		{types.T_json, types.T_time, types.T_time, types.T_time},
		{types.T_json, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_json, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_json, types.T_char, types.T_json, types.T_json},
		{types.T_json, types.T_varchar, types.T_json, types.T_json},
		{types.T_json, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_json, types.T_binary, types.T_json, types.T_json},
		{types.T_json, types.T_varbinary, types.T_json, types.T_json},
		{types.T_json, types.T_blob, types.T_json, types.T_json},
		{types.T_json, types.T_text, types.T_json, types.T_json},
		{types.T_uuid, types.T_char, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_varchar, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_binary, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_varbinary, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_blob, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_text, types.T_uuid, types.T_uuid},
		{types.T_binary, types.T_any, types.T_binary, types.T_binary},
		{types.T_binary, types.T_bool, types.T_bool, types.T_bool},
		{types.T_binary, types.T_int8, types.T_int8, types.T_int8},
		{types.T_binary, types.T_int16, types.T_int16, types.T_int16},
		{types.T_binary, types.T_int32, types.T_int32, types.T_int32},
		{types.T_binary, types.T_int64, types.T_int64, types.T_int64},
		{types.T_binary, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_binary, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_binary, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_binary, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_binary, types.T_float32, types.T_float32, types.T_float32},
		{types.T_binary, types.T_float64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_binary, types.T_date, types.T_date, types.T_date},
		{types.T_binary, types.T_time, types.T_time, types.T_time},
		{types.T_binary, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_binary, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_binary, types.T_char, types.T_char, types.T_char},
		{types.T_binary, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_binary, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_binary, types.T_varbinary, types.T_binary, types.T_binary},
		{types.T_binary, types.T_blob, types.T_binary, types.T_binary},
		{types.T_binary, types.T_text, types.T_binary, types.T_binary},
		{types.T_varbinary, types.T_any, types.T_varbinary, types.T_varbinary},
		{types.T_varbinary, types.T_bool, types.T_bool, types.T_bool},
		{types.T_varbinary, types.T_int8, types.T_int8, types.T_int8},
		{types.T_varbinary, types.T_int16, types.T_int16, types.T_int16},
		{types.T_varbinary, types.T_int32, types.T_int32, types.T_int32},
		{types.T_varbinary, types.T_int64, types.T_int64, types.T_int64},
		{types.T_varbinary, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_varbinary, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_varbinary, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_varbinary, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_varbinary, types.T_float32, types.T_float32, types.T_float32},
		{types.T_varbinary, types.T_float64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_date, types.T_date, types.T_date},
		{types.T_varbinary, types.T_time, types.T_time, types.T_time},
		{types.T_varbinary, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_varbinary, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_varbinary, types.T_char, types.T_char, types.T_char},
		{types.T_varbinary, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_varbinary, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_varbinary, types.T_binary, types.T_binary, types.T_binary},
		{types.T_varbinary, types.T_blob, types.T_varbinary, types.T_varbinary},
		{types.T_varbinary, types.T_text, types.T_varbinary, types.T_varbinary},
		{types.T_blob, types.T_any, types.T_blob, types.T_blob},
		{types.T_blob, types.T_bool, types.T_bool, types.T_bool},
		{types.T_blob, types.T_int8, types.T_int8, types.T_int8},
		{types.T_blob, types.T_int16, types.T_int16, types.T_int16},
		{types.T_blob, types.T_int32, types.T_int32, types.T_int32},
		{types.T_blob, types.T_int64, types.T_int64, types.T_int64},
		{types.T_blob, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_blob, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_blob, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_blob, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_blob, types.T_float32, types.T_float32, types.T_float32},
		{types.T_blob, types.T_float64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_blob, types.T_date, types.T_date, types.T_date},
		{types.T_blob, types.T_time, types.T_time, types.T_time},
		{types.T_blob, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_blob, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_blob, types.T_char, types.T_char, types.T_char},
		{types.T_blob, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_blob, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_blob, types.T_binary, types.T_binary, types.T_binary},
		{types.T_blob, types.T_varbinary, types.T_varbinary, types.T_varbinary},
		{types.T_blob, types.T_text, types.T_blob, types.T_blob},
		{types.T_text, types.T_any, types.T_text, types.T_text},
		{types.T_text, types.T_bool, types.T_bool, types.T_bool},
		{types.T_text, types.T_int8, types.T_int8, types.T_int8},
		{types.T_text, types.T_int16, types.T_int16, types.T_int16},
		{types.T_text, types.T_int32, types.T_int32, types.T_int32},
		{types.T_text, types.T_int64, types.T_int64, types.T_int64},
		{types.T_text, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_text, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_text, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_text, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_text, types.T_float32, types.T_float32, types.T_float32},
		{types.T_text, types.T_float64, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_text, types.T_date, types.T_date, types.T_date},
		{types.T_text, types.T_time, types.T_time, types.T_time},
		{types.T_text, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_text, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_text, types.T_char, types.T_char, types.T_char},
		{types.T_text, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_text, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_text, types.T_binary, types.T_binary, types.T_binary},
		{types.T_text, types.T_varbinary, types.T_varbinary, types.T_varbinary},
		{types.T_text, types.T_blob, types.T_blob, types.T_blob},
		{types.T_array_float32, types.T_varchar, types.T_array_float32, types.T_array_float32},
		{types.T_array_float32, types.T_array_float64, types.T_array_float64, types.T_array_float64},
		{types.T_array_float64, types.T_varchar, types.T_array_float64, types.T_array_float64},
		{types.T_array_float64, types.T_array_float32, types.T_array_float64, types.T_array_float64},
	}

	for _, r := range ru {
		addFixedBinaryCastRule1(r[0], r[1], r[2], r[3])
	}
}

func initFixed2() {
	ru := [][4]types.T{
		{types.T_any, types.T_any, types.T_float64, types.T_float64},
		{types.T_any, types.T_bool, types.T_float64, types.T_float64},
		{types.T_any, types.T_int8, types.T_float64, types.T_float64},
		{types.T_any, types.T_int16, types.T_float64, types.T_float64},
		{types.T_any, types.T_int32, types.T_float64, types.T_float64},
		{types.T_any, types.T_int64, types.T_float64, types.T_float64},
		{types.T_any, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_any, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_any, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_any, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_any, types.T_float32, types.T_float64, types.T_float64},
		{types.T_any, types.T_float64, types.T_float64, types.T_float64},
		{types.T_any, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_any, types.T_date, types.T_float64, types.T_float64},
		{types.T_any, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_char, types.T_float64, types.T_float64},
		{types.T_any, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_any, types.T_binary, types.T_float64, types.T_float64},
		{types.T_any, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_any, types.T_blob, types.T_float64, types.T_float64},
		{types.T_any, types.T_text, types.T_float64, types.T_float64},
		{types.T_any, types.T_json, types.T_float64, types.T_float64},
		{types.T_bool, types.T_any, types.T_float64, types.T_float64},
		{types.T_bool, types.T_int8, types.T_int8, types.T_int8},
		{types.T_bool, types.T_int16, types.T_int16, types.T_int16},
		{types.T_bool, types.T_int32, types.T_int32, types.T_int32},
		{types.T_bool, types.T_int64, types.T_int64, types.T_int64},
		{types.T_bool, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_bool, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_bool, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_bool, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_int8, types.T_any, types.T_float64, types.T_float64},
		{types.T_int8, types.T_bool, types.T_int8, types.T_int8},
		{types.T_int8, types.T_int8, types.T_float64, types.T_float64},
		{types.T_int8, types.T_int16, types.T_float64, types.T_float64},
		{types.T_int8, types.T_int32, types.T_float64, types.T_float64},
		{types.T_int8, types.T_int64, types.T_float64, types.T_float64},
		{types.T_int8, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_int8, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_int8, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_int8, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_int8, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int8, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int8, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_date, types.T_int64, types.T_int64},
		{types.T_int8, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int8, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_int8, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_int8, types.T_char, types.T_float64, types.T_float64},
		{types.T_int8, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_int8, types.T_binary, types.T_float64, types.T_float64},
		{types.T_int8, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_int8, types.T_blob, types.T_float64, types.T_float64},
		{types.T_int8, types.T_text, types.T_float64, types.T_float64},
		{types.T_int16, types.T_any, types.T_float64, types.T_float64},
		{types.T_int16, types.T_bool, types.T_int16, types.T_int16},
		{types.T_int16, types.T_int8, types.T_float64, types.T_float64},
		{types.T_int16, types.T_int16, types.T_float64, types.T_float64},
		{types.T_int16, types.T_int32, types.T_float64, types.T_float64},
		{types.T_int16, types.T_int64, types.T_float64, types.T_float64},
		{types.T_int16, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_int16, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_int16, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_int16, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_int16, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int16, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int16, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_date, types.T_int64, types.T_int64},
		{types.T_int16, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int16, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_int16, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_int16, types.T_char, types.T_float64, types.T_float64},
		{types.T_int16, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_int16, types.T_binary, types.T_float64, types.T_float64},
		{types.T_int16, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_int16, types.T_blob, types.T_float64, types.T_float64},
		{types.T_int16, types.T_text, types.T_float64, types.T_float64},
		{types.T_int32, types.T_any, types.T_float64, types.T_float64},
		{types.T_int32, types.T_bool, types.T_int32, types.T_int32},
		{types.T_int32, types.T_int8, types.T_float64, types.T_float64},
		{types.T_int32, types.T_int16, types.T_float64, types.T_float64},
		{types.T_int32, types.T_int32, types.T_float64, types.T_float64},
		{types.T_int32, types.T_int64, types.T_float64, types.T_float64},
		{types.T_int32, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_int32, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_int32, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_int32, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_int32, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int32, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_date, types.T_int64, types.T_int64},
		{types.T_int32, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int32, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_int32, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_int32, types.T_char, types.T_float64, types.T_float64},
		{types.T_int32, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_int32, types.T_binary, types.T_float64, types.T_float64},
		{types.T_int32, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_int32, types.T_blob, types.T_float64, types.T_float64},
		{types.T_int32, types.T_text, types.T_float64, types.T_float64},
		{types.T_int64, types.T_any, types.T_float64, types.T_float64},
		{types.T_int64, types.T_bool, types.T_int64, types.T_int64},
		{types.T_int64, types.T_int8, types.T_float64, types.T_float64},
		{types.T_int64, types.T_int16, types.T_float64, types.T_float64},
		{types.T_int64, types.T_int32, types.T_float64, types.T_float64},
		{types.T_int64, types.T_int64, types.T_float64, types.T_float64},
		{types.T_int64, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint64, types.T_int64, types.T_int64},
		{types.T_int64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int64, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_date, types.T_int64, types.T_int64},
		{types.T_int64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int64, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_int64, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_int64, types.T_char, types.T_float64, types.T_float64},
		{types.T_int64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_int64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_int64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_int64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_int64, types.T_text, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_any, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_bool, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_int8, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_int16, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_int32, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_int64, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint8, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_uint8, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_uint8, types.T_char, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_binary, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_blob, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_text, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_any, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_bool, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_int8, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_int16, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_int32, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_int64, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint16, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint16, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_uint16, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_uint16, types.T_char, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_binary, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_blob, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_text, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_any, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_bool, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_int8, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_int16, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_int32, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_int64, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint32, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint32, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_uint32, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_uint32, types.T_char, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_binary, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_blob, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_text, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_any, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_bool, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int8, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_int16, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_int32, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_int64, types.T_int64, types.T_int64},
		{types.T_uint64, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint64, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_uint64, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_uint64, types.T_char, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_text, types.T_float64, types.T_float64},
		{types.T_float32, types.T_any, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int8, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int16, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int32, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_float32, types.T_char, types.T_float64, types.T_float64},
		{types.T_float32, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_float32, types.T_binary, types.T_float64, types.T_float64},
		{types.T_float32, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_float32, types.T_blob, types.T_float64, types.T_float64},
		{types.T_float32, types.T_text, types.T_float64, types.T_float64},
		{types.T_float64, types.T_any, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int8, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int16, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_float64, types.T_char, types.T_float64, types.T_float64},
		{types.T_float64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_float64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_float64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_float64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_float64, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_date, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_char, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_any, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_float32, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_date, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_time, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_char, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_binary, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_blob, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_text, types.T_float64, types.T_float64},
		{types.T_date, types.T_any, types.T_int64, types.T_int64},
		{types.T_date, types.T_int8, types.T_int64, types.T_int64},
		{types.T_date, types.T_int16, types.T_int64, types.T_int64},
		{types.T_date, types.T_int32, types.T_int64, types.T_int64},
		{types.T_date, types.T_int64, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint64, types.T_int64, types.T_int64},
		{types.T_date, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_date, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_time, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int8, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int16, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int32, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint8, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint16, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint32, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_int8, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_int16, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_int32, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_int64, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_uint8, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_uint16, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_uint32, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_uint64, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_int8, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_int16, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_int32, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_int64, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_uint8, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_uint16, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_uint32, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_uint64, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_char, types.T_any, types.T_float64, types.T_float64},
		{types.T_char, types.T_int8, types.T_float64, types.T_float64},
		{types.T_char, types.T_int16, types.T_float64, types.T_float64},
		{types.T_char, types.T_int32, types.T_float64, types.T_float64},
		{types.T_char, types.T_int64, types.T_float64, types.T_float64},
		{types.T_char, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_char, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_char, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_char, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_char, types.T_float32, types.T_float64, types.T_float64},
		{types.T_char, types.T_float64, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_any, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_int8, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_int16, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_int32, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_int64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_float32, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_float64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal128, types.T_float64, types.T_float64},
		//A
		{types.T_varchar, types.T_array_float32, types.T_array_float32, types.T_array_float32},
		{types.T_varchar, types.T_array_float64, types.T_array_float64, types.T_array_float64},
		{types.T_binary, types.T_any, types.T_float64, types.T_float64},
		{types.T_binary, types.T_int8, types.T_float64, types.T_float64},
		{types.T_binary, types.T_int16, types.T_float64, types.T_float64},
		{types.T_binary, types.T_int32, types.T_float64, types.T_float64},
		{types.T_binary, types.T_int64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_binary, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_binary, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_binary, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_float32, types.T_float64, types.T_float64},
		{types.T_binary, types.T_float64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_any, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_int8, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_int16, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_int32, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_int64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_float32, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_float64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_blob, types.T_any, types.T_float64, types.T_float64},
		{types.T_blob, types.T_int8, types.T_float64, types.T_float64},
		{types.T_blob, types.T_int16, types.T_float64, types.T_float64},
		{types.T_blob, types.T_int32, types.T_float64, types.T_float64},
		{types.T_blob, types.T_int64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_blob, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_blob, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_blob, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_float32, types.T_float64, types.T_float64},
		{types.T_blob, types.T_float64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_json, types.T_any, types.T_float64, types.T_float64},
		{types.T_json, types.T_int8, types.T_float64, types.T_float64},
		{types.T_json, types.T_int16, types.T_float64, types.T_float64},
		{types.T_json, types.T_int32, types.T_float64, types.T_float64},
		{types.T_json, types.T_int64, types.T_float64, types.T_float64},
		{types.T_json, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_json, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_json, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_json, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_json, types.T_float32, types.T_float64, types.T_float64},
		{types.T_json, types.T_float64, types.T_float64, types.T_float64},
		{types.T_json, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_json, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_text, types.T_any, types.T_float64, types.T_float64},
		{types.T_text, types.T_int8, types.T_float64, types.T_float64},
		{types.T_text, types.T_int16, types.T_float64, types.T_float64},
		{types.T_text, types.T_int32, types.T_float64, types.T_float64},
		{types.T_text, types.T_int64, types.T_float64, types.T_float64},
		{types.T_text, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_text, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_text, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_text, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_text, types.T_float32, types.T_float64, types.T_float64},
		{types.T_text, types.T_float64, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal128, types.T_float64, types.T_float64},
		//B
		{types.T_array_float32, types.T_varchar, types.T_array_float32, types.T_array_float32},
		{types.T_array_float32, types.T_array_float32, types.T_array_float32, types.T_array_float32},
		{types.T_array_float64, types.T_varchar, types.T_array_float64, types.T_array_float64},
		{types.T_array_float64, types.T_array_float32, types.T_array_float64, types.T_array_float64},
	}

	for _, r := range ru {
		addFixedBinaryCastRule2(r[0], r[1], r[2], r[3])
	}
}

func initFixed3() {
	type toRule struct {
		toType      types.T
		preferLevel int
	}

	type rule struct {
		from   types.T
		toList []toRule
	}

	implicitCastSupported := []rule{
		{
			from: types.T_bool,
			toList: []toRule{
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_int8,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 1},
				{toType: types.T_int32, preferLevel: 1},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 1},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_int16,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 1},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 1},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_int32,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_int64,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_uint8,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 1},
				{toType: types.T_uint32, preferLevel: 1},
				{toType: types.T_uint64, preferLevel: 1},
				{toType: types.T_float32, preferLevel: 1},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_uint16,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 1},
				{toType: types.T_uint64, preferLevel: 1},
				{toType: types.T_float32, preferLevel: 1},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_uint32,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 1},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_uint64,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_float32,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_float64,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_decimal64,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_decimal128,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_date,
			toList: []toRule{
				{toType: types.T_datetime, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_time,
			toList: []toRule{
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_datetime,
			toList: []toRule{
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_timestamp,
			toList: []toRule{
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_char,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 1},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_varchar,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 1},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
				//C
				{toType: types.T_array_float32, preferLevel: 2},
				{toType: types.T_array_float64, preferLevel: 2},
			},
		},

		{
			from: types.T_uuid,
			toList: []toRule{
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_binary,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 1},
				{toType: types.T_blob, preferLevel: 1},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_varbinary,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 1},
				{toType: types.T_blob, preferLevel: 1},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_blob,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_text,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
			},
		},
		{
			from: types.T_enum,
			toList: []toRule{
				{toType: types.T_uint16, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_uint128, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},
		{
			from: types.T_array_float32,
			toList: []toRule{
				{toType: types.T_array_float64, preferLevel: 2},
			},
		},
		{
			from: types.T_array_float64,
			toList: []toRule{
				{toType: types.T_array_float32, preferLevel: 1},
			},
		},
	}

	for _, r := range implicitCastSupported {
		for _, to := range r.toList {
			addFixedImplicitTypeCastRule(r.from, to.toType, to.preferLevel)
		}
	}
}

type tarTypes struct {
	cast  bool
	left  types.T
	right types.T
}

type implicitTypeCastRule struct {
	from   types.T
	toList [300]struct {
		canCast     bool
		preferLevel int // 1 is the highest prefer level.
	}
}

func addFixedBinaryCastRule1(sourceLeft, sourceRight types.T, targetLeft, targetRight types.T) {
	fixedBinaryCastRule1[sourceLeft][sourceRight] = tarTypes{
		cast:  true,
		left:  targetLeft,
		right: targetRight,
	}
}

func addFixedBinaryCastRule2(sourceLeft, sourceRight types.T, targetLeft, targetRight types.T) {
	fixedBinaryCastRule2[sourceLeft][sourceRight] = tarTypes{
		cast:  true,
		left:  targetLeft,
		right: targetRight,
	}
}

func addFixedImplicitTypeCastRule(fromType types.T, toType types.T, preferLevel int) {
	fixedCanImplicitCastRule[fromType].from = fromType
	fixedCanImplicitCastRule[fromType].toList[toType].canCast = true
	fixedCanImplicitCastRule[fromType].toList[toType].preferLevel = preferLevel
}

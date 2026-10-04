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
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// OctStringOverloadStart separates persisted DECIMAL128 OCT identities from
// the VARCHAR implementation. Existing catalog expressions retain their type
// and overload ID and must continue to execute with the original semantics.
const OctStringOverloadStart = 15

func octTypeCheck(overloads []overload, inputs []types.Type) checkResult {
	result := fixedTypeMatch(overloads[OctStringOverloadStart:], inputs)
	if result.status == succeedMatched || result.status == succeedWithCast {
		result.idx += OctStringOverloadStart
	}
	return result
}

func withLegacyOctOverloads(current []overload) []overload {
	legacyOps := []executeLogicOfOverload{
		legacyOct[uint8], legacyOct[uint16], legacyOct[uint32], legacyOct[uint64],
		legacyOct[int8], legacyOct[int16], legacyOct[int32], legacyOct[int64],
		legacyOctFloat[float32], legacyOctFloat[float64], legacyOctDate, legacyOctDatetime,
		legacyOctString, legacyOctString, legacyOctString,
	}
	result := make([]overload, OctStringOverloadStart, OctStringOverloadStart+len(current))
	for i, op := range legacyOps {
		result[i] = current[i]
		result[i].retType = func([]types.Type) types.Type { return types.T_decimal128.ToType() }
		result[i].newOp = func() executeLogicOfOverload { return op }
	}
	for i := range current {
		current[i].overloadId += OctStringOverloadStart
	}
	return append(result, current...)
}

// These executors intentionally preserve pre-VARCHAR OCT behavior, including
// rounding and conversion errors, for already persisted expressions.
func legacyOct[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[T, types.Decimal128](ivecs, result, proc, length, legacyOctValue[T], selectList)
}

func legacyOctValue[T constraints.Unsigned | constraints.Signed](val T) (types.Decimal128, error) {
	_val := uint64(val)
	return types.ParseDecimal128(fmt.Sprintf("%o", _val), 38, 0)
}

func legacyOctFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[T, types.Decimal128](ivecs, result, proc, length, legacyOctFloatValue[T], selectList)
}

func legacyOctDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[types.Date, types.Decimal128](ivecs, result, proc, length, func(v types.Date) (types.Decimal128, error) {
		year, _, _, _ := v.Calendar(true)
		val := int64(year)
		return legacyOctValue[int64](val)
	}, selectList)
}

func legacyOctDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[types.Datetime, types.Decimal128](ivecs, result, proc, length, func(v types.Datetime) (types.Decimal128, error) {
		year, _, _, _ := v.ToDate().Calendar(true)
		val := int64(year)
		return legacyOctValue[int64](val)
	}, selectList)
}

func legacyOctString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[types.Decimal128](ivecs, result, proc, length, func(v []byte) (types.Decimal128, error) {
		s := string(v)
		dt, err := types.ParseDatetime(s, 6)
		if err == nil {
			year, _, _, _ := dt.ToDate().Calendar(true)
			val := int64(year)
			return legacyOctValue[int64](val)
		}
		d, err2 := types.ParseDateCast(s)
		if err2 == nil {
			year, _, _, _ := d.Calendar(true)
			val := int64(year)
			return legacyOctValue[int64](val)
		}
		val, err3 := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err3 == nil {
			return legacyOctValue[int64](val)
		}
		return types.Decimal128{}, moerr.NewInvalidArgNoCtx("function oct", s)
	}, selectList)
}

func legacyOctFloatValue[T constraints.Float](xs T) (types.Decimal128, error) {
	var res types.Decimal128

	if xs < 0 {
		val, err := strconv.ParseInt(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, moerr.NewInternalErrorNoCtx("the input value is out of integer range")
		}
		res, err = legacyOctValue(uint64(val))
		if err != nil {
			return res, err
		}
	} else {
		val, err := strconv.ParseUint(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, moerr.NewInternalErrorNoCtx("the input value is out of integer range")
		}
		res, err = legacyOctValue(val)
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

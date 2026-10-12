// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func bitwiseBinaryScalarResultType(left types.Type) types.Type {
	if left.Oid == types.T_blob {
		left.Charset = types.CharsetBinary
		return left
	}
	return bitwiseBinaryReturnType([]types.Type{left})
}

func bitwiseBinaryRowSkipped(selectList *FunctionSelectList, row uint64) bool {
	return selectList != nil && (selectList.IgnoreAllRow() ||
		(!selectList.ShouldEvalAllRow() && selectList.Contains(row)))
}

func operatorOpBitwiseBinaryNotFn(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	p := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	constantResult := result.GetResultVector().IsConst()
	if selectList != nil && selectList.IgnoreAllRow() {
		rs.SetNullResult(uint64(length))
		return nil
	}

	var scratch []byte
	var sourceForWriter []byte
	writer := func(dst []byte) error {
		for i, b := range sourceForWriter {
			dst[i] = ^b
		}
		return nil
	}
	for row := uint64(0); row < uint64(length); row++ {
		if bitwiseBinaryRowSkipped(selectList, row) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		src, isNull := p.GetStrValue(row)
		if isNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if constantResult {
			if cap(scratch) < len(src) {
				scratch = make([]byte, len(src))
			} else {
				scratch = scratch[:len(src)]
			}
			for i, b := range src {
				scratch[i] = ^b
			}
			if err := rs.AppendBytes(scratch, false); err != nil {
				return err
			}
		} else {
			sourceForWriter = src
			if err := rs.AppendBytesWithWriter(len(src), writer); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorOpBitShiftLeftBinaryInt64Fn(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	return operatorOpBitShiftBinary[int64](parameters, result, proc, length, selectList, shiftBinaryLeft)
}

func operatorOpBitShiftLeftBinaryUint64Fn(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	return operatorOpBitShiftBinary[uint64](parameters, result, proc, length, selectList, shiftBinaryLeft)
}

func operatorOpBitShiftRightBinaryInt64Fn(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	return operatorOpBitShiftBinary[int64](parameters, result, proc, length, selectList, shiftBinaryRight)
}

func operatorOpBitShiftRightBinaryUint64Fn(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	return operatorOpBitShiftBinary[uint64](parameters, result, proc, length, selectList, shiftBinaryRight)
}

func operatorOpBitShiftBinary[T int64 | uint64](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	selectList *FunctionSelectList,
	shift func(src, dst []byte, count uint64),
) error {
	p := vector.GenerateFunctionStrParameter(parameters[0])
	countParam := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	constantResult := result.GetResultVector().IsConst()
	if selectList != nil && selectList.IgnoreAllRow() {
		rs.SetNullResult(uint64(length))
		return nil
	}

	var scratch []byte
	var sourceForWriter []byte
	var countForWriter uint64
	writer := func(dst []byte) error {
		shift(sourceForWriter, dst, countForWriter)
		return nil
	}
	for row := uint64(0); row < uint64(length); row++ {
		if bitwiseBinaryRowSkipped(selectList, row) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		src, isNull := p.GetStrValue(row)
		if isNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		count, countIsNull := countParam.GetValue(row)
		if countIsNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		shiftCount := uint64(count)
		if constantResult {
			if cap(scratch) < len(src) {
				scratch = make([]byte, len(src))
			} else {
				scratch = scratch[:len(src)]
			}
			shift(src, scratch, shiftCount)
			if err := rs.AppendBytes(scratch, false); err != nil {
				return err
			}
		} else {
			sourceForWriter = src
			countForWriter = shiftCount
			if err := rs.AppendBytesWithWriter(len(src), writer); err != nil {
				return err
			}
		}
	}
	return nil
}

func shiftBinaryLeft(src, dst []byte, count uint64) {
	byteOffset := count / 8
	if byteOffset >= uint64(len(src)) {
		clear(dst)
		return
	}

	wholeBytes := int(byteOffset)
	remainingBits := uint(count % 8)
	activeBytes := len(src) - wholeBytes
	for out := 0; out < activeBytes; out++ {
		in := out + wholeBytes
		value := src[in] << remainingBits
		if remainingBits != 0 && in+1 < len(src) {
			value |= src[in+1] >> (8 - remainingBits)
		}
		dst[out] = value
	}
	clear(dst[activeBytes:])
}

func shiftBinaryRight(src, dst []byte, count uint64) {
	byteOffset := count / 8
	if byteOffset >= uint64(len(src)) {
		clear(dst)
		return
	}

	wholeBytes := int(byteOffset)
	remainingBits := uint(count % 8)
	clear(dst[:wholeBytes])
	for out := wholeBytes; out < len(src); out++ {
		in := out - wholeBytes
		value := src[in] >> remainingBits
		if remainingBits != 0 && in > 0 {
			value |= src[in-1] << (8 - remainingBits)
		}
		dst[out] = value
	}
}

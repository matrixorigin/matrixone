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

package bin

import (
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

var (
	Int8BitLen  = bitLen[int8]
	Int16BitLen = bitLen[int16]
	Int32BitLen = bitLen[int32]
	Int64BitLen = bitLen[int64]

	Uint8BitLen  = bitLen[uint8]
	Uint16BitLen = bitLen[uint16]
	Uint32BitLen = bitLen[uint32]
	Uint64BitLen = bitLen[uint64]

	Float32BitLen = bitLen[float32]
	Float64BitLen = bitLen[float64]

	Int8ToBinary  = toBinary[int8]
	Int16ToBinary = toBinary[int16]
	Int32ToBinary = toBinary[int32]
	Int64ToBinary = toBinary[int64]

	Uint8ToBinary  = toBinary[uint8]
	Uint16ToBinary = toBinary[uint16]
	Uint32ToBinary = toBinary[uint32]
	Uint64ToBinary = toBinary[uint64]

	Float32ToBinary = toBinary[float32]
	Float64ToBinary = toBinary[float64]
)

func bitLen[T constraints.Integer | constraints.Float](xs []T) int64 {
	var r int64
	for _, x := range xs {
		// convert to uint64 to count bits
		val := uint64(x)
		if val == 0 {
			// special case for zero
			r += 1
		} else {
			r += int64(bits.Len64(val))
		}
	}
	return r
}

func toBinary[T constraints.Integer | constraints.Float](xs []T, result []string) []string {
	for i, x := range xs {
		result[i] = uintToBinary(uint64(x))
	}
	return result
}

func uintToBinary(x uint64) string {
	if x == 0 {
		return "0"
	}
	b, i := [64]byte{}, 63
	for x > 0 {
		if x&1 == 1 {
			b[i] = '1'
		} else {
			b[i] = '0'
		}
		x >>= 1
		i -= 1
	}

	return string(b[i+1:])
}

func Bin[T constraints.Unsigned | constraints.Signed](intputVector, resultVector *vector.Vector, proc *process.Process) error {
	xs := vector.MustTCols[T](intputVector)
	rs := make([]string, len(xs))
	for idx := range xs {
		res := uintToBinary(uint64(xs[idx]))
		rs[idx] = res
	}
	vector.AppendString(resultVector, rs, proc.Mp())
	return nil
}

func BinFloat[T constraints.Float](intputVector, resultVector *vector.Vector, proc *process.Process) error {
	xs := vector.MustTCols[T](intputVector)
	err := binary.NumericToNumericOverflow(proc.Ctx, xs, []int64{})
	if err != nil {
		return err
	}
	rs := make([]string, len(xs))
	for idx, v := range xs {
		rs[idx] = uintToBinary(uint64(int64(v)))
	}
	vector.AppendString(resultVector, rs, proc.Mp())
	return nil
}

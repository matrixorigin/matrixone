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
	"math"
	"math/big"
	"math/bits"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int8BitLen  = intBitLen[int8]
	Int16BitLen = intBitLen[int16]
	Int32BitLen = intBitLen[int32]
	Int64BitLen = intBitLen[int64]

	Uint8BitLen  = intBitLen[uint8]
	Uint16BitLen = intBitLen[uint16]
	Uint32BitLen = intBitLen[uint32]
	Uint64BitLen = intBitLen[uint64]

	Float32BitLen = floatBitLen[float32]
	Float64BitLen = floatBitLen[float64]

	Int8ToBinary  = intToBinary[int8]
	Int16ToBinary = intToBinary[int16]
	Int32ToBinary = intToBinary[int32]
	Int64ToBinary = intToBinary[int64]

	Uint8ToBinary  = intToBinary[uint8]
	Uint16ToBinary = intToBinary[uint16]
	Uint32ToBinary = intToBinary[uint32]
	Uint64ToBinary = intToBinary[uint64]

	Float32ToBinary = floatToBinary[float32]
	Float64ToBinary = floatToBinary[float64]
)

func intBitLen[T constraints.Integer](xs []T) int64 {
	var r int64
	for _, x := range xs {
		if x == 0 {
			// special case for zero
			r += 1
		} else if x > 0 {
			r += int64(bits.Len64(uint64(x)))
		} else {
			// calc two's complement for negative number to improve the compatibility between arm64 and x86 platforms
			n := big.NewInt(int64(x))
			mask := big.NewInt(1)
			mask.Lsh(mask, 64)
			n.Add(n, mask)

			r += int64(n.BitLen())
		}
	}
	return r
}

func floatBitLen[T constraints.Float](xs []T) int64 {
	var r int64
	for _, x := range xs {
		v := math.Floor(float64(x))
		if v == 0 {
			// special case for zero
			r += 1
		} else if v > 0 {
			// truncate float to uint64
			r += int64(bits.Len64(uint64(v)))
		} else if math.Abs(v) >= math.MaxUint64 {
			// bits string produced by bin function should not exceeded 64 bits
			r += 64
		} else {
			// calc two's complement for negative number to improve the compatibility between arm64 and x86 platforms
			// convert to big int instead int64 to prevent truncating
			floatStr := strconv.FormatFloat(v, 'f', -1, 64)
			n := new(big.Int)
			n.SetString(floatStr, 10)
			mask := big.NewInt(1)
			mask.Lsh(mask, 64)
			n.Add(n, mask)

			r += int64(n.BitLen())
		}
	}
	return r
}

func intToBinary[T constraints.Integer](xs []T, result *types.Bytes) *types.Bytes {
	offset := 0
	var binStr string
	for i, x := range xs {
		if x >= 0 {
			binStr = uintToBinStr(uint64(x))
		} else {
			binStr = negativeToBinStr(int64(x))
		}
		copy(result.Data[offset:], binStr)
		result.Lengths[i] = uint32(len(binStr))
		result.Offsets[i] = uint32(offset)
		offset += len(binStr)
	}
	return result
}

func floatToBinary[T constraints.Float](xs []T, result *types.Bytes) *types.Bytes {
	offset := 0
	var binStr string
	for i, x := range xs {
		binStr = floatToBinStr(float64(x))
		copy(result.Data[offset:], binStr)
		result.Lengths[i] = uint32(len(binStr))
		result.Offsets[i] = uint32(offset)
		offset += len(binStr)
	}
	return result
}

func floatToBinStr(x float64) string {
	v := math.Floor(x)
	if v == 0 {
		return "0"
	} else if v > 0 {
		// truncate float to uint64
		return uintToBinStr(uint64(v))
	} else if math.Abs(v) >= math.MaxUint64 {
		b := [64]byte{}
		for i := 0; i < 64; i++ {
			b[i] = '1'
		}
		return string(b[:])
	} else {
		// calc two's complement for negative number to improve the compatibility between arm64 and x86 platforms
		// convert big int instead int64 to prevent truncating
		floatStr := strconv.FormatFloat(v, 'f', -1, 64)
		n := new(big.Int)
		n.SetString(floatStr, 10)
		mask := big.NewInt(1)
		mask.Lsh(mask, 64)
		n.Add(n, mask)

		b, i := [64]byte{}, 63
		d := n.Bits()[0]
		for d > 0 {
			if d&1 == 1 {
				b[i] = '1'
			} else {
				b[i] = '0'
			}
			d >>= 1
			i--
		}
		return string(b[i+1:])
	}
}

func negativeToBinStr(x int64) string {
	n := big.NewInt(x)
	mask := big.NewInt(1)
	mask.Lsh(mask, 64)
	n.Add(n, mask)

	b, i := [64]byte{}, 63
	d := n.Bits()[0]
	for d > 0 {
		if d&1 == 1 {
			b[i] = '1'
		} else {
			b[i] = '0'
		}
		d >>= 1
		i--
	}
	return string(b[i+1:])
}

func uintToBinStr(x uint64) string {
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
		i--
	}

	return string(b[i+1:])
}

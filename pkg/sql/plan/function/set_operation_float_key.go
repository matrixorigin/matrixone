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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Set membership refines the ordinary SQL equality key only for stored signed
// zero. Keep NaN and scale-normalized nonzero peers in their existing domain.
// In particular, a tiny negative FLOAT rounded to zero must not acquire the
// special key reserved for an actual negative zero.
func setOperationFloat32Key(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	codec := keycodec.NewFloat32Codec(parameters[0].GetType().Scale)
	return opUnaryFixedToFixed[float32, uint64](parameters, result, proc, length, func(value float32) uint64 {
		if bits := math.Float32bits(value); bits == 1<<31 {
			return uint64(bits)
		}
		return uint64(codec.CanonicalBits(value))
	}, selectList)
}

func setOperationFloat64Key(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[float64, uint64](parameters, result, proc, length, func(value float64) uint64 {
		if bits := math.Float64bits(value); bits == 1<<63 {
			return bits
		}
		return keycodec.CanonicalFloat64Bits(value)
	}, selectList)
}

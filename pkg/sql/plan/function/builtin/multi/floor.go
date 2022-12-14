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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// floor function's evaluation for arguments: [uint64]
func FloorUInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("floor", vecs, proc, floor.FloorUint64)
}

// floor function's evaluation for arguments: [int64]
func FloorInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("floor", vecs, proc, floor.FloorInt64)
}

// floor function's evaluation for arguments: [float64]
func FloorFloat64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("floor", vecs, proc, floor.FloorFloat64)
}

func FloorDecimal128(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	scale := vecs[0].GetType().Scale
	cb := func(vs []types.Decimal128, rs []types.Decimal128, digits int64) []types.Decimal128 {
		return floor.FloorDecimal128(scale, vs, rs, digits)
	}
	return generalMathMulti("floor", vecs, proc, cb)
}

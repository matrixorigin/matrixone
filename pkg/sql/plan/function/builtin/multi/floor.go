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
	"strconv"

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

// Parse string to float instead of int.
func FloorStr(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	values := vector.MustStrCol(vecs[0])
	floatvector, err := proc.AllocVectorOfRows(types.T_float64.ToType(), 0, nil)
	if err != nil {
		return nil, err
	}
	for _, v := range values {
		float, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, err
		}
		if err := vector.AppendFixed(floatvector, float, false, proc.Mp()); err != nil {
			floatvector.Free(proc.Mp())
			return nil, err
		}
	}
	newvecs := make([]*vector.Vector, 0)
	newvecs = append(newvecs, floatvector)
	return FloorFloat64(newvecs, proc)
}

// floor function's evaluation for arguments: [float64]
func FloorFloat64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("floor", vecs, proc, floor.FloorFloat64)
}

func FloorDecimal64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	scale := vecs[0].GetType().Scale
	cb := func(vs []types.Decimal64, rs []types.Decimal64, digits int64) []types.Decimal64 {
		return floor.FloorDecimal64(vs, rs, digits, scale)
	}
	return generalMathMulti("floor", vecs, proc, cb)
}

func FloorDecimal128(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	scale := vecs[0].GetType().Scale
	cb := func(vs []types.Decimal128, rs []types.Decimal128, digits int64) []types.Decimal128 {
		return floor.FloorDecimal128(vs, rs, digits, scale)
	}
	return generalMathMulti("floor", vecs, proc, cb)
}

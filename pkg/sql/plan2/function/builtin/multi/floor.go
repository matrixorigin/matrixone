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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// floor function's evaluation for arguments: [uint64]
func FdsFloorUInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vecs[0].Col.([]uint64)
	vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_uint64, Size: 8})
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeUint64Slice(vec.Data)
	rs = rs[:len(vs)]
	vec.Col = rs
	nulls.Set(vec.Nsp, vecs[0].Nsp)
	vector.SetCol(vec, floor.FloorUint64(vs, rs, digits))
	return vec, nil
}

// floor function's evaluation for arguments: [uint64, int64]
func FdsFloorUInt64Int64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vecs[0].Col.([]uint64)
	if !vecs[1].IsConstant() || vecs[1].Typ.Oid != types.T_int64 {
		return nil, errors.New("the second argument of the round function must be an int64 constant")
	}
	digits = vecs[1].Col.([]int64)[0]
	vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_uint64, Size: 8})
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeUint64Slice(vec.Data)
	rs = rs[:len(vs)]
	vec.Col = rs
	nulls.Set(vec.Nsp, vecs[0].Nsp)
	vector.SetCol(vec, floor.FloorUint64(vs, rs, digits))
	return vec, nil
}

// floor function's evaluation for arguments: [int64]
func FdsFloorInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vecs[0].Col.([]int64)
	vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_int64, Size: 8})
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeInt64Slice(vec.Data)
	rs = rs[:len(vs)]
	vec.Col = rs
	nulls.Set(vec.Nsp, vecs[0].Nsp)
	vector.SetCol(vec, floor.FloorInt64(vs, rs, digits))
	return vec, nil
}

// floor function's evaluation for arguments: [int64, int64]
func FdsFloorInt64Int64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vecs[0].Col.([]int64)
	if !vecs[1].IsConstant() || vecs[1].Typ.Oid != types.T_int64 {
		return nil, errors.New("the second argument of the round function must be an int64 constant")
	}
	digits = vecs[1].Col.([]int64)[0]
	vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_int64, Size: 8})
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeInt64Slice(vec.Data)
	rs = rs[:len(vs)]
	vec.Col = rs
	nulls.Set(vec.Nsp, vecs[0].Nsp)
	vector.SetCol(vec, floor.FloorInt64(vs, rs, digits))
	return vec, nil
}

// floor function's evaluation for arguments: [float64]
func FdsFloorFloat64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vecs[0].Col.([]float64)
	vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_float64, Size: 8})
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFloat64Slice(vec.Data)
	rs = rs[:len(vs)]
	vec.Col = rs
	nulls.Set(vec.Nsp, vecs[0].Nsp)
	vector.SetCol(vec, floor.FloorFloat64(vs, rs, digits))
	return vec, nil
}

// floor function's evaluation for arguments: [float64, int64]
func FdsFloorFloat64Int64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vecs[0].Col.([]float64)
	if !vecs[1].IsConstant() || vecs[1].Typ.Oid != types.T_int64 {
		return nil, errors.New("the second argument of the round function must be an int64 constant")
	}
	digits = vecs[1].Col.([]int64)[0]
	vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_float64, Size: 8})
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeFloat64Slice(vec.Data)
	rs = rs[:len(vs)]
	vec.Col = rs
	nulls.Set(vec.Nsp, vecs[0].Nsp)
	vector.SetCol(vec, floor.FloorFloat64(vs, rs, digits))
	return vec, nil
}

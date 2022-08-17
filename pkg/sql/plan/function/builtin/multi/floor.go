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
	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// floor function's evaluation for arguments: [uint64]
func FloorUInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vector.MustTCols[uint64](vecs[0])
	if vecs[0].IsScalar() {
		if vecs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_uint64, Size: 8}), nil
		}
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_uint64, Size: 8})
		rs := make([]uint64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorUint64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_uint64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeUint64Slice(vec.Data)
		rs = rs[:len(vs)]
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorUint64(vs, rs, digits))
		return vec, nil
	}
}

// floor function's evaluation for arguments: [int64]
func FloorInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vector.MustTCols[int64](vecs[0])
	if vecs[0].IsScalar() {
		if vecs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: 8}), nil
		}
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_int64, Size: 8})
		rs := make([]int64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorInt64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_int64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeInt64Slice(vec.Data)
		rs = rs[:len(vs)]
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorInt64(vs, rs, digits))
		return vec, nil
	}
}

// floor function's evaluation for arguments: [float64]
func FloorFloat64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vector.MustTCols[float64](vecs[0])
	if vecs[0].IsScalar() {
		if vecs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
		}
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_float64, Size: 8})
		rs := make([]float64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorFloat64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_float64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeFloat64Slice(vec.Data)
		rs = rs[:len(vs)]
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorFloat64(vs, rs, digits))
		return vec, nil
	}
}

// floor function's evaluation for arguments: [uint64, int64]
func FloorUInt64Int64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vector.MustTCols[uint64](vecs[0])
	// if the second paramter is null ,show error
	if !vecs[1].IsScalar() || vecs[1].Typ.Oid != types.T_int64 || vecs[1].IsScalarNull() {
		return nil, errors.New("the second argument of the floor function must be an int64 constant")
	}
	digits = vecs[1].Col.([]int64)[0]
	if vecs[0].IsScalar() {
		if vecs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_uint64, Size: 8}), nil
		}
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_uint64, Size: 8})
		rs := make([]uint64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorUint64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_uint64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeUint64Slice(vec.Data)
		rs = rs[:len(vs)]
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorUint64(vs, rs, digits))
		return vec, nil
	}
}

// floor function's evaluation for arguments: [int64, int64]
func FloorInt64Int64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vector.MustTCols[int64](vecs[0])
	// if the second paramter is null ,show error
	if !vecs[1].IsScalar() || vecs[1].Typ.Oid != types.T_int64 || vecs[1].IsScalarNull() {
		return nil, errors.New("the second argument of the floor function must be an int64 constant")
	}
	digits = vecs[1].Col.([]int64)[0]
	if vecs[0].IsScalar() {
		if vecs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: 8}), nil
		}
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_int64, Size: 8})
		rs := make([]int64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorInt64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_int64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeInt64Slice(vec.Data)
		rs = rs[:len(vs)]
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorInt64(vs, rs, digits))
		return vec, nil
	}
}

// floor function's evaluation for arguments: [float64, int64]
func FloorFloat64Int64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vector.MustTCols[float64](vecs[0])

	if !vecs[1].IsScalar() || vecs[1].Typ.Oid != types.T_int64 || vecs[1].IsScalarNull() {
		return nil, errors.New("the second argument of the floor function must be an int64 constant")
	}
	digits = vecs[1].Col.([]int64)[0]

	if vecs[0].IsScalar() {
		if vecs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
		}
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_float64, Size: 8})
		rs := make([]float64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorFloat64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_float64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeFloat64Slice(vec.Data)
		rs = rs[:len(vs)]
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorFloat64(vs, rs, digits))
		return vec, nil
	}
}

func FloorDecimal128(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	digits := int64(0)
	vs := vector.MustTCols[types.Decimal128](vecs[0])
	if vecs[0].IsScalar() {
		if vecs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_decimal128, Size: 16}), nil
		}
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_decimal128, Size: 16})
		rs := make([]types.Decimal128, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorDecimal128(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_decimal128, Size: 16}, 16*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeDecimal128Slice(vec.Data)
		rs = rs[:len(vs)]
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, floor.FloorDecimal128(vs, rs, digits))
		return vec, nil
	}
}

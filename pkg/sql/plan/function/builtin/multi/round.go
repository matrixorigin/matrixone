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

package multi

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/round"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RoundUint64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_uint64, Size: 8}), nil
	}
	digits := int64(0)
	vs := vector.MustTCols[uint64](vecs[0])
	if len(vecs) > 1 {
		if vecs[1].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_uint64, Size: 8}), nil
		}
		if !vecs[1].IsScalar() || vecs[1].Typ.Oid != types.T_int64 {
			return nil, errors.New("the second argument of the round function must be an int64 constant")
		}
		digits = vector.MustTCols[int64](vecs[1])[0]
	}

	if vecs[0].IsScalar() {
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_uint64, Size: 8})
		rs := make([]uint64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, round.RoundUint64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_uint64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeUint64Slice(vec.Data)
		rs = rs[:len(vs)]
		vec.Col = rs
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, round.RoundUint64(vs, rs, digits))
		return vec, nil
	}
}

func RoundInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: 8}), nil
	}
	digits := int64(0)
	vs := vector.MustTCols[int64](vecs[0])
	if len(vecs) > 1 {
		if vecs[1].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: 8}), nil
		}
		if !vecs[1].IsScalar() || vecs[1].Typ.Oid != types.T_int64 {
			return nil, errors.New("the second argument of the round function must be an int64 constant")
		}
		digits = vector.MustTCols[int64](vecs[1])[0]
	}
	if vecs[0].IsScalar() {
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_int64, Size: 8})
		rs := make([]int64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, round.RoundInt64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_int64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeInt64Slice(vec.Data)
		rs = rs[:len(vs)]
		vec.Col = rs
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, round.RoundInt64(vs, rs, digits))
		return vec, nil
	}
}

func RoundFloat64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
	}
	digits := int64(0)
	vs := vector.MustTCols[float64](vecs[0])
	if len(vecs) > 1 {
		if vecs[1].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
		}
		if !vecs[1].IsScalar() || vecs[1].Typ.Oid != types.T_int64 {
			return nil, errors.New("the second argument of the round function must be an int64 constant")
		}
		digits = vector.MustTCols[int64](vecs[1])[0]
	}

	if vecs[0].IsScalar() {
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_float64, Size: 8})
		rs := make([]float64, 1)
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, round.RoundFloat64(vs, rs, digits))
		return vec, nil
	} else {
		vec, err := proc.AllocVector(types.Type{Oid: types.T_float64, Size: 8}, 8*int64(len(vs)))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeFloat64Slice(vec.Data)
		rs = rs[:len(vs)]
		vec.Col = rs
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		vector.SetCol(vec, round.RoundFloat64(vs, rs, digits))
		return vec, nil
	}
}

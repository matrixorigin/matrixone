// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/div"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func IntegerDiv[T constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := lv.Col.([]T), rv.Col.([]T)
	rtl := types.T_int64.FixedLength()

	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_int64, Size: 8}), nil
	}

	switch {
	case lv.IsScalar() && rv.IsScalar():
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_int64, Size: 8})
		rs := make([]int64, 1)
		nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
		if !nulls.Any(rv.Nsp) {
			for _, v := range rvs {
				if v == 0 {
					return nil, ErrDivByZero
				}
			}
			vector.SetCol(vec, div.FloatIntegerDiv(lvs, rvs, rs))
			return vec, nil
		}
		sels := process.GetSels(proc)
		defer process.PutSels(sels, proc)
		for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
			if nulls.Contains(rv.Nsp, i) {
				continue
			}
			if rvs[i] == 0 {
				return nil, ErrDivByZero
			}
			sels = append(sels, int64(i))
		}
		vector.SetCol(vec, div.FloatIntegerDivSels(lvs, rvs, rs, sels))
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(types.Type{Oid: types.T_int64, Size: 8}, int64(rtl)*int64(len(rvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeInt64Slice(vec.Data)
		rs = rs[:len(rvs)]

		if !nulls.Any(rv.Nsp) {
			for _, v := range rvs {
				if v == 0 {
					return nil, ErrDivByZero
				}
			}
			nulls.Set(vec.Nsp, rv.Nsp)
			vector.SetCol(vec, div.FloatIntegerDivScalar(lvs[0], rvs, rs))
			return vec, nil
		}
		sels := process.GetSels(proc)
		defer process.PutSels(sels, proc)
		for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
			if nulls.Contains(rv.Nsp, i) {
				continue
			}
			if rvs[i] == 0 {
				return nil, ErrDivByZero
			}
			sels = append(sels, int64(i))
		}
		vector.SetCol(vec, div.FloatIntegerDivSels(lvs, rvs, rs, sels))
		nulls.Set(vec.Nsp, lv.Nsp.Or(rv.Nsp))
		return vec, nil
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(types.Type{Oid: types.T_int64, Size: 8}, int64(rtl)*int64(len(lvs)))
		if err != nil {
			return nil, err
		}
		rs := encoding.DecodeInt64Slice(vec.Data)
		rs = rs[:len(lvs)]

		if rvs[0] == 0 {
			return nil, ErrDivByZero
		}
		vector.SetCol(vec, div.FloatIntegerDivByScalar(rvs[0], lvs, rs))
		nulls.Set(vec.Nsp, lv.Nsp)
		return vec, nil
	}
	vec, err := proc.AllocVector(types.Type{Oid: types.T_int64, Size: 8}, int64(rtl)*int64(len(rvs)))
	if err != nil {
		return nil, err
	}
	rs := encoding.DecodeInt64Slice(vec.Data)
	rs = rs[:len(rvs)]

	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	if !nulls.Any(rv.Nsp) {
		for _, v := range rvs {
			if v == 0 {
				return nil, ErrDivByZero
			}
		}
		vector.SetCol(vec, div.FloatIntegerDiv(lvs, rvs, rs))
		return vec, nil
	}
	sels := process.GetSels(proc)
	defer process.PutSels(sels, proc)
	for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
		if nulls.Contains(rv.Nsp, i) {
			continue
		}
		if rvs[i] == 0 {
			return nil, ErrDivByZero
		}
		sels = append(sels, int64(i))
	}
	vector.SetCol(vec, div.FloatIntegerDivSels(lvs, rvs, rs, sels))
	return vec, nil
}

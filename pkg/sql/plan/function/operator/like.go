// Copyright 2022 Matrix Origin
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

package operator

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/like"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	errUnexpected = errors.New("unexpected case for LIKE operator")
)

func Like(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustBytesCols(lv), vector.MustBytesCols(rv)
	rtl := 8

	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocScalarNullVector(types.Type{Oid: types.T_bool}), nil
	}

	switch {
	case !lv.IsScalar() && rv.IsScalar():
		vec, err := proc.AllocVector(types.Type{Oid: types.T_bool}, int64(len(lvs.Offsets)*rtl))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeInt64Slice(vec.Data)
		rs = rs[:len(lvs.Lengths)]
		if nulls.Any(lv.Nsp) {
			rs, err = like.BtSliceNullAndConst(lvs, rvs.Get(0), lv.Nsp.Np, rs)
			if err != nil {
				return nil, err
			}
			vec.Nsp = lv.Nsp
		} else {
			rs, err = like.BtSliceAndConst(lvs, rvs.Get(0), rs)
			if err != nil {
				return nil, err
			}
		}
		col := make([]bool, len(lvs.Offsets))
		rsi := 0
		for i := 0; i < len(col); i++ {
			if rsi >= len(rs) {
				break
			}
			if int64(i) == rs[rsi] {
				col[i] = true
				rsi++
			} else {
				col[i] = false
			}
		}
		vector.SetCol(vec, col)
		return vec, nil
	case lv.IsScalar() && rv.IsScalar(): // in our design, this case should deal while pruning extends.
		vec := proc.AllocScalarVector(types.Type{Oid: types.T_bool})
		rs := make([]int64, 1)
		rs, err := like.BtConstAndConst(lvs.Get(0), rvs.Get(0), rs)
		if err != nil {
			return nil, err
		}
		col := make([]bool, 1)
		if rs == nil {
			col[0] = false
		} else {
			col[0] = rs[0] == int64(0)
		}
		vector.SetCol(vec, col)
		return vec, nil
	case lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(types.Type{Oid: types.T_bool}, int64(len(rvs.Offsets)*rtl))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeInt64Slice(vec.Data)
		rs = rs[:len(rvs.Lengths)]
		if nulls.Any(rv.Nsp) {
			rs, err = like.BtConstAndSliceNull(lvs.Get(0), rvs, rv.Nsp.Np, rs)
			if err != nil {
				return nil, err
			}
			vec.Nsp = rv.Nsp
		} else {
			rs, err = like.BtConstAndSlice(lvs.Get(0), rvs, rs)
			if err != nil {
				return nil, err
			}
		}
		col := make([]bool, len(rvs.Offsets))
		rsi := 0
		for i := 0; i < len(col); i++ {
			if rsi >= len(rs) {
				break
			}
			if int64(i) == rs[rsi] {
				col[i] = true
				rsi++
			} else {
				col[i] = false
			}
		}
		vector.SetCol(vec, col)
		return vec, nil
	case !lv.IsScalar() && !rv.IsScalar():
		vec, err := proc.AllocVector(types.Type{Oid: types.T_bool}, int64(len(lvs.Offsets)*rtl))
		if err != nil {
			return nil, err
		}
		rs := types.DecodeInt64Slice(vec.Data)
		rs = rs[:len(rvs.Lengths)]
		if nulls.Any(rv.Nsp) && nulls.Any(lv.Nsp) {
			nsp := lv.Nsp.Or(rv.Nsp)
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp.Np, rs)
			if err != nil {
				return nil, err
			}
			vec.Nsp = nsp
		} else if nulls.Any(rv.Nsp) && !nulls.Any(lv.Nsp) {
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, rv.Nsp.Np, rs)
			if err != nil {
				return nil, err
			}
			vec.Nsp = rv.Nsp
		} else if !nulls.Any(rv.Nsp) && nulls.Any(lv.Nsp) {
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, lv.Nsp.Np, rs)
			if err != nil {
				return nil, err
			}
			//vector.SetCol(vec, rs)
			vec.Nsp = lv.Nsp
		} else {
			rs, err = like.BtSliceAndSlice(lvs, rvs, rs)
			if err != nil {
				return nil, err
			}
		}
		col := make([]bool, len(lvs.Offsets))
		rsi := 0
		for i := 0; i < len(col); i++ {
			if rsi >= len(rs) {
				break
			}
			if int64(i) == rs[rsi] {
				col[i] = true
				rsi++
			} else {
				col[i] = false
			}
		}
		vector.SetCol(vec, col)
		return vec, nil
	}
	return nil, errUnexpected
}

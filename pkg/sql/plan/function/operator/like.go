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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/like"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Like(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := ivecs[0], ivecs[1]
	lvs, rvs := vector.MustStrCol(lv), vector.MustBytesCol(rv)
	rtyp := types.T_bool.ToType()

	if lv.IsConstNull() || rv.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	var err error
	rs := make([]bool, lv.Length())

	switch {
	case !lv.IsConst() && rv.IsConst():
		if nulls.Any(lv.GetNulls()) {
			rs, err = like.BtSliceNullAndConst(lvs, rvs[0], lv.GetNulls(), rs)
			if err != nil {
				return nil, err
			}
		} else {
			rs, err = like.BtSliceAndConst(lvs, rvs[0], rs)
			if err != nil {
				return nil, err
			}
		}
		vec := vector.NewVec(rtyp)
		vector.AppendFixedList(vec, rs, nil, proc.Mp())
		return vec, nil
	case lv.IsConst() && rv.IsConst(): // in our design, this case should deal while pruning extends.
		ok, err := like.BtConstAndConst(lvs[0], rvs[0])
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, ok, ivecs[0].Length(), proc.Mp()), nil
	case lv.IsConst() && !rv.IsConst():
		rs, err = like.BtConstAndSliceNull(lvs[0], rvs, rv.GetNulls(), rs)
		if err != nil {
			return nil, err
		}
		vec := vector.NewVec(rtyp)
		vector.AppendFixedList(vec, rs, nil, proc.Mp())
		return vec, nil
	case !lv.IsConst() && !rv.IsConst():
		var nsp *nulls.Nulls
		if nulls.Any(rv.GetNulls()) && nulls.Any(lv.GetNulls()) {
			nulls.Or(lv.GetNulls(), rv.GetNulls(), nsp)
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if nulls.Any(rv.GetNulls()) && !nulls.Any(lv.GetNulls()) {
			nsp = rv.GetNulls()
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if !nulls.Any(rv.GetNulls()) && nulls.Any(lv.GetNulls()) {
			nsp = lv.GetNulls()
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else {
			rs, err = like.BtSliceAndSlice(lvs, rvs, rs)
			if err != nil {
				return nil, err
			}
		}
		vec := vector.NewVec(rtyp)
		vector.AppendFixedList(vec, rs, nil, proc.Mp())
		vec.SetNulls(nsp)
		return vec, nil
	}
	return nil, moerr.NewInternalError(proc.Ctx, "unexpected case for LIKE operator")
}

func ILike(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := ivecs[0], ivecs[1]
	lvs, rvs := vector.MustStrCol(lv), vector.MustBytesCol(rv)
	for i := range lvs {
		lvs[i] = strings.ToLower(lvs[i])
	}
	for i := range rvs {
		rvs[i] = []byte(strings.ToLower(string(rvs[i])))
	}
	rtyp := types.T_bool.ToType()

	if lv.IsConstNull() || rv.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	var err error
	rs := make([]bool, lv.Length())

	switch {
	case !lv.IsConst() && rv.IsConst():
		if nulls.Any(lv.GetNulls()) {
			rs, err = like.BtSliceNullAndConst(lvs, rvs[0], lv.GetNulls(), rs)
			if err != nil {
				return nil, err
			}
		} else {
			rs, err = like.BtSliceAndConst(lvs, rvs[0], rs)
			if err != nil {
				return nil, err
			}
		}
		rv := vector.NewVec(rtyp)
		vector.AppendFixedList(rv, rs, nil, proc.Mp())
		nulls.Set(rv.GetNulls(), lv.GetNulls())
		return rv, nil
	case lv.IsConst() && rv.IsConst(): // in our design, this case should deal while pruning extends.
		ok, err := like.BtConstAndConst(lvs[0], rvs[0])
		if err != nil {
			return nil, err
		}
		rv := vector.NewConstFixed(rtyp, ok, lv.Length(), proc.Mp())
		return rv, nil
	case lv.IsConst() && !rv.IsConst():
		rs, err = like.BtConstAndSliceNull(lvs[0], rvs, rv.GetNulls(), rs)
		if err != nil {
			return nil, err
		}
		rv := vector.NewVec(rtyp)
		vector.AppendFixedList(rv, rs, nil, proc.Mp())
		nulls.Set(rv.GetNulls(), lv.GetNulls())
		return rv, nil
	case !lv.IsConst() && !rv.IsConst():
		var nsp *nulls.Nulls
		if nulls.Any(rv.GetNulls()) && nulls.Any(lv.GetNulls()) {
			nulls.Or(lv.GetNulls(), rv.GetNulls(), nsp)
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if nulls.Any(rv.GetNulls()) && !nulls.Any(lv.GetNulls()) {
			nsp = rv.GetNulls()
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if !nulls.Any(rv.GetNulls()) && nulls.Any(lv.GetNulls()) {
			nsp = lv.GetNulls()
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else {
			rs, err = like.BtSliceAndSlice(lvs, rvs, rs)
			if err != nil {
				return nil, err
			}
		}
		rv := vector.NewVec(rtyp)
		vector.AppendFixedList(rv, rs, nil, proc.Mp())
		rv.SetNulls(nsp)
		return rv, nil
	}
	return nil, moerr.NewInternalError(proc.Ctx, "unexpected case for LIKE operator")
}

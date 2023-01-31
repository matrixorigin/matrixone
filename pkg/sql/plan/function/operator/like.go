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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/like"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

func Like(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustStrCols(lv), vector.MustBytesCols(rv)
	rtyp := types.T_bool.ToType()

	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocConstNullVector(rtyp, lv.Length()), nil
	}

	var err error
	rs := make([]bool, lv.Length())

	switch {
	case !lv.IsScalar() && rv.IsScalar():
		if nulls.Any(lv.Nsp) {
			rs, err = like.BtSliceNullAndConst(lvs, rvs[0], lv.Nsp, rs)
			if err != nil {
				return nil, err
			}
		} else {
			rs, err = like.BtSliceAndConst(lvs, rvs[0], rs)
			if err != nil {
				return nil, err
			}
		}
		return vector.NewWithFixed(rtyp, rs, lv.Nsp, proc.Mp()), nil
	case lv.IsScalar() && rv.IsScalar(): // in our design, this case should deal while pruning extends.
		ok, err := like.BtConstAndConst(lvs[0], rvs[0])
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, lv.Length(), ok, proc.Mp()), nil
	case lv.IsScalar() && !rv.IsScalar():
		rs, err = like.BtConstAndSliceNull(lvs[0], rvs, rv.Nsp, rs)
		if err != nil {
			return nil, err
		}
		return vector.NewWithFixed(rtyp, rs, lv.Nsp, proc.Mp()), nil
	case !lv.IsScalar() && !rv.IsScalar():
		var nsp *nulls.Nulls
		if nulls.Any(rv.Nsp) && nulls.Any(lv.Nsp) {
			nulls.Or(lv.Nsp, rv.Nsp, nsp)
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if nulls.Any(rv.Nsp) && !nulls.Any(lv.Nsp) {
			nsp = rv.Nsp
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if !nulls.Any(rv.Nsp) && nulls.Any(lv.Nsp) {
			nsp = lv.Nsp
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
		return vector.NewWithFixed(rtyp, rs, nsp, proc.Mp()), nil
	}
	return nil, moerr.NewInternalError(proc.Ctx, "unexpected case for LIKE operator")
}

func ILike(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := vector.MustStrCols(lv), vector.MustBytesCols(rv)
	for i := range lvs {
		lvs[i] = strings.ToLower(lvs[i])
	}
	for i := range rvs {
		rvs[i] = []byte(strings.ToLower(string(rvs[i])))
	}
	rtyp := types.T_bool.ToType()

	if lv.IsScalarNull() || rv.IsScalarNull() {
		return proc.AllocConstNullVector(rtyp, lv.Length()), nil
	}

	var err error
	rs := make([]bool, lv.Length())

	switch {
	case !lv.IsScalar() && rv.IsScalar():
		if nulls.Any(lv.Nsp) {
			rs, err = like.BtSliceNullAndConst(lvs, rvs[0], lv.Nsp, rs)
			if err != nil {
				return nil, err
			}
		} else {
			rs, err = like.BtSliceAndConst(lvs, rvs[0], rs)
			if err != nil {
				return nil, err
			}
		}
		return vector.NewWithFixed(rtyp, rs, lv.Nsp, proc.Mp()), nil
	case lv.IsScalar() && rv.IsScalar(): // in our design, this case should deal while pruning extends.
		ok, err := like.BtConstAndConst(lvs[0], rvs[0])
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, lv.Length(), ok, proc.Mp()), nil
	case lv.IsScalar() && !rv.IsScalar():
		rs, err = like.BtConstAndSliceNull(lvs[0], rvs, rv.Nsp, rs)
		if err != nil {
			return nil, err
		}
		return vector.NewWithFixed(rtyp, rs, lv.Nsp, proc.Mp()), nil
	case !lv.IsScalar() && !rv.IsScalar():
		var nsp *nulls.Nulls
		if nulls.Any(rv.Nsp) && nulls.Any(lv.Nsp) {
			nulls.Or(lv.Nsp, rv.Nsp, nsp)
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if nulls.Any(rv.Nsp) && !nulls.Any(lv.Nsp) {
			nsp = rv.Nsp
			rs, err = like.BtSliceNullAndSliceNull(lvs, rvs, nsp, rs)
			if err != nil {
				return nil, err
			}
		} else if !nulls.Any(rv.Nsp) && nulls.Any(lv.Nsp) {
			nsp = lv.Nsp
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
		return vector.NewWithFixed(rtyp, rs, nsp, proc.Mp()), nil
	}
	return nil, moerr.NewInternalError(proc.Ctx, "unexpected case for LIKE operator")
}

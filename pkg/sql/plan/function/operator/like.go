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
	rtyp := types.T_bool.ToType()

	if lv.IsConstNull() || rv.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	lvs, rvs := vector.MustStrCol(lv), vector.MustBytesCol(rv)

	switch {
	case !lv.IsConst() && rv.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, lv.Length(), lv.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[bool](rvec)
		if nulls.Any(lv.GetNulls()) {
			_, err = like.BtSliceNullAndConst(lvs, rvs[0], lv.GetNulls(), rs)
			if err != nil {
				return nil, err
			}
		} else {
			_, err = like.BtSliceAndConst(lvs, rvs[0], rs)
			if err != nil {
				return nil, err
			}
		}
		return rvec, nil
	case lv.IsConst() && rv.IsConst(): // in our design, this case should deal while pruning extends.
		ok, err := like.BtConstAndConst(lvs[0], rvs[0])
		if err != nil {
			return nil, err
		}
		rv := vector.NewConstFixed(rtyp, ok, lv.Length(), proc.Mp())
		return rv, nil
	case lv.IsConst() && !rv.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, lv.Length(), rv.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[bool](rvec)
		_, err = like.BtConstAndSliceNull(lvs[0], rvs, rv.GetNulls(), rs)
		if err != nil {
			return nil, err
		}
		return rvec, nil
	case !lv.IsConst() && !rv.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, lv.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Or(lv.GetNulls(), rv.GetNulls(), rvec.GetNulls())
		rs := vector.MustFixedCol[bool](rvec)
		if nulls.Any(rvec.GetNulls()) {
			_, err = like.BtSliceNullAndSliceNull(lvs, rvs, rvec.GetNulls(), rs)
			if err != nil {
				return nil, err
			}
		} else {
			_, err = like.BtSliceAndSlice(lvs, rvs, rs)
			if err != nil {
				return nil, err
			}
		}
		return rvec, nil
	}
	return nil, moerr.NewInternalError(proc.Ctx, "unexpected case for LIKE operator")
}

func ILike(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := ivecs[0], ivecs[1]
	rtyp := types.T_bool.ToType()

	if lv.IsConstNull() || rv.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	lvs, rvs := vector.MustStrCol(lv), vector.MustBytesCol(rv)
	for i := range lvs {
		lvs[i] = strings.ToLower(lvs[i])
	}
	for i := range rvs {
		rvs[i] = []byte(strings.ToLower(string(rvs[i])))
	}

	switch {
	case !lv.IsConst() && rv.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, lv.Length(), lv.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[bool](rvec)
		if nulls.Any(lv.GetNulls()) {
			_, err = like.BtSliceNullAndConst(lvs, rvs[0], lv.GetNulls(), rs)
			if err != nil {
				return nil, err
			}
		} else {
			_, err = like.BtSliceAndConst(lvs, rvs[0], rs)
			if err != nil {
				return nil, err
			}
		}
		return rvec, nil
	case lv.IsConst() && rv.IsConst(): // in our design, this case should deal while pruning extends.
		ok, err := like.BtConstAndConst(lvs[0], rvs[0])
		if err != nil {
			return nil, err
		}
		rv := vector.NewConstFixed(rtyp, ok, lv.Length(), proc.Mp())
		return rv, nil
	case lv.IsConst() && !rv.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, lv.Length(), rv.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := vector.MustFixedCol[bool](rvec)
		_, err = like.BtConstAndSliceNull(lvs[0], rvs, rv.GetNulls(), rs)
		if err != nil {
			return nil, err
		}
		return rvec, nil
	case !lv.IsConst() && !rv.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, lv.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Or(lv.GetNulls(), rv.GetNulls(), rvec.GetNulls())
		rs := vector.MustFixedCol[bool](rvec)
		if nulls.Any(rvec.GetNulls()) {
			_, err = like.BtSliceNullAndSliceNull(lvs, rvs, rvec.GetNulls(), rs)
			if err != nil {
				return nil, err
			}
		} else {
			_, err = like.BtSliceAndSlice(lvs, rvs, rs)
			if err != nil {
				return nil, err
			}
		}
		return rvec, nil
	}
	return nil, moerr.NewInternalError(proc.Ctx, "unexpected case for LIKE operator")
}

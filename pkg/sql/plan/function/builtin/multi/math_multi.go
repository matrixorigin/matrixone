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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type mathMultiT interface {
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128
}

type mathMultiFun[T mathMultiT] func([]T, []T, int64) []T

func generalMathMulti[T mathMultiT](funName string, ivecs []*vector.Vector, proc *process.Process, cb mathMultiFun[T]) (*vector.Vector, error) {
	rtyp := ivecs[0].GetType()
	digits := int64(0)
	if len(ivecs) > 1 {
		// if vecs[1].IsConstNull() {
		// 	return proc.AllocScalarNullVector(typ), nil
		// }
		if !ivecs[1].IsConst() || ivecs[1].GetType().Oid != types.T_int64 {
			return nil, moerr.NewInvalidArg(proc.Ctx, fmt.Sprintf("the second argument of the %s", funName), "not const")
		}
		digits = vector.MustFixedCol[int64](ivecs[1])[0]
	}
	vs := vector.MustFixedCol[T](ivecs[0])
	if ivecs[0].IsConst() {
		if ivecs[0].IsConstNull() {
			return vector.NewConstNull(*rtyp, ivecs[0].Length(), proc.Mp()), nil
		}

		var rs [1]T
		ret_rs := cb(vs, rs[:], digits)
		return vector.NewConstFixed(*rtyp, ret_rs[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(*rtyp, len(vs), ivecs[0].GetNulls())
		if err != nil {
			return nil, err
		}

		rs := vector.MustFixedCol[T](rvec)
		new_rs := cb(vs, rs, digits)
		if &new_rs[0] == &vs[0] {
			copy(rs, vs)
		}

		return rvec, nil
	}
}

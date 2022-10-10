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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type mathMultiT interface {
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128
}

type mathMultiFun[T mathMultiT] func([]T, []T, int64) []T

func generalMathMulti[T mathMultiT](funName string, vecs []*vector.Vector, proc *process.Process, cb mathMultiFun[T]) (*vector.Vector, error) {
	typ := vecs[0].Typ.Oid.ToType()
	digits := int64(0)
	if len(vecs) > 1 {
		// if vecs[1].IsScalarNull() {
		// 	return proc.AllocScalarNullVector(typ), nil
		// }
		if !vecs[1].IsScalar() || vecs[1].Typ.Oid != types.T_int64 {
			return nil, moerr.NewInvalidArg(fmt.Sprintf("the second argument of the %s", funName), "not const")
		}
		digits = vecs[1].Col.([]int64)[0]
	}
	vs := vector.MustTCols[T](vecs[0])
	if vecs[0].IsScalarNull() {
		return proc.AllocScalarNullVector(typ), nil
	}

	if vecs[0].IsScalar() {
		rs := make([]T, 1)
		ret_rs := cb(vs, rs, digits)

		vec := vector.NewConstFixed(typ, 1, ret_rs[0], proc.Mp())
		nulls.Set(vec.Nsp, vecs[0].Nsp)
		return vec, nil
	} else {
		rs := make([]T, len(vs))
		ret_rs := cb(vs, rs, digits)

		vec := vector.NewWithFixed(typ, ret_rs, nulls.NewWithSize(len(ret_rs)), proc.Mp())
		nulls.Set(vec.Nsp, vecs[0].Nsp)

		return vec, nil
	}
}

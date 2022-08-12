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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type opBitT interface {
	constraints.Unsigned
}

type opBitFun[T opBitT] func(v1, v2 T) T

func opBitXor[T opBitT](v1, v2 T) T {
	return v1 ^ v2
}

func opBitOr[T opBitT](v1, v2 T) T {
	return v1 | v2
}

func opBitAnd[T opBitT](v1, v2 T) T {
	return v1 & v2
}

func OpBitAndFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return OpBitBase(args, proc, args[0].GetType(), opBitAnd[T])
}

func OpBitOrFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return OpBitBase(args, proc, args[0].GetType(), opBitOr[T])
}

func OpBitXorFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return OpBitBase(args, proc, args[0].GetType(), opBitXor[T])
}

func OpBitBase[T opBitT](vectors []*vector.Vector, proc *process.Process, typ types.Type, afn opBitFun[T]) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	leftValues, rightValues := vector.MustTCols[T](left), vector.MustTCols[T](right)

	if left.IsScalarNull() || right.IsScalarNull() {
		return proc.AllocScalarNullVector(typ), nil
	}

	if left.IsScalar() && right.IsScalar() {
		vec := proc.AllocScalarVector(typ)
		vec.Col = make([]T, 1)
		vec.Col.([]T)[0] = afn(left.Col.([]T)[0], right.Col.([]T)[0])
		return vec, nil
	}

	resultElementSize := typ.Oid.TypeLen()
	nEle := len(leftValues)
	if left.IsScalar() {
		nEle = len(rightValues)
	}

	resultVector, err := proc.AllocVector(typ, int64(resultElementSize*nEle))
	if err != nil {
		return nil, err
	}

	resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
	nulls.Or(left.Nsp, right.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, resultValues)

	goOpBitGeneral(left, right, resultVector, afn)

	return resultVector, nil
}

func goOpBitGeneral[T opBitT](xs, ys, rs *vector.Vector, bfn opBitFun[T]) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		if nulls.Any(ys.Nsp) {
			for i, y := range yt {
				if !nulls.Contains(rs.Nsp, uint64(i)) {
					rt[i] = bfn(xt[0], y)
				}
			}
		} else {
			for i, y := range yt {
				rt[i] = bfn(xt[0], y)
			}
		}
		return nil
	} else if ys.IsScalar() {
		if nulls.Any(xs.Nsp) {
			for i, x := range xt {
				if !nulls.Contains(rs.Nsp, uint64(i)) {
					rt[i] = bfn(x, yt[0])
				}
			}
		} else {
			for i, x := range xt {
				rt[i] = bfn(x, yt[0])
			}
		}
		return nil
	} else {
		if nulls.Any(rs.Nsp) {
			for i, x := range xt {
				if !nulls.Contains(rs.Nsp, uint64(i)) {
					rt[i] = bfn(x, yt[i])
				}
			}
		} else {
			for i, x := range xt {
				rt[i] = bfn(x, yt[i])
			}
		}
		return nil
	}
}

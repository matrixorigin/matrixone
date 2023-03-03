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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type opBitT interface {
	constraints.Integer
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

func opBitRightShift[T opBitT](v1, v2 T) T {
	if v2 < 0 {
		return 0
	}
	return v1 >> v2
}

func opBitLeftShift[T opBitT](v1, v2 T) T {
	if v2 < 0 {
		return 0
	}
	return v1 << v2
}

func OpBinaryBitAnd(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return opBinaryBitAll(args, proc, types.BitAnd)
}

func OpBinaryBitOr(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return opBinaryBitAll(args, proc, types.BitOr)
}

func OpBinaryBitXor(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return opBinaryBitAll(args, proc, types.BitXor)
}

func makeRsbytes(vc *vector.Vector, values, rsbytes [][]byte) {
	for i := range rsbytes {
		if nulls.Contains(vc.Nsp, uint64(i)) {
			rsbytes[i] = make([]byte, 0)
			continue
		}
		rsbytes[i] = make([]byte, len(values[i]))
	}
}

func opBinaryBitAll(args []*vector.Vector, proc *process.Process, opt func([]byte, []byte, []byte)) (*vector.Vector, error) {
	left, right := args[0], args[1]
	leftValues, rightValues := vector.MustBytesCols(left), vector.MustBytesCols(right)

	maxLen := vector.Length(left)
	if vector.Length(right) > maxLen {
		maxLen = vector.Length(right)
	}

	if left.IsScalarNull() || right.IsScalarNull() {
		return proc.AllocScalarNullVector(types.T_varbinary.ToType()), nil
	}
	resultVector, err := proc.AllocVectorOfRows(types.T_varbinary.ToType(), 0, nil)
	if err != nil {
		return nil, err
	}

	rsbytes := make([][]byte, maxLen)

	// Get bytes length.
	if vector.Length(right) > vector.Length(left) {
		maxLen = vector.Length(right)
		makeRsbytes(right, rightValues, rsbytes)
	} else {
		maxLen = vector.Length(left)
		makeRsbytes(left, leftValues, rsbytes)
	}

	for i := range rsbytes {
		rsbytes[i] = make([]byte, len(leftValues[0]))
	}

	if vector.Length(right) == 1 {
		for i := 0; i < maxLen; i++ {
			// Nulls determine.
			if nulls.Contains(left.Nsp, uint64(i)) || nulls.Contains(right.Nsp, 0) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			opt(rsbytes[i], leftValues[i], rightValues[0])
		}
		vector.AppendBytes(resultVector, rsbytes, proc.Mp())
	} else if vector.Length(left) == 1 {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(left.Nsp, 0) || nulls.Contains(right.Nsp, uint64(i)) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			opt(rsbytes[i], leftValues[0], rightValues[i])
		}
		vector.AppendBytes(resultVector, rsbytes, proc.Mp())
	} else {
		for i := 0; i < maxLen; i++ {
			if nulls.Contains(left.Nsp, uint64(i)) || nulls.Contains(right.Nsp, uint64(i)) {
				nulls.Add(resultVector.Nsp, uint64(i))
				continue
			}
			opt(rsbytes[i], leftValues[i], rightValues[i])
		}
		vector.AppendBytes(resultVector, rsbytes, proc.Mp())
	}

	return resultVector, nil
}

func OpBitAndFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitAnd[T])
	})
}

func OpBitOrFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitOr[T])
	})
}

func OpBitXorFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitXor[T])
	})
}

func OpBitRightShiftFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitRightShift[T])
	})
}

func OpBitLeftShiftFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitLeftShift[T])
	})
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

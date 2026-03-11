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

func opBinaryBitAll(ivecs []*vector.Vector, proc *process.Process, opt func([]byte, []byte, []byte)) (*vector.Vector, error) {
	left, right := ivecs[0], ivecs[1]

	rtyp := left.GetType()
	if left.IsConstNull() || right.IsConstNull() {
		return vector.NewConstNull(*rtyp, left.Length(), proc.Mp()), nil
	}

	var rval [types.MaxBinaryLen]byte

	if left.IsConst() && right.IsConst() {
		val0 := left.GetBytesAt(0)
		opt(rval[:], val0, right.GetBytesAt(0))
		return vector.NewConstBytes(*rtyp, rval[:len(val0)], left.Length(), proc.Mp()), nil
	}

	rvec, err := proc.AllocVectorOfRows(*rtyp, 0, nil)
	if err != nil {
		return nil, err
	}
	nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())

	for i := 0; i < left.Length(); i++ {
		if !rvec.GetNulls().Contains(uint64(i)) {
			val0 := left.GetBytesAt(i)
			opt(rval[:], val0, right.GetBytesAt(i))
			vector.SetBytesAt(rvec, i, rval[:len(val0)], proc.Mp())
		}
	}

	return rvec, nil
}

func OpBitAndFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, *args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitAnd[T])
	})
}

func OpBitOrFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, *args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitOr[T])
	})
}

func OpBitXorFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, *args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitXor[T])
	})
}

func OpBitRightShiftFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, *args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitRightShift[T])
	})
}

func OpBitLeftShiftFun[T opBitT](args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Arith[T, T](args, proc, *args[0].GetType(), func(xs, ys, rs *vector.Vector) error {
		return goOpBitGeneral(xs, ys, rs, opBitLeftShift[T])
	})
}

func goOpBitGeneral[T opBitT](xs, ys, rs *vector.Vector, bfn opBitFun[T]) error {
	xt, yt, rt := vector.MustFixedCol[T](xs), vector.MustFixedCol[T](ys), vector.MustFixedCol[T](rs)
	if xs.IsConst() {
		if nulls.Any(ys.GetNulls()) {
			for i, y := range yt {
				if !nulls.Contains(rs.GetNulls(), uint64(i)) {
					rt[i] = bfn(xt[0], y)
				}
			}
		} else {
			for i, y := range yt {
				rt[i] = bfn(xt[0], y)
			}
		}
		return nil
	} else if ys.IsConst() {
		if nulls.Any(xs.GetNulls()) {
			for i, x := range xt {
				if !nulls.Contains(rs.GetNulls(), uint64(i)) {
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
		if nulls.Any(rs.GetNulls()) {
			for i, x := range xt {
				if !nulls.Contains(rs.GetNulls(), uint64(i)) {
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

// Copyright 2021 Matrix Origin
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

package add

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"golang.org/x/exp/constraints"
)

func signedOverflow[T constraints.Signed](a, b, c T) bool {
	return (a > 0 && b > 0 && c < 0) || (a < 0 && b < 0 && c > 0)
}

func unsignedOverflow[T constraints.Unsigned](a, b, c T) bool {
	return c < a || c < b
}

func NumericAddSigned[T constraints.Signed](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] + y
				if signedOverflow(xt[0], y, rt[i]) {
					return moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow")
				}
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[0]
				if signedOverflow(x, yt[0], rt[i]) {
					return moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow")
				}
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[i]
				if signedOverflow(x, yt[i], rt[i]) {
					return moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow")
				}
			}
		}
		return nil
	}
}

func NumericAddUnsigned[T constraints.Unsigned](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] + y
				if unsignedOverflow(xt[0], y, rt[i]) {
					return moerr.NewError(moerr.OUT_OF_RANGE, "uint add overflow")
				}
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[0]
				if unsignedOverflow(x, yt[0], rt[i]) {
					return moerr.NewError(moerr.OUT_OF_RANGE, "uint add overflow")
				}
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[i]
				if unsignedOverflow(x, yt[i], rt[i]) {
					return moerr.NewError(moerr.OUT_OF_RANGE, "uint add overflow")
				}
			}
		}
		return nil
	}
}

func NumericAddFloat[T constraints.Float](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[T](rs)
	if xs.IsScalar() {
		for i, y := range yt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = xt[0] + y
			}
		}
		return nil
	} else if ys.IsScalar() {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[0]
			}
		}
		return nil
	} else {
		for i, x := range xt {
			if !nulls.Contains(rs.Nsp, uint64(i)) {
				rt[i] = x + yt[i]
			}
		}
		return nil
	}
}

func Uint32AddScalar(x uint32, ys []uint32, zs []uint32) {
	for i, y := range ys {
		zs[i] = x + y
		if unsignedOverflow(x, y, zs[i]) {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "uint32 add overflow"))
		}
	}
}

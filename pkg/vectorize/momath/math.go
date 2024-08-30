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

package momath

import (
	"golang.org/x/exp/constraints"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func Acos(v float64) (float64, error) {
	if v < -1 || v > 1 {
		// MySQL is totally F***ed.
		return 0, moerr.NewInvalidArgNoCtx("acos", v)
	} else {
		return math.Acos(v), nil
	}
}

func Atan(v float64) (float64, error) {
	return math.Atan(v), nil
}

func Cos(v float64) (float64, error) {
	return math.Cos(v), nil
}

func Cot(v float64) (float64, error) {
	if v == 0 {
		return 0, moerr.NewInvalidArgNoCtx("cot", "0")
	} else {
		return math.Tan(math.Pi/2.0 - v), nil
	}
}

func Exp(v float64) (float64, error) {
	return math.Exp(v), nil
}

func Sqrt(v float64) (float64, error) {
	if v < 0 {
		return 0, moerr.NewInvalidArgNoCtx("Sqrt", v)
	} else {
		return math.Sqrt(v), nil
	}
}

func Ln(v float64) (float64, error) {
	if v <= 0 {
		return 0, moerr.NewInvalidArgNoCtx("ln", v)
	} else {
		return math.Log(v), nil
	}
}

func Log2(v float64) (float64, error) {
	if v <= 0 {
		return 0, moerr.NewInvalidArgNoCtx("log2", v)
	} else {
		return math.Log2(v), nil
	}
}

func Lg(v float64) (float64, error) {
	if v <= 0 {
		return 0, moerr.NewInvalidArgNoCtx("log10", v)
	} else {
		return math.Log10(v), nil
	}
}

func Sin(v float64) (float64, error) {
	return math.Sin(v), nil
}

func Sinh(v float64) (float64, error) {
	r := math.Sinh(v)
	if math.IsInf(r, 0) {
		return 0, moerr.NewOutOfRangeNoCtxf("float64", "DOUBLE value is out of range in 'sinh(%v)'", v)
	}
	return r, nil
}

func Tan(v float64) (float64, error) {
	return math.Tan(v), nil
}

func AbsSigned[T constraints.Signed | constraints.Float](v T) (T, error) {
	//NOTE: AbsSigned specifically deals with int and float and not uint.
	// If we have uint, we return the value as such.
	if v < 0 {
		v = -v
	}
	if v < 0 {
		// This could occur for int8 (-128 to 127)
		// If the v is -128 and if we multiply by -1 = 128, which is out of range of int8. It could give a -ve value for such case.
		return 0, moerr.NewOutOfRangeNoCtxf("int", "'%v'", v)
	}
	return v, nil
}

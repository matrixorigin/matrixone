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

func Ln(v float64) (float64, error) {
	if v <= 0 {
		return 0, moerr.NewInvalidArgNoCtx("ln", v)
	} else {
		return math.Log(v), nil
	}
}

func Sin(v float64) (float64, error) {
	return math.Sin(v), nil
}

func Sinh(v float64) (float64, error) {
	r := math.Sinh(v)
	if math.IsInf(r, 0) {
		return 0, moerr.NewOutOfRangeNoCtx("float64", "DOUBLE value is out of range in 'sinh(%v)'", v)
	}
	return r, nil
}

func Tan(v float64) (float64, error) {
	return math.Tan(v), nil
}

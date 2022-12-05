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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func Acos(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			if v < -1 || v > 1 {
				// MySQL is totally F***ed.
				// return moerr.NewError(moerr.INVALID_ARGUMENT, fmt.Sprintf("acos argument %v is not valid", v))
				nulls.Add(result.Nsp, uint64(i))
			} else {
				resCol[i] = math.Acos(v)
			}
		}
	}
	return nil
}

func Atan(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			resCol[i] = math.Atan(v)
		}
	}
	return nil
}

func AtanWithTwoArg(firstArg, secondArg, result *vector.Vector) error {
	firstCol := vector.MustTCols[float64](firstArg)
	secondCol := vector.MustTCols[float64](secondArg)
	resCol := vector.MustTCols[float64](result)
	for i, v := range firstCol {
		if v == 0 {
			return moerr.NewInvalidArgNoCtx("Atan first input", 0)
		}
		resCol[i] = math.Atan(secondCol[i] / v)
	}
	return nil
}

func Cos(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			resCol[i] = math.Cos(v)
		}
	}
	return nil
}

func Cot(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			if v == 0 {
				panic(moerr.NewInvalidArgNoCtx("cot", "cot(0)"))
			} else {
				resCol[i] = math.Tan(math.Pi/2.0 - v)
			}
		}
	}
	return nil
}

func Exp(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			resCol[i] = math.Exp(v)
		}
	}
	return nil
}

func Ln(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			if v <= 0 {
				return moerr.NewInvalidArgNoCtx("ln", v)
			} else {
				resCol[i] = math.Log(v)
			}
		}
	}
	return nil
}

func Sin(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			resCol[i] = math.Sin(v)
		}
	}
	return nil
}

func Sinh(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			r := math.Sinh(v)
			if math.IsInf(r, 0) {
				return moerr.NewOutOfRangeNoCtx("float64", "DOUBLE value is out of range in 'sinh(%v)'", v)
			}
			resCol[i] = r
		}
	}
	return nil
}

func Tan(arg, result *vector.Vector) error {
	argCol := vector.MustTCols[float64](arg)
	resCol := vector.MustTCols[float64](result)
	nulls.Set(result.Nsp, arg.Nsp)
	for i, v := range argCol {
		if !nulls.Contains(arg.Nsp, (uint64)(i)) {
			resCol[i] = math.Tan(v)
		}
	}
	return nil
}

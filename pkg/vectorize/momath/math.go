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

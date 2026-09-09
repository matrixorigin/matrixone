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

package function

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// UnsignedArithmeticBound validates the Decimal128 intermediate produced when
// a prepared expression has acquired an unsigned integer runtime domain.  A
// cast from Decimal128 to UINT64 is intentionally not used as the check: that
// conversion is saturating for positive values beyond UINT64_MAX on some
// execution paths.  Keeping this as a strict function also prevents an
// enclosing expression from cancelling a wrapped intermediate before the
// range check runs.
func UnsignedArithmeticBound(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	max := types.Decimal128{B0_63: math.MaxUint64}
	return opUnaryFixedToFixedWithErrorCheck[types.Decimal128, types.Decimal128](
		parameters,
		result,
		proc,
		length,
		func(v types.Decimal128) (types.Decimal128, error) {
			if max.Less(v) {
				return v, moerr.NewOutOfRangef(proc.Ctx, "uint64", "value '%s'", v.Format(0))
			}
			return v, nil
		},
		selectList,
	)
}

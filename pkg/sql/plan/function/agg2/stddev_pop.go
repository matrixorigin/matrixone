// Copyright 2024 Matrix Origin
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

package agg2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

type aggStdVarPopDecimal64 struct {
	aggVarPopDecimal128
}

func (a *aggStdVarPopDecimal64) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	r, err := getVarianceFromSumPowCount(a.sum, get(), a.count)
	if err != nil {
		panic(err)
	}
	if r.B0_63 == 0 && r.B64_127 == 0 {
		set(r)
		return
	}
	temp, err1 := types.Decimal128FromFloat64(
		math.Sqrt(types.Decimal128ToFloat64(get(), a.scale)),
		38, a.scale)
	if err1 != nil {
		panic(err1)
	}
	set(temp)
}

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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var andFn = optimizedTpsToTrFn[bool, bool](
	func(v1 bool) (bool, bool) { return false, v1 }, false,
	func(v2 bool) (bool, bool) { return false, v2 }, false,
	func(v1, v2 bool) (bool, bool) { return v1 && v2, false }, true,
	nil, true, true,
)

var orFn = optimizedTpsToTrFn[bool, bool](
	func(v1 bool) (bool, bool) { return v1, !v1 }, false,
	func(v2 bool) (bool, bool) { return v2, !v2 }, false,
	func(v1, v2 bool) (bool, bool) { return v1 || v2, false }, true,
	nil, true, true,
)

var xorFn = optimizedTpsToTrFn[bool, bool](
	nil, true,
	nil, true,
	func(v1, v2 bool) (bool, bool) { return v1 != v2, false }, true,
	nil, true, true,
)

func notFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	rs := vector.MustFunctionResult[bool](result)

	if !p1.WithAnyNullValue() {
		rss := vector.MustFixedCol[bool](rs.GetResultVector())
		for i := uint64(0); i < uint64(length); i++ {
			v, _ := p1.GetValue(i)
			rss[i] = !v
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		if err := rs.Append(!v1, null1); err != nil {
			return err
		}
	}
	return nil
}

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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var andFn = generalFunctionTemplateFactor[bool, bool](
	func(v1 bool) (bool, bool) { return false, v1 }, false,
	func(v2 bool) (bool, bool) { return false, v2 }, false,
	func(v1, v2 bool) (bool, bool) { return v1 && v2, false }, true,
	nil, true, true,
)

var orFn = generalFunctionTemplateFactor[bool, bool](
	func(v1 bool) (bool, bool) { return v1, !v1 }, false,
	func(v2 bool) (bool, bool) { return v2, !v2 }, false,
	func(v1, v2 bool) (bool, bool) { return v1 || v2, false }, true,
	nil, true, true,
)

var xorFn = generalFunctionTemplateFactor[bool, bool](
	nil, true,
	nil, true,
	func(v1, v2 bool) (bool, bool) { return v1 != v2, false }, true,
	nil, true, true,
)

func notFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[bool, bool](parameters, result, proc, length, func(v bool) bool {
		return !v
	})
}

func opMultiAnd(params []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rsVec := vector.MustFunctionResult[bool](result).GetResultVector()
	rsArr := vector.MustFixedCol[bool](rsVec)

	arr0 := vector.MustFixedCol[bool](params[0])
	if params[0].IsConstNull() {
		for i := 0; i < length; i++ {
			rsArr[i] = false
		}
		nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
	} else if params[0].IsConst() {
		for i := 0; i < length; i++ {
			rsArr[i] = arr0[0]
		}
	} else {
		copy(rsArr, arr0)
		nulls.Set(rsVec.GetNulls(), params[0].GetNulls())
	}

	for idx := 1; idx < len(params); idx++ {
		arr1 := vector.MustFixedCol[bool](params[idx])

		if params[idx].IsConstNull() {
			for i := 0; i < length; i++ {
				if rsArr[i] {
					rsArr[i] = false
					rsVec.GetNulls().Add(uint64(i))
				}
			}
		} else if params[idx].IsConst() {
			if !arr1[0] {
				for i := 0; i < length; i++ {
					rsArr[i] = false
				}
				nulls.RemoveRange(rsVec.GetNulls(), 0, uint64(length))
			}
		} else {
			if rsVec.GetNulls().Any() || params[idx].GetNulls().Any() {
				rowCount := uint64(length)
				for i := uint64(0); i < rowCount; i++ {
					null1 := rsVec.GetNulls().Contains(i)
					null2 := params[idx].GetNulls().Contains(i)

					if null1 && !null2 {
						if !arr1[i] {
							rsVec.GetNulls().Del(i)
							rsArr[i] = false
						}
					} else if !null1 && null2 {
						if rsArr[i] {
							rsVec.GetNulls().Add(i)
							rsArr[i] = false
						}
					} else if !null1 && !null2 {
						rsArr[i] = rsArr[i] && arr1[i]
					}
				}
			} else {
				rowCount := uint64(length)
				for i := uint64(0); i < rowCount; i++ {
					rsArr[i] = rsArr[i] && arr1[i]
				}
			}
		}
	}
	return nil
}

func opMultiOr(params []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rsVec := vector.MustFunctionResult[bool](result).GetResultVector()
	rsArr := vector.MustFixedCol[bool](rsVec)

	arr0 := vector.MustFixedCol[bool](params[0])
	if params[0].IsConstNull() {
		for i := 0; i < length; i++ {
			rsArr[i] = false
		}
		nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
	} else if params[0].IsConst() {
		for i := 0; i < length; i++ {
			rsArr[i] = arr0[0]
		}
	} else {
		copy(rsArr, arr0)
		nulls.Set(rsVec.GetNulls(), params[0].GetNulls())
	}

	for idx := 1; idx < len(params); idx++ {
		arr1 := vector.MustFixedCol[bool](params[idx])

		if params[idx].IsConstNull() {
			for i := 0; i < length; i++ {
				if !rsArr[i] {
					rsVec.GetNulls().Add(uint64(i))
				}
			}
		} else if params[idx].IsConst() {
			if arr1[0] {
				for i := 0; i < length; i++ {
					rsArr[i] = true
				}
				nulls.RemoveRange(rsVec.GetNulls(), 0, uint64(length))
			}
		} else {
			if rsVec.GetNulls().Any() || params[idx].GetNulls().Any() {
				rowCount := uint64(length)
				for i := uint64(0); i < rowCount; i++ {
					null1 := rsVec.GetNulls().Contains(i)
					null2 := params[idx].GetNulls().Contains(i)

					if null1 && !null2 {
						if arr1[i] {
							rsVec.GetNulls().Del(i)
							rsArr[i] = true
						}
					} else if !null1 && null2 {
						if !rsArr[i] {
							rsVec.GetNulls().Add(i)
						}
					} else if !null1 && !null2 {
						rsArr[i] = rsArr[i] || arr1[i]
					}
				}
			} else {
				rowCount := uint64(length)
				for i := uint64(0); i < rowCount; i++ {
					rsArr[i] = rsArr[i] || arr1[i]
				}
			}
		}
	}
	return nil
}

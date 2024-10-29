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

package vector

import "github.com/matrixorigin/matrixone/pkg/container/types"

type reusableParameterWrapper interface{}

type OptFunctionResultWrapper interface {
	UseOptFunctionParamFrame(paramCount int)
	getConvenientParamList() []reusableParameterWrapper
}

func OptGetParamFromWrapper[ParamType types.FixedSizeTExceptStrType](
	wrapper FunctionResultWrapper, idx int, src *Vector) FunctionParameterWrapper[ParamType] {
	ws := wrapper.getConvenientParamList()

	if fr, ok := ws[idx].(FunctionParameterWrapper[ParamType]); ok && ReuseFunctionFixedTypeParameter(src, fr) {
		return fr
	}

	fr := GenerateFunctionFixedTypeParameter[ParamType](src)
	ws[idx] = fr
	return fr
}

func OptGetBytesParamFromWrapper(wrapper FunctionResultWrapper, idx int, src *Vector) FunctionParameterWrapper[types.Varlena] {
	ws := wrapper.getConvenientParamList()

	if fr, ok := ws[idx].(FunctionParameterWrapper[types.Varlena]); ok && ReuseFunctionStrParameter(src, fr) {
		return fr
	}

	fr := GenerateFunctionStrParameter(src)
	ws[idx] = fr
	return fr
}

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

package plan2

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

//BuiltinFunctionsMap  change BuiltinFunctions to map
var BuiltinFunctionsMap map[string]*FunctionSig

var CastLowTypeToHighTypeMap map[plan.Type_TypeId]map[plan.Type_TypeId]plan.Type_TypeId

func init() {
	//change BuiltinFunctions to map
	BuiltinFunctionsMap = make(map[string]*FunctionSig)
	for _, fun := range BuiltinFunctions {
		BuiltinFunctionsMap[fun.Name] = fun
	}

	//map type cast rule
	lowCastToHighTypeArray := make(map[plan.Type_TypeId][]plan.Type_TypeId)
	lowCastToHighTypeArray[plan.Type_INT8] = []plan.Type_TypeId{
		plan.Type_INT16, plan.Type_INT32, plan.Type_INT64, plan.Type_INT128,
		plan.Type_UINT16, plan.Type_UINT32, plan.Type_UINT64, plan.Type_UINT128,
		plan.Type_FLOAT32, plan.Type_FLOAT64, plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_INT16] = []plan.Type_TypeId{
		plan.Type_INT32, plan.Type_INT64, plan.Type_INT128,
		plan.Type_UINT32, plan.Type_UINT64, plan.Type_UINT128,
		plan.Type_FLOAT32, plan.Type_FLOAT64, plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_INT32] = []plan.Type_TypeId{
		plan.Type_INT64, plan.Type_INT128,
		plan.Type_UINT64, plan.Type_UINT128,
		plan.Type_FLOAT64, plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_INT64] = []plan.Type_TypeId{
		plan.Type_INT128,
		plan.Type_UINT128,
		plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_INT128] = []plan.Type_TypeId{
		plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_UINT8] = []plan.Type_TypeId{
		plan.Type_INT16, plan.Type_INT32, plan.Type_INT64, plan.Type_INT128,
		plan.Type_UINT16, plan.Type_UINT32, plan.Type_UINT64, plan.Type_UINT128,
		plan.Type_FLOAT32, plan.Type_FLOAT64, plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_UINT16] = []plan.Type_TypeId{
		plan.Type_INT32, plan.Type_INT64, plan.Type_INT128,
		plan.Type_UINT32, plan.Type_UINT64, plan.Type_UINT128,
		plan.Type_FLOAT32, plan.Type_FLOAT64, plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_UINT32] = []plan.Type_TypeId{
		plan.Type_INT64, plan.Type_INT128,
		plan.Type_UINT64, plan.Type_UINT128,
		plan.Type_FLOAT64, plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_UINT64] = []plan.Type_TypeId{
		plan.Type_INT128,
		plan.Type_UINT128,
		plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_UINT128] = []plan.Type_TypeId{
		plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_DECIMAL] = []plan.Type_TypeId{
		plan.Type_DECIMAL128,
	}
	lowCastToHighTypeArray[plan.Type_DECIMAL64] = []plan.Type_TypeId{
		plan.Type_DECIMAL128,
	}

	twoEqualTypeCastToHightTypeArray := []plan.Type_TypeId{
		plan.Type_INT8, plan.Type_UINT8, plan.Type_INT16, //means: int8+uint8 will cast to int16+int16
		plan.Type_INT16, plan.Type_UINT16, plan.Type_INT32,
		plan.Type_INT32, plan.Type_UINT32, plan.Type_INT64,
		plan.Type_INT64, plan.Type_UINT64, plan.Type_INT128,
		plan.Type_INT128, plan.Type_UINT128, plan.Type_INT128, //?
		plan.Type_INT64, plan.Type_DECIMAL, plan.Type_DECIMAL128, //?
		plan.Type_INT64, plan.Type_DECIMAL64, plan.Type_DECIMAL128, //?

		plan.Type_INT32, plan.Type_FLOAT32, plan.Type_FLOAT64,
		plan.Type_UINT32, plan.Type_FLOAT32, plan.Type_FLOAT64,
		plan.Type_INT64, plan.Type_FLOAT64, plan.Type_FLOAT64, //?
		plan.Type_UINT64, plan.Type_FLOAT64, plan.Type_FLOAT64, //?
		plan.Type_UINT64, plan.Type_DECIMAL, plan.Type_DECIMAL128, //?
		plan.Type_UINT64, plan.Type_DECIMAL64, plan.Type_DECIMAL128, //?

		plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128, //?
	}

	CastLowTypeToHighTypeMap = make(map[plan.Type_TypeId]map[plan.Type_TypeId]plan.Type_TypeId)
	for k, v := range lowCastToHighTypeArray {
		if _, ok := CastLowTypeToHighTypeMap[k]; !ok {
			CastLowTypeToHighTypeMap[k] = make(map[plan.Type_TypeId]plan.Type_TypeId)
		}
		for _, typ := range v {
			if _, ok := CastLowTypeToHighTypeMap[typ]; !ok {
				CastLowTypeToHighTypeMap[typ] = make(map[plan.Type_TypeId]plan.Type_TypeId)
			}
			CastLowTypeToHighTypeMap[k][typ] = typ
			CastLowTypeToHighTypeMap[typ][k] = typ
		}
	}
	for i := 0; i < len(twoEqualTypeCastToHightTypeArray); i = i + 3 {
		if _, ok := CastLowTypeToHighTypeMap[twoEqualTypeCastToHightTypeArray[i]]; !ok {
			CastLowTypeToHighTypeMap[twoEqualTypeCastToHightTypeArray[i]] = make(map[plan.Type_TypeId]plan.Type_TypeId)
		}
		if _, ok := CastLowTypeToHighTypeMap[twoEqualTypeCastToHightTypeArray[i+1]]; !ok {
			CastLowTypeToHighTypeMap[twoEqualTypeCastToHightTypeArray[i+1]] = make(map[plan.Type_TypeId]plan.Type_TypeId)
		}
		CastLowTypeToHighTypeMap[twoEqualTypeCastToHightTypeArray[i]][twoEqualTypeCastToHightTypeArray[i+1]] = twoEqualTypeCastToHightTypeArray[i+2]
		CastLowTypeToHighTypeMap[twoEqualTypeCastToHightTypeArray[i+1]][twoEqualTypeCastToHightTypeArray[i]] = twoEqualTypeCastToHightTypeArray[i+2]
	}

}

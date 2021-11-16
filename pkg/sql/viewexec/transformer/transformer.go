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

package transformer

import (
	"matrixone/pkg/container/ring"
	"matrixone/pkg/container/ring/sum"
	"matrixone/pkg/container/types"
)

func init() {
	TransformerNamesMap = make(map[string]int)
	for i := range TransformerNames {
		TransformerNamesMap[TransformerNames[i]] = i
	}
}

var sumReturnTypes = map[types.T]types.T{
	types.T_int8:    types.T_int64,
	types.T_int16:   types.T_int64,
	types.T_int32:   types.T_int64,
	types.T_int64:   types.T_int64,
	types.T_uint8:   types.T_uint64,
	types.T_uint16:  types.T_uint64,
	types.T_uint32:  types.T_uint64,
	types.T_uint64:  types.T_uint64,
	types.T_float32: types.T_float64,
	types.T_float64: types.T_float64,
}

func ReturnType(op int, typ types.T) types.T {
	switch op {
	case Avg:
		return types.T_float64
	case Max:
		return typ
	case Min:
		return typ
	case Sum:
		return sumReturnTypes[typ]
	case Count:
		return types.T_int64
	case StarCount:
		return types.T_int64
	}
	return 0
}

func New(op int, typ types.Type) (ring.Ring, error) {
	switch op {
	case Sum:
		return NewSum(typ)
	}
	return nil, nil
}

func NewSum(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_float32, types.T_float64:
		return sum.NewFloat(typ), nil
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return sum.NewUint(typ), nil
	}
	return nil, nil
}

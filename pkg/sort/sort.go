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

package sort

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sort/asc/float32s"
	"matrixone/pkg/sort/asc/float64s"
	"matrixone/pkg/sort/asc/int16s"
	"matrixone/pkg/sort/asc/int32s"
	"matrixone/pkg/sort/asc/int64s"
	"matrixone/pkg/sort/asc/int8s"
	"matrixone/pkg/sort/asc/uint16s"
	"matrixone/pkg/sort/asc/uint32s"
	"matrixone/pkg/sort/asc/uint64s"
	"matrixone/pkg/sort/asc/uint8s"
	"matrixone/pkg/sort/asc/varchar"
	dfloat32s "matrixone/pkg/sort/desc/float32s"
	dfloat64s "matrixone/pkg/sort/desc/float64s"
	dint16s "matrixone/pkg/sort/desc/int16s"
	dint32s "matrixone/pkg/sort/desc/int32s"
	dint64s "matrixone/pkg/sort/desc/int64s"
	dint8s "matrixone/pkg/sort/desc/int8s"
	duint16s "matrixone/pkg/sort/desc/uint16s"
	duint32s "matrixone/pkg/sort/desc/uint32s"
	duint64s "matrixone/pkg/sort/desc/uint64s"
	duint8s "matrixone/pkg/sort/desc/uint8s"
	dvarchar "matrixone/pkg/sort/desc/varchar"
)

func Sort(desc bool, os []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		if desc {
			dint8s.Sort(vec.Col.([]int8), os)
		} else {
			int8s.Sort(vec.Col.([]int8), os)
		}
	case types.T_int16:
		if desc {
			dint16s.Sort(vec.Col.([]int16), os)
		} else {
			int16s.Sort(vec.Col.([]int16), os)
		}
	case types.T_int32:
		if desc {
			dint32s.Sort(vec.Col.([]int32), os)
		} else {
			int32s.Sort(vec.Col.([]int32), os)
		}
	case types.T_int64:
		if desc {
			dint64s.Sort(vec.Col.([]int64), os)
		} else {
			int64s.Sort(vec.Col.([]int64), os)
		}
	case types.T_uint8:
		if desc {
			duint8s.Sort(vec.Col.([]uint8), os)
		} else {
			uint8s.Sort(vec.Col.([]uint8), os)
		}
	case types.T_uint16:
		if desc {
			duint16s.Sort(vec.Col.([]uint16), os)
		} else {
			uint16s.Sort(vec.Col.([]uint16), os)
		}
	case types.T_uint32:
		if desc {
			duint32s.Sort(vec.Col.([]uint32), os)
		} else {
			uint32s.Sort(vec.Col.([]uint32), os)
		}
	case types.T_uint64:
		if desc {
			duint64s.Sort(vec.Col.([]uint64), os)
		} else {
			uint64s.Sort(vec.Col.([]uint64), os)
		}
	case types.T_float32:
		if desc {
			dfloat32s.Sort(vec.Col.([]float32), os)
		} else {
			float32s.Sort(vec.Col.([]float32), os)
		}
	case types.T_float64:
		if desc {
			dfloat64s.Sort(vec.Col.([]float64), os)
		} else {
			float64s.Sort(vec.Col.([]float64), os)
		}
	case types.T_char, types.T_json, types.T_varchar:
		if desc {
			dvarchar.Sort(vec.Col.(*types.Bytes), os)
		} else {
			varchar.Sort(vec.Col.(*types.Bytes), os)
		}
	}
}

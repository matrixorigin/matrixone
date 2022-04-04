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
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/ring/bitand"
	"github.com/matrixorigin/matrixone/pkg/container/ring/bitxor"
	"github.com/matrixorigin/matrixone/pkg/container/ring/stddevpop"
	"github.com/matrixorigin/matrixone/pkg/container/ring/variance"

	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/ring/approxcd"
	"github.com/matrixorigin/matrixone/pkg/container/ring/avg"
	"github.com/matrixorigin/matrixone/pkg/container/ring/count"
	"github.com/matrixorigin/matrixone/pkg/container/ring/max"
	"github.com/matrixorigin/matrixone/pkg/container/ring/min"
	"github.com/matrixorigin/matrixone/pkg/container/ring/starcount"
	"github.com/matrixorigin/matrixone/pkg/container/ring/sum"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	case Count, StarCount:
		return types.T_int64
	case ApproxCountDistinct:
		return types.T_uint64
	case Variance:
		return types.T_float64
	case BitAnd:
		return types.T_uint64
	case BitXor:
		return types.T_uint64
	case StdDevPop:
		return types.T_float64
	}
	return 0
}

func New(op int, typ types.Type) (ring.Ring, error) {
	switch op {
	case Sum:
		return NewSum(typ)
	case Avg:
		return avg.NewAvg(typ), nil
	case Max:
		return NewMax(typ)
	case Min:
		return NewMin(typ)
	case Count:
		return count.NewCount(typ), nil
	case StarCount:
		return starcount.NewCount(typ), nil
	case ApproxCountDistinct:
		return approxcd.NewApproxCountDistinct(typ), nil
	case Variance:
		return variance.NewVarianceRingWithTypeCheck(typ)
	case BitAnd:
		return NewBitAnd(typ)
	case BitXor:
		return NewBitXor(typ)
	case StdDevPop:
		return stddevpop.NewStdDevPopRing(typ), nil
	}
	return nil, nil
}

func NewBitAnd(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_float32, types.T_float64:
		return bitand.NewNumeric(typ), nil
	}
	return nil, errors.New(fmt.Sprintf("'%v' not support BitAnd", typ))
}

func NewBitXor(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_float32, types.T_float64:
		return bitxor.NewBitXor(typ), nil
	}
	return nil, errors.New(fmt.Sprintf("'%v' not support BitXor", typ))
}

func NewSum(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_float32, types.T_float64:
		return sum.NewFloat(typ), nil
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return sum.NewInt(typ), nil
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return sum.NewUint(typ), nil
	}
	return nil, errors.New(fmt.Sprintf("'%v' not support Sum", typ))
}

func NewAvg(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_float32, types.T_float64:
		return avg.NewAvg(typ), nil
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return avg.NewAvg(typ), nil
	}
	return nil, errors.New(fmt.Sprintf("'%v' not support Avg", typ))
}

func NewMax(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_int8:
		return max.NewInt8(typ), nil
	case types.T_int16:
		return max.NewInt16(typ), nil
	case types.T_int32:
		return max.NewInt32(typ), nil
	case types.T_int64:
		return max.NewInt64(typ), nil
	case types.T_uint8:
		return max.NewUInt8(typ), nil
	case types.T_uint16:
		return max.NewUInt16(typ), nil
	case types.T_uint32:
		return max.NewUInt32(typ), nil
	case types.T_uint64:
		return max.NewUInt64(typ), nil
	case types.T_float32:
		return max.NewFloat32(typ), nil
	case types.T_float64:
		return max.NewFloat64(typ), nil
	case types.T_char, types.T_varchar:
		return max.NewStr(typ), nil
	case types.T_date:
		return max.NewDate(typ), nil
	case types.T_datetime:
		return max.NewDatetime(typ), nil
	}
	return nil, errors.New(fmt.Sprintf("'%v' not support Max", typ))
}

func NewMin(typ types.Type) (ring.Ring, error) {
	switch typ.Oid {
	case types.T_int8:
		return min.NewInt8(typ), nil
	case types.T_int16:
		return min.NewInt16(typ), nil
	case types.T_int32:
		return min.NewInt32(typ), nil
	case types.T_int64:
		return min.NewInt64(typ), nil
	case types.T_uint8:
		return min.NewUInt8(typ), nil
	case types.T_uint16:
		return min.NewUInt16(typ), nil
	case types.T_uint32:
		return min.NewUInt32(typ), nil
	case types.T_uint64:
		return min.NewUInt64(typ), nil
	case types.T_float32:
		return min.NewFloat32(typ), nil
	case types.T_float64:
		return min.NewFloat64(typ), nil
	case types.T_char, types.T_varchar:
		return min.NewStr(typ), nil
	case types.T_date:
		return min.NewDate(typ), nil
	case types.T_datetime:
		return min.NewDatetime(typ), nil
	}
	return nil, errors.New(fmt.Sprintf("'%v' not support Min", typ))
}

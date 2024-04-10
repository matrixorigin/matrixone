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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

var MaxSupportedTypes = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_date, types.T_datetime,
	types.T_timestamp, types.T_time,
	types.T_decimal64, types.T_decimal128,
	types.T_bool,
	types.T_bit,
	types.T_varchar, types.T_char, types.T_blob, types.T_text,
	types.T_uuid,
	types.T_binary, types.T_varbinary,
}

func MaxReturnType(typs []types.Type) types.Type {
	return typs[0]
}

type aggMax[from canCompare] struct{}

func newAggMax[from canCompare]() aggexec.SingleAggFromFixedRetFixed[from, from] {
	return aggMax[from]{}
}

type aggMaxDecimal64 struct{}

func newAggMaxDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64] {
	return aggMaxDecimal64{}
}

type aggMaxDecimal128 struct{}

func newAggMaxDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return aggMaxDecimal128{}
}

type aggBytesMax struct {
	isEmpty bool
}

func newAggBytesMax() aggexec.SingleAggFromVarRetVar {
	return &aggBytesMax{}
}

type aggUuidMax struct {
	isEmpty bool
}

func newAggUuidMax() aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid] {
	return &aggUuidMax{}
}

type aggMaxBool struct{}

func newAggMaxBool() aggexec.SingleAggFromFixedRetFixed[bool, bool] {
	return aggMaxBool{}
}

func (a aggMax[from]) Marshal() []byte  { return nil }
func (a aggMax[from]) Unmarshal([]byte) {}
func (a aggMax[from]) Init(set aggexec.AggSetter[from], arg, ret types.Type) error {
	set(getMinValue[from]().(from))
	return nil
}

func getMinValue[T canCompare]() interface{} {
	var t T
	var tt interface{} = &t
	switch tt.(type) {
	case *uint8, *uint16, *uint32, *uint64,
		*types.Date, *types.Datetime, *types.Timestamp, *types.Time:
		return T(0)
	case *int8:
		return int8(math.MinInt8)
	case *int16:
		return int16(math.MinInt16)
	case *int32:
		return int32(math.MinInt32)
	case *int64:
		return int64(math.MinInt64)
	case *float32:
		return float32(-math.MaxFloat32)
	case *float64:
		return -math.MaxFloat64
	}
	panic("unexpected type")
}

func (a aggMaxDecimal64) Marshal() []byte  { return nil }
func (a aggMaxDecimal64) Unmarshal([]byte) {}
func (a aggMaxDecimal64) Init(set aggexec.AggSetter[types.Decimal64], arg, ret types.Type) error {
	set(types.Decimal64Min)
	return nil
}

func (a aggMaxDecimal128) Marshal() []byte  { return nil }
func (a aggMaxDecimal128) Unmarshal([]byte) {}
func (a aggMaxDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) error {
	set(types.Decimal128Min)
	return nil
}

func (a *aggBytesMax) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggBytesMax) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggBytesMax) Init(setter aggexec.AggBytesSetter, arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}

func (a *aggUuidMax) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggUuidMax) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggUuidMax) Init(setter aggexec.AggSetter[types.Uuid], arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}

func (a aggMaxBool) Marshal() []byte     { return nil }
func (a aggMaxBool) Unmarshal(bs []byte) {}
func (a aggMaxBool) Init(setter aggexec.AggSetter[bool], arg types.Type, ret types.Type) error {
	setter(false)
	return nil
}

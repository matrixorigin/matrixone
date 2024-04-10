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

var MinSupportedTypes = []types.T{
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

func MinReturnType(typs []types.Type) types.Type {
	return typs[0]
}

type aggMin[from canCompare] struct{}

func newAggMin[from canCompare]() aggexec.SingleAggFromFixedRetFixed[from, from] {
	return aggMin[from]{}
}

type aggMinDecimal64 struct{}

func newAggMinDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64] {
	return aggMinDecimal64{}
}

type aggMinDecimal128 struct{}

func newAggMinDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return aggMinDecimal128{}
}

type aggBytesMin struct {
	isEmpty bool
}

func newAggBytesMin() aggexec.SingleAggFromVarRetVar {
	return &aggBytesMin{}
}

type aggUuidMin struct {
	isEmpty bool
}

func newAggUuidMin() aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid] {
	return &aggUuidMin{}
}

type aggMinBool struct{}

func newAggMinBool() aggexec.SingleAggFromFixedRetFixed[bool, bool] {
	return aggMinBool{}
}

func (a aggMin[from]) Marshal() []byte  { return nil }
func (a aggMin[from]) Unmarshal([]byte) {}
func (a aggMin[from]) Init(set aggexec.AggSetter[from], arg, ret types.Type) error {
	set(getMaxValue[from]().(from))
	return nil
}

func getMaxValue[T canCompare]() interface{} {
	var t T
	var tt interface{} = &t
	switch tt.(type) {
	case *uint8:
		return uint8(math.MaxUint8)
	case *uint16:
		return uint16(math.MaxUint16)
	case *uint32:
		return uint32(math.MaxUint32)
	case *uint64:
		return uint64(math.MaxUint64)
	case *int8:
		return int8(math.MaxInt8)
	case *int16:
		return int16(math.MaxInt16)
	case *int32:
		return int32(math.MaxInt32)
	case *int64:
		return int64(math.MaxInt64)
	case *float32:
		return float32(math.MaxFloat32)
	case *float64:
		return math.MaxFloat64
	case *types.Date:
		return types.Date(math.MaxInt32)
	case *types.Datetime:
		return types.Datetime(math.MaxInt64)
	case *types.Timestamp:
		return types.Timestamp(math.MaxInt64)
	case *types.Time:
		return types.Time(math.MaxInt64)
	}
	panic("unexpected type")
}

func (a aggMinDecimal64) Marshal() []byte  { return nil }
func (a aggMinDecimal64) Unmarshal([]byte) {}
func (a aggMinDecimal64) Init(set aggexec.AggSetter[types.Decimal64], arg, ret types.Type) error {
	set(types.Decimal64Max)
	return nil
}

func (a aggMinDecimal128) Marshal() []byte  { return nil }
func (a aggMinDecimal128) Unmarshal([]byte) {}
func (a aggMinDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) error {
	set(types.Decimal128Max)
	return nil
}

func (a *aggBytesMin) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggBytesMin) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggBytesMin) Init(setter aggexec.AggBytesSetter, arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}

func (a *aggBytesMin) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	return nil
}

func (a *aggUuidMin) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggUuidMin) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggUuidMin) Init(setter aggexec.AggSetter[types.Uuid], arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}

func (a *aggUuidMin) Flush(get aggexec.AggGetter[types.Uuid], set aggexec.AggSetter[types.Uuid]) error {
	return nil
}

func (a aggMinBool) Marshal() []byte     { return nil }
func (a aggMinBool) Unmarshal(bs []byte) {}
func (a aggMinBool) Init(setter aggexec.AggSetter[bool], arg types.Type, ret types.Type) error {
	setter(true)
	return nil
}

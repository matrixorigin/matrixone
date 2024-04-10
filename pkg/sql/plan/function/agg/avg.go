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
)

var AvgSupportedTypes = []types.T{
	types.T_bit,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
}

func AvgReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_decimal128, 18, s)
	case types.T_decimal128:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_decimal128, 18, s)
	default:
		return types.T_float64.ToType()
	}
}

type aggAvg[from numeric] struct {
	count int64
}

func newAggAvg[from numeric]() aggexec.SingleAggFromFixedRetFixed[from, float64] {
	return &aggAvg[from]{}
}

type aggAvgDecimal64 struct {
	count    int64
	argScale int32
}

func newAggAvgDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128] {
	return &aggAvgDecimal64{}
}

type aggAvgDecimal128 struct {
	count    int64
	argScale int32
}

func newAggAvgDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return &aggAvgDecimal128{}
}

func (a *aggAvg[from]) Marshal() []byte       { return types.EncodeInt64(&a.count) }
func (a *aggAvg[from]) Unmarshal(data []byte) { a.count = types.DecodeInt64(data) }
func (a *aggAvg[from]) Init(set aggexec.AggSetter[float64], arg, ret types.Type) error {
	set(0)
	a.count = 0
	return nil
}

func (a *aggAvgDecimal64) Marshal() []byte {
	bs := types.EncodeInt64(&a.count)
	bs = append(bs, types.EncodeInt32(&a.argScale)...)
	return bs
}
func (a *aggAvgDecimal64) Unmarshal(data []byte) {
	a.count = types.DecodeInt64(data[:8])
	a.argScale = types.DecodeInt32(data[8:])
}
func (a *aggAvgDecimal64) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) error {
	set(types.Decimal128{B0_63: 0, B64_127: 0})
	a.count = 0
	a.argScale = arg.Scale
	return nil
}

func (a *aggAvgDecimal128) Marshal() []byte {
	bs := types.EncodeInt64(&a.count)
	bs = append(bs, types.EncodeInt32(&a.argScale)...)
	return bs
}
func (a *aggAvgDecimal128) Unmarshal(data []byte) {
	a.count = types.DecodeInt64(data[:8])
	a.argScale = types.DecodeInt32(data[8:])
}
func (a *aggAvgDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) error {
	set(types.Decimal128{B0_63: 0, B64_127: 0})
	a.count = 0
	a.argScale = arg.Scale
	return nil
}

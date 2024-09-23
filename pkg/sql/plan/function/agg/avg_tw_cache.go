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

func RegisterAvgTwCache(id int64) {
	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[uint64], aggAvgTwCacheFills[uint64], aggAvgTwCacheMerge[uint64], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[int8], aggAvgTwCacheFills[int8], aggAvgTwCacheMerge[int8], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[int16], aggAvgTwCacheFills[int16], aggAvgTwCacheMerge[int16], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[int32], aggAvgTwCacheFills[int32], aggAvgTwCacheMerge[int32], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[int64], aggAvgTwCacheFills[int64], aggAvgTwCacheMerge[int64], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[uint8], aggAvgTwCacheFills[uint8], aggAvgTwCacheMerge[uint8], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[uint16], aggAvgTwCacheFills[uint16], aggAvgTwCacheMerge[uint16], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[uint32], aggAvgTwCacheFills[uint32], aggAvgTwCacheMerge[uint32], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[uint64], aggAvgTwCacheFills[uint64], aggAvgTwCacheMerge[uint64], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[float32], aggAvgTwCacheFills[float32], aggAvgTwCacheMerge[float32], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), AvgTwCacheReturnType, true),
		nil, generateAvgTwCacheContext, nil,
		aggAvgTwCacheFill[float64], aggAvgTwCacheFills[float64], aggAvgTwCacheMerge[float64], aggTwCacheAvgFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), AvgTwCacheReturnType, true),
		nil, generateAggTwCacheAvgDecimalContext, nil,
		aggAvgTwCacheOfDecimal64Fill, aggAvgTwCacheOfDecimal64Fills, aggAvgTwCacheOfDecimalMerge, aggAvgTwCacheOfDecimalFlush)

	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), AvgTwCacheReturnType, true),
		nil, generateAggTwCacheAvgDecimalContext, nil,
		aggAvgTwCacheOfDecimal128Fill, aggAvgTwCacheOfDecimal128Fills, aggAvgTwCacheOfDecimalMerge, aggAvgTwCacheOfDecimalFlush)
}

var AvgTwCacheSupportedTypes = []types.T{
	types.T_bit,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
}

func AvgTwCacheReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64, types.T_decimal128:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_varchar, types.MaxVarcharLen, s)
	default:
		return types.T_char.ToType()
	}
}

type AvgTwCacheContext struct {
	Sum   float64
	Count int64
}

func (a *AvgTwCacheContext) Marshal() []byte {
	res := make([]byte, 16)
	s := types.EncodeFloat64(&a.Sum)
	copy(res, s)
	c := types.EncodeInt64(&a.Count)
	copy(res[8:], c)
	return res
}
func (a *AvgTwCacheContext) Unmarshal(bs []byte) {
	a.Sum = types.DecodeFloat64(bs[0:])
	a.Count = types.DecodeInt64(bs[8:])
}
func generateAvgTwCacheContext(_ types.Type, _ ...types.Type) aggexec.AggGroupExecContext {
	return &AvgTwCacheContext{}
}

func aggAvgTwCacheFill[from numeric](
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	groupCtx.(*AvgTwCacheContext).Count++
	groupCtx.(*AvgTwCacheContext).Sum += float64(value)
	return nil
}

func aggAvgTwCacheFills[from numeric](
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	groupCtx.(*AvgTwCacheContext).Count += int64(count)
	groupCtx.(*AvgTwCacheContext).Sum += float64(value) * float64(count)
	return nil
}

func aggAvgTwCacheMerge[from numeric](
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	groupCtx1.(*AvgTwCacheContext).Count += groupCtx2.(*AvgTwCacheContext).Count
	groupCtx1.(*AvgTwCacheContext).Sum += groupCtx2.(*AvgTwCacheContext).Sum
	return nil
}
func aggTwCacheAvgFlush(
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	resultGetter aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	res := make([]byte, 16)
	s := types.EncodeFloat64(&groupCtx.(*AvgTwCacheContext).Sum)
	copy(res, s)
	c := types.EncodeInt64(&groupCtx.(*AvgTwCacheContext).Count)
	copy(res[8:], c)
	err := resultSetter(res)
	return err
}

type AvgTwCacheDecimalContext struct {
	Sum   types.Decimal128
	Count int64
	Scale int32
}

func (a *AvgTwCacheDecimalContext) Marshal() []byte {
	res := make([]byte, 28)
	d1 := types.EncodeUint64(&a.Sum.B0_63)
	copy(res[0:], d1)
	d2 := types.EncodeUint64(&a.Sum.B64_127)
	copy(res[8:], d2)
	c := types.EncodeInt64(&a.Count)
	copy(res[16:], c)
	s := types.EncodeInt32(&a.Scale)
	copy(res[24:], s)
	return res
}

func (a *AvgTwCacheDecimalContext) Unmarshal(bs []byte) {
	d1 := types.DecodeUint64(bs[0:])
	d2 := types.DecodeUint64(bs[8:])
	a.Sum = types.Decimal128{B0_63: d1, B64_127: d2}
	a.Count = types.DecodeInt64(bs[16:])
	a.Scale = types.DecodeInt32(bs[24:])
}

func generateAggTwCacheAvgDecimalContext(_ types.Type, parameters ...types.Type) aggexec.AggGroupExecContext {
	return &AvgTwCacheDecimalContext{
		Scale: parameters[0].Scale,
		Sum:   types.Decimal128{B0_63: 0, B64_127: 0},
	}
}

func aggAvgTwCacheOfDecimalMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	var err error
	t1 := groupCtx1.(*AvgTwCacheDecimalContext)
	t2 := groupCtx2.(*AvgTwCacheDecimalContext)
	t1.Count += t2.Count
	t1.Sum, err = t1.Sum.Add128(t2.Sum)
	return err
}
func aggAvgTwCacheOfDecimalFlush(
	groupCtx aggexec.AggGroupExecContext,
	commonCtx aggexec.AggCommonExecContext,
	resultGetter aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	res := make([]byte, 28)
	t := groupCtx.(*AvgTwCacheDecimalContext)
	d1 := types.EncodeUint64(&t.Sum.B0_63)
	copy(res[0:], d1)
	d2 := types.EncodeUint64(&t.Sum.B64_127)
	copy(res[8:], d2)
	c := types.EncodeInt64(&t.Count)
	copy(res[16:], c)
	s := types.EncodeInt32(&t.Scale)
	copy(res[24:], s)
	err := resultSetter(res)
	return err
}

func aggAvgTwCacheOfDecimal64Fill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal64, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	var err error
	t := groupCtx.(*AvgTwCacheDecimalContext)
	t.Count++
	t.Sum, err = t.Sum.Add64(value)
	return err
}

func aggAvgTwCacheOfDecimal64Fills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal64, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	t := groupCtx.(*AvgTwCacheDecimalContext)
	t.Count += int64(count)

	v := aggexec.FromD64ToD128(value)
	r, _, err := v.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, t.Scale, 0)
	if err != nil {
		return err
	}
	t.Sum, err = t.Sum.Add128(r)
	return err
}

func aggAvgTwCacheOfDecimal128Fill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal128, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	var err error
	t := groupCtx.(*AvgTwCacheDecimalContext)
	t.Count++
	t.Sum, err = t.Sum.Add128(value)
	return err
}

func aggAvgTwCacheOfDecimal128Fills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value types.Decimal128, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	t := groupCtx.(*AvgTwCacheDecimalContext)
	scale := t.Scale
	t.Count += int64(count)

	r, _, err := value.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, scale, 0)
	if err != nil {
		return err
	}
	t.Sum, err = t.Sum.Add128(r)
	return err
}

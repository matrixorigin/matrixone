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

func RegisterAvgTwResult(id int64) {
	aggexec.RegisterAggFromBytesRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_char.ToType(), AvgTwResultReturnType, true),
		nil, generateAvgTwResultContext, nil,
		aggAvgTwResultFill, aggAvgTwResultFills, aggAvgTwResultMerge, aggTwResultAvgFlush)

	aggexec.RegisterAggFromBytesRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_varchar.ToType(), AvgTwResultReturnType, true),
		nil, generateAggTwResultAvgDecimalContext, nil,
		aggAvgTwResultOfDecimalFill, aggAvgTwResultOfDecimalFills, aggAvgTwResultOfDecimalMerge, aggAvgTwResultOfDecimalFlush)
}

var AvgTwResultSupportedTypes = []types.T{
	types.T_char, types.T_varchar,
}

func AvgTwResultReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_varchar:
		return types.New(types.T_decimal128, 18, typs[0].Scale)
	default:
		return types.T_float64.ToType()
	}
}

type AvgTwResultContext struct {
	Sum   float64
	Count int64
}

func (a *AvgTwResultContext) Marshal() []byte {
	res := make([]byte, 16)
	s := types.EncodeFloat64(&a.Sum)
	copy(res, s)
	c := types.EncodeInt64(&a.Count)
	copy(res[8:], c)
	return res
}
func (a *AvgTwResultContext) Unmarshal(bs []byte) {
	a.Sum = types.DecodeFloat64(bs[0:])
	a.Count = types.DecodeInt64(bs[8:])
}
func generateAvgTwResultContext(_ types.Type, _ ...types.Type) aggexec.AggGroupExecContext {
	return &AvgTwResultContext{}
}

func aggAvgTwResultFill(
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	ResultGetter aggexec.AggGetter[float64], ResultSetter aggexec.AggSetter[float64]) error {
	sum := types.DecodeFloat64(value[0:])
	count := types.DecodeInt64(value[8:])
	groupCtx.(*AvgTwResultContext).Count += count
	groupCtx.(*AvgTwResultContext).Sum += sum
	return nil
}

func aggAvgTwResultFills(
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	ResultGetter aggexec.AggGetter[float64], ResultSetter aggexec.AggSetter[float64]) error {
	sum := types.DecodeFloat64(value[0:])
	c := types.DecodeInt64(value[8:])
	groupCtx.(*AvgTwResultContext).Count += c * int64(count)
	groupCtx.(*AvgTwResultContext).Sum += sum * float64(count)
	return nil
}

func aggAvgTwResultMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	ResultGetter1, ResultGetter2 aggexec.AggGetter[float64],
	ResultSetter aggexec.AggSetter[float64]) error {
	groupCtx1.(*AvgTwResultContext).Count += groupCtx2.(*AvgTwResultContext).Count
	groupCtx1.(*AvgTwResultContext).Sum += groupCtx2.(*AvgTwResultContext).Sum
	return nil
}
func aggTwResultAvgFlush(
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	ResultGetter aggexec.AggGetter[float64],
	ResultSetter aggexec.AggSetter[float64]) error {
	t := groupCtx.(*AvgTwResultContext)
	ResultSetter(t.Sum / float64(t.Count))
	return nil
}

type AvgTwResultDecimalContext struct {
	Sum   types.Decimal128
	Count int64
	Scale int32
}

func (a *AvgTwResultDecimalContext) Marshal() []byte {
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

func (a *AvgTwResultDecimalContext) Unmarshal(bs []byte) {
	d1 := types.DecodeUint64(bs[0:])
	d2 := types.DecodeUint64(bs[8:])
	a.Sum = types.Decimal128{B0_63: d1, B64_127: d2}
	a.Count = types.DecodeInt64(bs[16:])
	a.Scale = types.DecodeInt32(bs[24:])
}

func generateAggTwResultAvgDecimalContext(_ types.Type, parameters ...types.Type) aggexec.AggGroupExecContext {
	return &AvgTwResultDecimalContext{
		Scale: -1,
		Sum:   types.Decimal128{B0_63: 0, B64_127: 0},
	}
}

func aggAvgTwResultOfDecimalMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	ResultGetter1, ResultGetter2 aggexec.AggGetter[types.Decimal128],
	ResultSetter aggexec.AggSetter[types.Decimal128]) error {
	var err error
	t1 := groupCtx1.(*AvgTwResultDecimalContext)
	t2 := groupCtx2.(*AvgTwResultDecimalContext)
	t1.Count += t2.Count
	t1.Sum, err = t1.Sum.Add128(t2.Sum)
	return err
}
func aggAvgTwResultOfDecimalFlush(
	groupCtx aggexec.AggGroupExecContext,
	commonCtx aggexec.AggCommonExecContext,
	ResultGetter aggexec.AggGetter[types.Decimal128],
	ResultSetter aggexec.AggSetter[types.Decimal128]) error {
	t := groupCtx.(*AvgTwResultDecimalContext)
	if t.Count == 0 {
		ResultSetter(types.Decimal128{B0_63: 0, B64_127: 0})
		return nil
	}
	v, _, err := t.Sum.Div(types.Decimal128{B0_63: uint64(t.Count), B64_127: 0}, t.Scale, 0)
	if err != nil {
		return err
	}
	ResultSetter(v)
	return nil
}

func aggAvgTwResultOfDecimalFill(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	ResultGetter aggexec.AggGetter[types.Decimal128], ResultSetter aggexec.AggSetter[types.Decimal128]) error {
	var err error
	t := groupCtx.(*AvgTwResultDecimalContext)
	d1 := types.DecodeUint64(value[0:])
	d2 := types.DecodeUint64(value[8:])
	sum := types.Decimal128{B0_63: d1, B64_127: d2}
	count := types.DecodeInt64(value[16:])
	if t.Scale < 0 {
		t.Scale = types.DecodeInt32(value[24:])
	}
	t.Count += count
	t.Sum, err = t.Sum.Add128(sum)
	return err
}

func aggAvgTwResultOfDecimalFills(
	groupCtx aggexec.AggGroupExecContext, commonCtx aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	ResultGetter aggexec.AggGetter[types.Decimal128], ResultSetter aggexec.AggSetter[types.Decimal128]) error {
	t := groupCtx.(*AvgTwResultDecimalContext)
	d1 := types.DecodeUint64(value[0:])
	d2 := types.DecodeUint64(value[8:])
	sum := types.Decimal128{B0_63: d1, B64_127: d2}
	c := types.DecodeInt64(value[16:])
	if t.Scale < 0 {
		t.Scale = types.DecodeInt32(value[24:])
	}
	t.Count += c * int64(count)
	r, _, err := sum.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, t.Scale, 0)
	if err != nil {
		return err
	}
	t.Sum, err = t.Sum.Add128(r)
	return err
}

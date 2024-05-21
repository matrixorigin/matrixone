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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterBitmapConstruct2(id int64) {
	aggexec.RegisterAggFromFixedRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), BitmapConstructReturnType, true),
		nil, generateBitmapGroupContext, nil,
		aggBitmapConstructFill, aggBitmapConstructFills, aggBitmapConstructMerge, aggBitmapConstructFlush)
}

func RegisterBitmapOr2(id int64) {
	aggexec.RegisterAggFromBytesRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_varbinary.ToType(), BitmapOrReturnType, true),
		nil, generateBitmapGroupContext, nil,
		aggBitmapOrFill, aggBitmapOrFills, aggBitmapConstructMerge, aggBitmapConstructFlush)
}

var BitmapConstructSupportedTypes = []types.T{
	types.T_uint64,
}

func BitmapConstructReturnType(_ []types.Type) types.Type {
	return types.T_varbinary.ToType()
}

type aggBitmapGroupContext struct {
	bmp *roaring.Bitmap
}

func generateBitmapGroupContext(_ types.Type, _ ...types.Type) aggexec.AggGroupExecContext {
	return &aggBitmapGroupContext{bmp: roaring.New()}
}
func (a *aggBitmapGroupContext) Marshal() []byte {
	b, _ := a.bmp.ToBytes()
	return b
}
func (a *aggBitmapGroupContext) Unmarshal(bs []byte) {
	a.bmp = roaring.New()
	_ = a.bmp.UnmarshalBinary(bs)
}

func aggBitmapConstructFill(
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value uint64, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	a := groupCtx.(*aggBitmapGroupContext)
	a.bmp.Add(uint32(value))
	return nil
}
func aggBitmapConstructFills(
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value uint64, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	return aggBitmapConstructFill(groupCtx, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggBitmapConstructMerge(
	groupCtx1, groupCtx2 aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	if isEmpty2 {
		return nil
	}
	a1 := groupCtx1.(*aggBitmapGroupContext)
	a2 := groupCtx2.(*aggBitmapGroupContext)
	a1.bmp.Or(a2.bmp)
	return nil
}
func aggBitmapConstructFlush(
	groupCtx aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	resultGetter aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	a := groupCtx.(*aggBitmapGroupContext)
	b, err := a.bmp.MarshalBinary()
	if err != nil {
		return err
	}
	return resultSetter(b)
}

var BitmapOrSupportedTypes = []types.T{
	types.T_varbinary,
}

var BitmapOrReturnType = BitmapConstructReturnType

func aggBitmapOrFill(
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	bmp := roaring.New()
	if err := bmp.UnmarshalBinary(value); err != nil {
		return err
	}

	a := groupCtx.(*aggBitmapGroupContext)
	if isEmpty {
		a.bmp = bmp
		return nil
	}
	a.bmp.Or(bmp)
	return nil
}
func aggBitmapOrFills(
	groupCtx aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	return aggBitmapOrFill(groupCtx, nil, value, isEmpty, resultGetter, resultSetter)
}

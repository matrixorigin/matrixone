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

func RegisterBitmapConstruct1(id int64) {
	aggexec.RegisterSingleAggFromFixedToVar(
		aggexec.MakeSingleAgg2RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), BitmapConstructReturnType, false, true),
			newAggBitmapConstruct,
			InitAggBitmapConstruct,
			FillAggBitmapConstruct, nil, FillsAggBitmapConstruct,
			MergeAggBitmapConstruct,
			FlushAggBitmapConstruct,
		))
}

func RegisterBitmapOr1(id int64) {
	aggexec.RegisterSingleAggFromVarToVar(
		aggexec.MakeSingleAgg4RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_varbinary.ToType(), BitmapOrReturnType, false, true),
			newAggBitmapOr,
			InitAggBitmapOr,
			FillAggBitmapOr, nil, FillsAggBitmapOr,
			MergeAggBitmapOr,
			FlushAggBitmapOr,
		))
}

var BitmapConstructSupportedTypes = []types.T{
	types.T_uint64,
}

func BitmapConstructReturnType(_ []types.Type) types.Type {
	return types.T_varbinary.ToType()
}

type aggBitmapConstruct struct {
	bmp *roaring.Bitmap
}

func newAggBitmapConstruct() aggexec.SingleAggFromFixedRetVar[uint64] {
	return &aggBitmapConstruct{}
}

func (a *aggBitmapConstruct) Marshal() []byte {
	b, _ := a.bmp.MarshalBinary()
	return b
}
func (a *aggBitmapConstruct) Unmarshal(bs []byte) {
	a.bmp = roaring.New()
	_ = a.bmp.UnmarshalBinary(bs)
}
func (a *aggBitmapConstruct) Init(set aggexec.AggBytesSetter, arg, ret types.Type) error {
	a.bmp = roaring.New()
	return nil
}

func InitAggBitmapConstruct(
	exec aggexec.SingleAggFromFixedRetVar[uint64], set aggexec.AggBytesSetter, arg, ret types.Type) error {
	a := exec.(*aggBitmapConstruct)
	a.bmp = roaring.New()
	return nil
}
func FillAggBitmapConstruct(
	exec aggexec.SingleAggFromFixedRetVar[uint64],
	value uint64, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggBitmapConstruct)
	a.bmp.Add(uint32(value))
	return nil
}
func FillsAggBitmapConstruct(
	exec aggexec.SingleAggFromFixedRetVar[uint64],
	value uint64, isNull bool, count int, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	if !isNull {
		return FillAggBitmapConstruct(exec, value, getter, setter)
	}
	return nil
}
func MergeAggBitmapConstruct(
	exec1, exec2 aggexec.SingleAggFromFixedRetVar[uint64],
	getter1, getter2 aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a1 := exec1.(*aggBitmapConstruct)
	a2 := exec2.(*aggBitmapConstruct)
	a1.bmp.Or(a2.bmp)
	return nil
}
func FlushAggBitmapConstruct(
	exec aggexec.SingleAggFromFixedRetVar[uint64],
	getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggBitmapConstruct)
	res, err := a.bmp.MarshalBinary()
	if err != nil {
		return err
	}
	return setter(res)
}

var BitmapOrSupportedTypes = []types.T{
	types.T_varbinary,
}

var BitmapOrReturnType = BitmapConstructReturnType

type aggBitmapOr struct {
	bmp *roaring.Bitmap
}

func newAggBitmapOr() aggexec.SingleAggFromVarRetVar {
	return &aggBitmapOr{}
}

func (a *aggBitmapOr) Marshal() []byte {
	b, _ := a.bmp.MarshalBinary()
	return b
}
func (a *aggBitmapOr) Unmarshal(bs []byte) {
	a.bmp = roaring.New()
	_ = a.bmp.UnmarshalBinary(bs)
}
func (a *aggBitmapOr) Init(set aggexec.AggBytesSetter, arg, ret types.Type) error {
	a.bmp = roaring.New()
	return nil
}

func InitAggBitmapOr(
	exec aggexec.SingleAggFromVarRetVar, set aggexec.AggBytesSetter, arg, ret types.Type) error {
	a := exec.(*aggBitmapOr)
	a.bmp = roaring.New()
	return nil
}
func FillAggBitmapOr(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	bmp := roaring.New()
	if err := bmp.UnmarshalBinary(value); err != nil {
		return err
	}

	a := exec.(*aggBitmapOr)
	a.bmp.Or(bmp)
	return nil
}
func FillsAggBitmapOr(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, isNull bool, count int, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	if !isNull {
		return FillAggBitmapOr(exec, value, getter, setter)
	}
	return nil
}
func MergeAggBitmapOr(
	exec1, exec2 aggexec.SingleAggFromVarRetVar,
	getter1, getter2 aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a1 := exec1.(*aggBitmapOr)
	a2 := exec2.(*aggBitmapOr)
	a1.bmp.Or(a2.bmp)
	return nil
}
func FlushAggBitmapOr(
	exec aggexec.SingleAggFromVarRetVar,
	getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggBitmapOr)
	res, err := a.bmp.MarshalBinary()
	if err != nil {
		return err
	}
	return setter(res)
}

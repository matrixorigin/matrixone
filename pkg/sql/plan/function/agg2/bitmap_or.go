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

package agg2

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterBitmapOr(id int64) {
	aggexec.RegisterDeterminedSingleAgg(
		aggexec.MakeDeterminedSingleAggInfo(id, types.T_varbinary.ToType(), types.T_varbinary.ToType(), false, true),
		newAggBitmapOr)
}

var BitmapOrSupportedTypes = []types.T{
	types.T_varbinary,
}

type aggBitmapOr struct {
	bmp *roaring.Bitmap
}

func newAggBitmapOr() *aggBitmapOr {
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
func (a *aggBitmapOr) Init(set aggexec.AggBytesSetter, arg, ret types.Type) {
	a.bmp = roaring.New()
}
func (a *aggBitmapOr) FillBytes(value []byte, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	bmp := roaring.New()
	_ = bmp.UnmarshalBinary(value)
	a.bmp.Or(bmp)
}
func (a *aggBitmapOr) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {}
func (a *aggBitmapOr) Fills(value []byte, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if isNull {
		return
	}
	bmp := roaring.New()
	_ = bmp.UnmarshalBinary(value)
	a.bmp.Or(bmp)
}
func (a *aggBitmapOr) Merge(other aggexec.SingleAggFromVarRetVar, get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	a.bmp.Or(other.(*aggBitmapOr).bmp)
}
func (a *aggBitmapOr) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	b, _ := a.bmp.MarshalBinary()
	_ = set(b)
}

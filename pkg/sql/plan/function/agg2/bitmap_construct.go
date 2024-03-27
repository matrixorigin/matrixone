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

func RegisterBitmapConstruct(id int64) {
	aggexec.RegisterDeterminedSingleAgg(
		aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_varbinary.ToType(), false, true),
		newAggBitmapConstruct)
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

func newAggBitmapConstruct() *aggBitmapConstruct {
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
func (a *aggBitmapConstruct) Fill(value uint64, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	a.bmp.Add(uint32(value))
	return nil
}
func (a *aggBitmapConstruct) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	return nil
}
func (a *aggBitmapConstruct) Fills(value uint64, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	if !isNull {
		a.bmp.Add(uint32(value))
	}
	return nil
}
func (a *aggBitmapConstruct) Merge(other aggexec.SingleAggFromFixedRetVar[uint64], get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	a.bmp.Or(other.(*aggBitmapConstruct).bmp)
	return nil
}
func (a *aggBitmapConstruct) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) error {
	res, err := a.bmp.MarshalBinary()
	if err != nil {
		return err
	}
	return set(res)
}

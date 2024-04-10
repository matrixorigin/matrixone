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

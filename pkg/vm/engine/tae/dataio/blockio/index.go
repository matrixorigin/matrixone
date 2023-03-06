// Copyright 2021 Matrix Origin
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

package blockio

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type BlockIndex struct {
	zm *index.ZoneMap
}

func NewZoneMap(typ types.Type) dataio.Index {
	zm := index.NewZoneMap(typ)
	return &BlockIndex{
		zm: zm,
	}
}

func (b *BlockIndex) Contains(key any) (ok bool) {
	return b.zm.Contains(key)
}

func (b *BlockIndex) FastContainsAny(keys containers.Vector) (ok bool) {
	return b.zm.FastContainsAny(keys)
}
func (b *BlockIndex) ContainsAny(keys containers.Vector) (visibility *roaring.Bitmap, ok bool) {
	return b.zm.ContainsAny(keys)
}

func (b *BlockIndex) GetMax() any {
	return b.zm.GetMax()
}

func (b *BlockIndex) GetMin() any {
	return b.zm.GetMin()
}

func (b *BlockIndex) GetBuf() []byte {
	return b.zm.GetBuf()
}

func (b *BlockIndex) Marshal() (buf []byte, err error) {
	return b.zm.Marshal()
}

func (b *BlockIndex) Unmarshal(buf []byte) error {
	return b.zm.Unmarshal(buf)
}

func (b *BlockIndex) String() string {
	return b.zm.String()
}

func (b *BlockIndex) GetType() types.Type {
	return b.zm.GetType()
}

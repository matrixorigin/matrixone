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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

const ZoneMapSize = index.ZMSize

type ZoneMap = index.ZM
type StaticFilter = index.StaticFilter

type IndexData interface {
	Write(writer *ObjectWriter, block BlockObject, idx uint16) error
	GetIdx() uint16
}

type BloomFilter struct {
	alg  uint8
	data any
}

func NewBloomFilter(alg uint8, data any) *BloomFilter {
	bloomFilter := &BloomFilter{
		alg:  alg,
		data: data,
	}
	return bloomFilter
}

func (b *BloomFilter) GetIdx() uint16 {
	return 0
}

func (b *BloomFilter) GetData() any {
	return b.data
}

func (b *BloomFilter) Write(writer *ObjectWriter, block BlockObject, idx uint16) error {
	extent, err := writer.buffer.WriteWithCompress(b.data.([]byte))
	block.ColumnMeta(idx).setBloomFilter(extent)
	return err
}

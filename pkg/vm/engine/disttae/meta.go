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

package disttae

import "github.com/matrixorigin/matrixone/pkg/objectio"

// tae's block metadata, which is currently just an empty one,
// does not serve any purpose When tae submits a concrete structure,
// it will replace this structure with tae's code
type BlockMeta struct {
	header      BlockHeader
	columns     []*ColumnMeta
	localExtent Extent
}

type BlockHeader struct {
	tableId     uint64
	segmentId   uint64
	blockId     uint64
	columnCount uint16
	dummy       [34]byte
	checksum    uint32
}

type ColumnMeta struct {
	typ         uint8
	idx         uint16
	alg         uint8
	location    Extent
	zoneMap     objectio.IndexData
	bloomFilter objectio.IndexData
	dummy       [32]byte
	checksum    uint32
}

type Header struct {
	magic   uint64
	version uint16
	dummy   [22]byte
}

type Extent struct {
	id         uint64
	offset     uint32
	length     uint32
	originSize uint32
}

// type ZoneMap struct {
// 	idx uint16
// 	min []byte
// 	max []byte
// }

// func NewZoneMap(idx uint16, min, max []byte) (*ZoneMap, error) {
// 	zoneMap := &ZoneMap{
// 		idx: idx,
// 		min: min,
// 		max: max,
// 	}
// 	return zoneMap, nil
// }

// func (z *ZoneMap) GetIdx() uint16 {
// 	return z.idx
// }

// func (z *ZoneMap) GetMin() []byte {
// 	return z.min
// }

// func (z *ZoneMap) GetMax() []byte {
// 	return z.max
// }

// type BloomFilter struct {
// 	idx uint16
// 	alg uint8
// 	buf []byte
// }

// func NewBloomFilter(idx uint16, alg uint8, buf []byte) *BloomFilter {
// 	bloomFilter := &BloomFilter{
// 		idx: idx,
// 		alg: alg,
// 		buf: buf,
// 	}
// 	return bloomFilter
// }

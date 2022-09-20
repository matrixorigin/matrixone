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

type BlockMeta struct {
	header  BlockHeader
	columns []*ColumnMeta
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
	zoneMap     ZoneMap
	bloomFilter Extent
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

type ZoneMap struct {
	idx uint16
	min []byte
	max []byte
}

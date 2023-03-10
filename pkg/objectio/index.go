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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/pierrec/lz4"
)

type IndexDataType uint8

const (
	ZoneMapType IndexDataType = iota
	BloomFilterType
)

const ZoneMapMinSize = 32
const ZoneMapMaxSize = 32

type IndexData interface {
	Write(writer *ObjectWriter, block *Block) error
	GetIdx() uint16
}

type ZoneMap struct {
	idx uint16
	buf []byte
}

func NewZoneMap(idx uint16, buf []byte) (IndexData, error) {
	if len(buf) != ZoneMapMinSize+ZoneMapMaxSize {
		return nil, moerr.NewInternalErrorNoCtx("object io: New ZoneMap failed")
	}
	zoneMap := &ZoneMap{
		idx: idx,
		buf: buf,
	}
	return zoneMap, nil
}

func (z *ZoneMap) GetIdx() uint16 {
	return z.idx
}

func (z *ZoneMap) Write(_ *ObjectWriter, block *Block) error {
	var err error
	block.columns[z.idx].(*ColumnBlock).meta.zoneMap = *z
	return err
}

func (z *ZoneMap) GetData() []byte {
	return z.buf
}

type BloomFilter struct {
	idx uint16
	alg uint8
	buf []byte
}

func NewBloomFilter(idx uint16, alg uint8, buf []byte) IndexData {
	bloomFilter := &BloomFilter{
		idx: idx,
		alg: alg,
		buf: buf,
	}
	return bloomFilter
}

func (b *BloomFilter) GetIdx() uint16 {
	return b.idx
}

func (b *BloomFilter) GetData() []byte {
	return b.buf
}

func (b *BloomFilter) Write(writer *ObjectWriter, block *Block) error {
	var err error
	data := make([]byte, lz4.CompressBlockBound(len(b.buf)))
	if data, err = compress.Compress(b.buf, data, compress.Lz4); err != nil {
		return err
	}
	offset, length, err := writer.buffer.Write(data)
	if err != nil {
		return err
	}
	block.columns[b.idx].(*ColumnBlock).meta.bloomFilter.offset = uint32(offset)
	block.columns[b.idx].(*ColumnBlock).meta.bloomFilter.length = uint32(length)
	block.columns[b.idx].(*ColumnBlock).meta.bloomFilter.originSize = uint32(len(b.buf))
	return err
}

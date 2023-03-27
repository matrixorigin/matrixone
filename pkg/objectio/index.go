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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

type ZoneMapUnmarshalFunc = func(buf []byte, t types.Type) (any, error)

type ZoneMap struct {
	idx           uint16
	data          any
	unmarshalFunc ZoneMapUnmarshalFunc
}

func NewZoneMap(idx uint16, data any) (IndexData, error) {
	zoneMap := &ZoneMap{
		idx:  idx,
		data: data,
	}
	return zoneMap, nil
}

func (z *ZoneMap) GetIdx() uint16 {
	return z.idx
}

func (z *ZoneMap) Write(_ *ObjectWriter, block *Block) error {
	var err error
	block.columns[z.idx].meta.zoneMap = *z
	return err
}

func (z *ZoneMap) GetData() any {
	return z.data
}

func (z *ZoneMap) Unmarshal(buf []byte, t types.Type) (err error) {
	if z.unmarshalFunc == nil {
		z.data = buf
		return err
	}
	z.data, err = z.unmarshalFunc(buf, t)
	return err
}

type BloomFilter struct {
	idx  uint16
	alg  uint8
	data any
}

func NewBloomFilter(idx uint16, alg uint8, data any) IndexData {
	bloomFilter := &BloomFilter{
		idx:  idx,
		alg:  alg,
		data: data,
	}
	return bloomFilter
}

func (b *BloomFilter) GetIdx() uint16 {
	return b.idx
}

func (b *BloomFilter) GetData() any {
	return b.data
}

func (b *BloomFilter) Write(writer *ObjectWriter, block *Block) error {
	var err error
	dataLen := len(b.data.([]byte))
	data := make([]byte, lz4.CompressBlockBound(dataLen))
	if data, err = compress.Compress(b.data.([]byte), data, compress.Lz4); err != nil {
		return err
	}
	offset, length, err := writer.buffer.Write(data)
	if err != nil {
		return err
	}
	block.columns[b.idx].meta.bloomFilter.offset = uint32(offset)
	block.columns[b.idx].meta.bloomFilter.length = uint32(length)
	block.columns[b.idx].meta.bloomFilter.originSize = uint32(dataLen)
	return err
}

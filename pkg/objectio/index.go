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

const ZoneMapSize = 64

type IndexData interface {
	Write(writer *ObjectWriter, block *Block, idx uint16) error
	GetIdx() uint16
}

type ZoneMapUnmarshalFunc = func(buf []byte, t types.Type) (any, error)

type ZoneMap struct {
	data []byte
}

func NewZoneMap(zm []byte) (IndexData, error) {
	zoneMap := &ZoneMap{
		data: zm,
	}
	return zoneMap, nil
}

func (z *ZoneMap) GetIdx() uint16 {
	return 0
}

func (z *ZoneMap) Write(_ *ObjectWriter, block *Block, idx uint16) error {
	block.meta.ColumnMeta(idx).setZoneMap(*z)
	return nil
}

func (z *ZoneMap) GetData() any {
	return z.data
}

type BloomFilter struct {
	alg  uint8
	data any
}

func NewBloomFilter(alg uint8, data any) IndexData {
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

func (b *BloomFilter) Write(writer *ObjectWriter, block *Block, idx uint16) error {
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
	extent := Extent{
		offset:     uint32(offset),
		length:     uint32(length),
		originSize: uint32(dataLen),
	}
	block.meta.ColumnMeta(idx).setBloomFilter(extent)
	return err
}

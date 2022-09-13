package objectio

import "errors"

const ZoneMapMinSize = 32
const ZoneMapMaxSize = 32

type IndexData interface {
	Write(writer *ObjectWriter, block *Block) error
	GetIdx() uint16
}

type ZoneMap struct {
	idx uint16
	min []byte
	max []byte
}

func NewZoneMap(idx uint16, min, max []byte) (IndexData, error) {
	if len(min) != ZoneMapMinSize || len(max) != ZoneMapMaxSize {
		return nil, errors.New("object io: New ZoneMap failed")
	}
	zoneMap := &ZoneMap{
		idx: idx,
		min: min,
		max: max,
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

func (b *BloomFilter) Write(writer *ObjectWriter, block *Block) error {
	var err error
	offset, length, err := writer.buffer.Write(b.buf)
	if err != nil {
		return err
	}
	block.columns[b.idx].(*ColumnBlock).meta.bloomFilter.offset = uint32(offset)
	block.columns[b.idx].(*ColumnBlock).meta.bloomFilter.length = uint32(length)
	block.columns[b.idx].(*ColumnBlock).meta.bloomFilter.originSize = uint32(length)
	return err
}

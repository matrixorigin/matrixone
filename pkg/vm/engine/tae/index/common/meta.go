package common

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

type IndexType uint8

const (
	InvalidIndexType IndexType = iota
	BlockZoneMapIndex
	SegmentZoneMapIndex
	StaticFilterIndex
	ARTIndex
)

type CompressType uint8

const (
	Plain CompressType = iota
	Lz4
)

func Compress(raw []byte, ctyp CompressType) []byte {
	return raw
}

func Decompress(compressed []byte, ctyp CompressType) []byte {
	return compressed
}

type IndexMeta struct {
	IdxType IndexType
	CompType CompressType
	ColIdx uint16
	PartOffset uint32
	StartOffset uint32
	Size uint32
	RawSize uint32
}

func NewEmptyIndexMeta() *IndexMeta {
	return &IndexMeta{
		IdxType: InvalidIndexType,
		CompType: Plain,
	}
}

func (meta *IndexMeta) SetIndexType(typ IndexType) {
	meta.IdxType = typ
}

func (meta *IndexMeta) SetCompressType(typ CompressType) {
	meta.CompType = typ
}

func (meta *IndexMeta) SetIndexedColumn(colIdx uint16) {
	meta.ColIdx = colIdx
}

func (meta *IndexMeta) SetPartOffset(offset uint32) {
	meta.PartOffset = offset
}

func (meta *IndexMeta) SetStartOffset(offset uint32) {
	meta.StartOffset = offset
}

func (meta *IndexMeta) SetSize(raw, exact uint32) {
	meta.RawSize = raw
	meta.Size = exact
}

func (meta *IndexMeta) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint8(uint8(meta.IdxType)))
	buf.Write(encoding.EncodeUint8(uint8(meta.CompType)))
	buf.Write(encoding.EncodeUint16(meta.ColIdx))
	buf.Write(encoding.EncodeUint32(meta.PartOffset))
	buf.Write(encoding.EncodeUint32(meta.StartOffset))
	buf.Write(encoding.EncodeUint32(meta.Size))
	buf.Write(encoding.EncodeUint32(meta.RawSize))
	return buf.Bytes(), nil
}

func (meta *IndexMeta) Unmarshal(buf []byte) error {
	meta.IdxType = IndexType(encoding.DecodeUint8(buf[:1]))
	buf = buf[1:]
	meta.CompType = CompressType(encoding.DecodeUint8(buf[1:]))
	buf = buf[1:]
	meta.ColIdx = encoding.DecodeUint16(buf[:2])
	buf = buf[2:]
	meta.PartOffset = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	meta.StartOffset = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	meta.Size = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	meta.RawSize = encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	return nil
}

type IndicesMeta struct {
	Metas []IndexMeta
}

func NewEmptyIndicesMeta() *IndicesMeta {
	return &IndicesMeta{
		Metas:        make([]IndexMeta, 0),
	}
}

func (metas *IndicesMeta) AddIndex(meta IndexMeta) {
	metas.Metas = append(metas.Metas, meta)
}

func (metas *IndicesMeta) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint8(uint8(len(metas.Metas))))
	for _, meta := range metas.Metas {
		v, err := meta.Marshal()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(v))))
		buf.Write(v)
	}
	return buf.Bytes(), nil
}

func (metas *IndicesMeta) Unmarshal(buf []byte) error {
	count := encoding.DecodeUint8(buf[:1])
	buf = buf[1:]
	metas.Metas = make([]IndexMeta, 0)
	for i := uint8(0); i < count; i++ {
		size := encoding.DecodeUint32(buf[:4])
		buf = buf[4:]
		metaBuf := buf[:size]
		buf = buf[size:]
		var meta IndexMeta
		if err := meta.Unmarshal(metaBuf); err != nil {
			return err
		}
		metas.Metas = append(metas.Metas, meta)
	}
	return nil
}

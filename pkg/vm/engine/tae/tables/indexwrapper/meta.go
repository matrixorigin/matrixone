// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type IndexType uint8

const (
	InvalidIndexType IndexType = iota
	BlockZoneMapIndex
	SegmentZoneMapIndex
	StaticFilterIndex
	ARTIndex
)

type IndexMeta struct {
	IdxType     IndexType
	CompType    common.CompressType
	ColIdx      uint16
	InternalIdx uint16
	Size        uint32
	RawSize     uint32
}

func NewEmptyIndexMeta() *IndexMeta {
	return &IndexMeta{
		IdxType:  InvalidIndexType,
		CompType: common.Plain,
	}
}

func (meta *IndexMeta) SetIndexType(typ IndexType) {
	meta.IdxType = typ
}

func (meta *IndexMeta) SetCompressType(typ common.CompressType) {
	meta.CompType = typ
}

func (meta *IndexMeta) SetIndexedColumn(colIdx uint16) {
	meta.ColIdx = colIdx
}

//func (meta *IndexMeta) SetPartOffset(offset uint32) {
//	meta.PartOffset = offset
//}

//func (meta *IndexMeta) SetStartOffset(offset uint32) {
//	meta.StartOffset = offset
//}

func (meta *IndexMeta) SetInternalIndex(idx uint16) {
	meta.InternalIdx = idx
}

func (meta *IndexMeta) SetSize(raw, exact uint32) {
	meta.RawSize = raw
	meta.Size = exact
}

func (meta *IndexMeta) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(types.EncodeFixed(uint8(meta.IdxType)))
	buf.Write(types.EncodeFixed(uint8(meta.CompType)))
	buf.Write(types.EncodeFixed(meta.ColIdx))
	buf.Write(types.EncodeFixed(meta.InternalIdx))
	buf.Write(types.EncodeFixed(meta.Size))
	buf.Write(types.EncodeFixed(meta.RawSize))
	return buf.Bytes(), nil
}

func (meta *IndexMeta) Unmarshal(buf []byte) error {
	meta.IdxType = IndexType(types.DecodeFixed[uint8](buf[:1]))
	buf = buf[1:]
	meta.CompType = common.CompressType(types.DecodeFixed[uint8](buf[1:]))
	buf = buf[1:]
	meta.ColIdx = types.DecodeFixed[uint16](buf[:2])
	buf = buf[2:]
	meta.InternalIdx = types.DecodeFixed[uint16](buf[:2])
	buf = buf[2:]
	meta.Size = types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	meta.RawSize = types.DecodeFixed[uint32](buf[:4])
	return nil
}

type IndicesMeta struct {
	Metas []IndexMeta
}

func NewEmptyIndicesMeta() *IndicesMeta {
	return &IndicesMeta{
		Metas: make([]IndexMeta, 0),
	}
}

func (metas *IndicesMeta) AddIndex(meta ...IndexMeta) {
	metas.Metas = append(metas.Metas, meta...)
}

func (metas *IndicesMeta) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(types.EncodeFixed(uint8(len(metas.Metas))))
	for _, meta := range metas.Metas {
		v, err := meta.Marshal()
		if err != nil {
			return nil, err
		}
		buf.Write(types.EncodeFixed(uint32(len(v))))
		buf.Write(v)
	}
	return buf.Bytes(), nil
}

func (metas *IndicesMeta) Unmarshal(buf []byte) error {
	count := types.DecodeFixed[uint8](buf[:1])
	buf = buf[1:]
	metas.Metas = make([]IndexMeta, 0)
	for i := uint8(0); i < count; i++ {
		size := types.DecodeFixed[uint32](buf[:4])
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

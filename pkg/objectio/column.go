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
	"bytes"
	"context"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ColumnBlock struct {
	meta  *ColumnMeta
	block *Block
}

func NewColumnBlock(idx uint16, block *Block) ColumnObject {
	meta := &ColumnMeta{
		idx:         idx,
		zoneMap:     &index.ZoneMap{},
		bloomFilter: Extent{},
	}
	col := &ColumnBlock{
		block: block,
		meta:  meta,
	}
	return col
}

func (cb *ColumnBlock) GetData() (*fileservice.IOVector, error) {
	var err error
	data := &fileservice.IOVector{
		FilePath: cb.block.object.name,
		Entries:  make([]fileservice.IOEntry, 1),
	}
	data.Entries[0] = fileservice.IOEntry{
		Offset: int(cb.meta.location.Offset()),
		Size:   int(cb.meta.location.Length()),
	}
	err = cb.block.object.fs.Read(context.Background(), data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cb *ColumnBlock) GetIndex() (*fileservice.IOVector, error) {
	var err error
	data := &fileservice.IOVector{
		FilePath: cb.block.object.name,
		Entries:  make([]fileservice.IOEntry, 1),
	}
	data.Entries[0] = fileservice.IOEntry{
		Offset: int(cb.meta.bloomFilter.Offset()),
		Size:   int(cb.meta.bloomFilter.Length()),
	}
	err = cb.block.object.fs.Read(context.Background(), data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cb *ColumnBlock) GetMeta() *ColumnMeta {
	return cb.meta
}

func (cb *ColumnBlock) MarshalMeta() ([]byte, error) {
	var (
		err    error
		buffer bytes.Buffer
	)
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.typ); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.alg); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.idx); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.location.Offset()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.location.Length()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.location.OriginSize()); err != nil {
		return nil, err
	}
	/*if err = binary.Write(&buffer, binary.BigEndian, cb.meta.zoneMap.GetMin()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.zoneMap.GetMax()); err != nil {
		return nil, err
	}*/
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.bloomFilter.Offset()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.bloomFilter.Length()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, cb.meta.bloomFilter.OriginSize()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}
	reserved := make([]byte, 32)
	if err = binary.Write(&buffer, binary.BigEndian, reserved); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (cb *ColumnBlock) UnMarshalMate(cache *bytes.Buffer) error {
	var err error
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.typ); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.alg); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.idx); err != nil {
		return err
	}
	cb.meta.location = Extent{}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.location.offset); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.location.length); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.location.originSize); err != nil {
		return err
	}
	cb.meta.bloomFilter = Extent{}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.bloomFilter.offset); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.bloomFilter.length); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.bloomFilter.originSize); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &cb.meta.checksum); err != nil {
		return err
	}
	reserved := make([]byte, 32)
	if err = binary.Read(cache, binary.BigEndian, &reserved); err != nil {
		return err
	}
	return err
}

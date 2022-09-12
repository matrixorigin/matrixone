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
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"sync"
)

type ObjectWriter struct {
	sync.RWMutex
	object *Object
	blocks map[int]BlockObject
	buffer *ObjectBuffer
	name   string
	lastId int
}

func NewObjectWriter(name string, fs fileservice.FileService) (Writer, error) {
	object := NewObject(name, fs)
	writer := &ObjectWriter{
		name:   name,
		object: object,
		buffer: NewObjectBuffer(name),
		blocks: make(map[int]BlockObject),
		lastId: 0,
	}
	err := writer.WriteHeader()
	return writer, err
}

func (w *ObjectWriter) WriteHeader() error {
	var (
		err    error
		header bytes.Buffer
	)
	h := Header{magic: Magic, version: Version}
	if err = binary.Write(&header, binary.BigEndian, h.magic); err != nil {
		return err
	}
	if err = binary.Write(&header, binary.BigEndian, h.version); err != nil {
		return err
	}
	reserved := make([]byte, 22)
	if err = binary.Write(&header, binary.BigEndian, reserved); err != nil {
		return err
	}
	_, _, err = w.buffer.Write(header.Bytes())
	return err
}

func (w *ObjectWriter) Write(batch *batch.Batch) (BlockObject, error) {
	block := NewBlock(batch, w.object)
	w.AddBlock(block.(*Block))
	for i, vec := range batch.Vecs {
		buf, err := vec.Show()
		if err != nil {
			return nil, err
		}
		offset, length, err := w.buffer.Write(buf)
		if err != nil {
			return nil, err
		}
		block.(*Block).columns[i].(*ColumnBlock).meta.location = Extent{
			id:         block.GetMeta().header.blockId,
			offset:     uint32(offset),
			length:     uint32(length),
			originSize: uint32(length),
		}
	}
	return block, nil
}

func (w *ObjectWriter) WriteIndex(fd int, idx uint16, buf []byte) error {
	var err error
	block := w.GetBlock(fd)
	if block == nil || block.columns[idx] == nil {
		return ErrNotFound
	}
	offset, length, err := w.buffer.Write(buf)
	if err != nil {
		return err
	}
	block.columns[idx].(*ColumnBlock).meta.bloomFilter.offset = uint32(offset)
	block.columns[idx].(*ColumnBlock).meta.bloomFilter.length = uint32(length)
	block.columns[idx].(*ColumnBlock).meta.bloomFilter.originSize = uint32(length)
	return err
}

func (w *ObjectWriter) WriteEnd() (map[int]BlockObject, error) {
	var err error
	w.RLock()
	defer w.RUnlock()
	var buf bytes.Buffer
	for i, block := range w.blocks {
		meta, err := block.(*Block).MarshalMeta()
		if err != nil {
			return nil, err
		}
		offset, length, err := w.buffer.Write(meta)
		if err != nil {
			return nil, err
		}
		w.blocks[i].(*Block).extent = Extent{
			id:         block.(*Block).header.blockId,
			offset:     uint32(offset),
			length:     uint32(length),
			originSize: uint32(length),
		}
		if err = binary.Write(&buf, binary.BigEndian, w.blocks[i].(*Block).extent.Offset()); err != nil {
			return nil, err
		}
		if err = binary.Write(&buf, binary.BigEndian, w.blocks[i].(*Block).extent.Length()); err != nil {
			return nil, err
		}
		if err = binary.Write(&buf, binary.BigEndian, w.blocks[i].(*Block).extent.OriginSize()); err != nil {
			return nil, err
		}
	}
	if err = binary.Write(&buf, binary.BigEndian, uint8(0)); err != nil {
		return nil, err
	}
	if err = binary.Write(&buf, binary.BigEndian, uint32(len(w.blocks))); err != nil {
		return nil, err
	}
	if err = binary.Write(&buf, binary.BigEndian, uint64(Magic)); err != nil {
		return nil, err
	}
	_, _, err = w.buffer.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return w.blocks, err
}

// Sync is for testing
func (w *ObjectWriter) Sync(dir string) error {
	err := w.object.fs.Write(nil, w.buffer.GetData())
	if err != nil {
		return err
	}
	return err
}

func (w *ObjectWriter) AddBlock(block *Block) {
	w.Lock()
	defer w.Unlock()
	block.fd = w.lastId
	w.blocks[block.fd] = block
	w.lastId++
}

func (w *ObjectWriter) GetBlock(fd int) *Block {
	w.Lock()
	defer w.Unlock()
	return w.blocks[fd].(*Block)
}

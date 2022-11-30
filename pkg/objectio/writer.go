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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/pierrec/lz4"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type ObjectWriter struct {
	sync.RWMutex
	object *Object
	blocks []BlockObject
	buffer *ObjectBuffer
	name   string
	lastId uint32
}

func NewObjectWriter(name string, fs fileservice.FileService) (Writer, error) {
	object := NewObject(name, fs)
	writer := &ObjectWriter{
		name:   name,
		object: object,
		buffer: NewObjectBuffer(name),
		blocks: make([]BlockObject, 0),
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
	if err = binary.Write(&header, endian, h.magic); err != nil {
		return err
	}
	if err = binary.Write(&header, endian, h.version); err != nil {
		return err
	}
	if err = binary.Write(&header, endian, h.dummy); err != nil {
		return err
	}
	_, _, err = w.buffer.Write(header.Bytes())
	return err
}

func (w *ObjectWriter) Write(batch *batch.Batch) (BlockObject, error) {
	block := NewBlock(uint16(len(batch.Vecs)), w.object, w.name)
	w.AddBlock(block.(*Block))
	for i, vec := range batch.Vecs {
		buf, err := vec.Show()
		if err != nil {
			return nil, err
		}
		originSize := len(buf)
		// TODO:Now by default, lz4 compression must be used for Write,
		// and parameters need to be passed in later to determine the compression type
		data := make([]byte, lz4.CompressBlockBound(originSize))
		if buf, err = compress.Compress(buf, data, compress.Lz4); err != nil {
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
			originSize: uint32(originSize),
		}
		block.(*Block).columns[i].(*ColumnBlock).meta.alg = compress.Lz4
	}
	return block, nil
}

func (w *ObjectWriter) WriteIndex(fd BlockObject, index IndexData) error {
	var err error

	block := w.GetBlock(fd.GetID())
	if block == nil || block.columns[index.GetIdx()] == nil {
		return moerr.NewInternalError("object io: not found")
	}
	err = index.Write(w, block)
	return err
}

func (w *ObjectWriter) WriteEnd(ctx context.Context) ([]BlockObject, error) {
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
		if err = binary.Write(&buf, endian, w.blocks[i].(*Block).extent.Offset()); err != nil {
			return nil, err
		}
		if err = binary.Write(&buf, endian, w.blocks[i].(*Block).extent.Length()); err != nil {
			return nil, err
		}
		if err = binary.Write(&buf, endian, w.blocks[i].(*Block).extent.OriginSize()); err != nil {
			return nil, err
		}
	}
	if err = binary.Write(&buf, endian, uint32(len(w.blocks))); err != nil {
		return nil, err
	}
	if err = binary.Write(&buf, endian, uint64(Magic)); err != nil {
		return nil, err
	}
	_, _, err = w.buffer.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	err = w.Sync(ctx)
	if err != nil {
		return nil, err
	}

	// The buffer needs to be released at the end of WriteEnd
	// Because the outside may hold this writer
	// After WriteEnd is called, no more data can be written
	w.buffer = nil
	return w.blocks, err
}

// Sync is for testing
func (w *ObjectWriter) Sync(ctx context.Context) error {
	err := w.object.fs.Write(ctx, w.buffer.GetData())
	if err != nil {
		return err
	}
	return err
}

func (w *ObjectWriter) AddBlock(block *Block) {
	w.Lock()
	defer w.Unlock()
	block.id = w.lastId
	w.blocks = append(w.blocks, block)
	//w.blocks[block.id] = block
	w.lastId++
}

func (w *ObjectWriter) GetBlock(id uint32) *Block {
	w.Lock()
	defer w.Unlock()
	return w.blocks[id].(*Block)
}

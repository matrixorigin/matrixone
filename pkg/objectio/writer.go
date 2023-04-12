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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type ObjectWriter struct {
	sync.RWMutex
	object   *Object
	blocks   []BlockObject
	totalRow uint32
	colmeta  []ObjectColumnMeta
	buffer   *ObjectBuffer
	nameStr  string
	lastId   uint32
	name     ObjectName
}

func NewObjectWriter(name string, fs fileservice.FileService) (*ObjectWriter, error) {
	object := NewObject(name, fs)
	writer := &ObjectWriter{
		nameStr: name,
		object:  object,
		buffer:  NewObjectBuffer(name),
		blocks:  make([]BlockObject, 0),
		lastId:  0,
	}
	err := writer.WriteHeader()
	return writer, err
}

func NewObjectWriterNew(name ObjectName, fs fileservice.FileService) (*ObjectWriter, error) {
	nameStr := name.String()
	object := NewObject(nameStr, fs)
	writer := &ObjectWriter{
		nameStr: nameStr,
		name:    name,
		object:  object,
		buffer:  NewObjectBuffer(nameStr),
		blocks:  make([]BlockObject, 0),
		lastId:  0,
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
	header.Write(types.EncodeFixed(h.magic))
	header.Write(types.EncodeFixed(h.version))
	header.Write(make([]byte, 22))
	_, _, err = w.buffer.Write(header.Bytes())
	return err
}

func (w *ObjectWriter) Write(batch *batch.Batch) (BlockObject, error) {
	block := NewBlock(uint16(len(batch.Vecs)))
	w.AddBlock(block)
	for i, vec := range batch.Vecs {
		buf, err := vec.MarshalBinary()
		if err != nil {
			return nil, err
		}
		var ext *Extent
		if ext, err = w.buffer.WriteWithCompress(buf); err != nil {
			return nil, err
		}
		ext.id = block.GetID()
		block.ColumnMeta(uint16(i)).setLocation(ext)
		block.ColumnMeta(uint16(i)).setAlg(compress.Lz4)
		block.ColumnMeta(uint16(i)).setType(uint8(vec.GetType().Oid))
	}
	return block, nil
}

func (w *ObjectWriter) UpdateBlockZM(blkIdx, colIdx int, zm ZoneMap) {
	w.blocks[blkIdx].ColumnMeta(uint16(colIdx)).setZoneMap(zm)
}

func (w *ObjectWriter) WriteBF(blkIdx, colIdx int, buf []byte) (err error) {
	var ext *Extent
	if ext, err = w.buffer.WriteWithCompress(buf); err != nil {
		return
	}
	meta := w.blocks[blkIdx].ColumnMeta(uint16(colIdx))
	meta.setBloomFilter(ext)
	return
}

func (w *ObjectWriter) WriteObjectMeta(ctx context.Context, totalrow uint32, metas []ObjectColumnMeta) {
	w.totalRow = totalrow
	w.colmeta = metas
}

func (w *ObjectWriter) WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error) {
	var err error
	w.RLock()
	defer w.RUnlock()
	var columnCount uint16
	columnCount = 0
	if len(w.blocks) == 0 {
		logutil.Warn("object io: no block needs to be written")
	} else {
		columnCount = w.blocks[0].GetColumnCount()
	}

	blockCount := uint32(len(w.blocks))
	objectMeta := BuildObjectMeta(columnCount)
	objectMeta.BlockHeader().SetBlockID(blockCount)
	objectMeta.BlockHeader().SetRows(w.totalRow)
	objectMeta.BlockHeader().SetColumnCount(columnCount)
	blockIndex := BuildBlockIndex(blockCount)
	blockIndex.SetBlockCount(blockCount)
	start := int(objectMeta.Length())
	start += int(blockIndex.Length())
	length := 0

	// write block meta
	metabuf := &bytes.Buffer{}
	for i, block := range w.blocks {
		var n int
		meta := block.MarshalMeta()
		if err != nil {
			return nil, err
		}
		if n, err = metabuf.Write(meta); err != nil {
			return nil, err
		}
		blockIndex.SetBlockMetaPos(uint32(i), uint32(start), uint32(start+n))
		start += n
	}

	// write column meta
	for i, colmeta := range w.colmeta {
		objectMeta.AddColumnMeta(uint16(i), colmeta)
	}
	// begin write
	start, n, err := w.buffer.Write(objectMeta)
	if err != nil {
		return nil, err
	}
	length += n
	_, n, err = w.buffer.Write(blockIndex)
	if err != nil {
		return nil, err
	}
	length += n
	_, n, err = w.buffer.Write(metabuf.Bytes())
	if err != nil {
		return nil, err
	}
	length += n
	extent := &Extent{
		offset:     uint32(start),
		length:     uint32(length),
		originSize: uint32(length),
	}
	objectMeta.BlockHeader().SetMetaLocation(extent)
	// write footer
	footer := Footer{
		metaStart: uint32(start),
		metaLen:   uint32(length),
		magic:     Magic,
	}

	if _, _, err = w.buffer.Write(footer.Marshal()); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	for i := range w.blocks {
		header := w.blocks[i].BlockHeader()
		extent := &Extent{
			id:         uint32(i),
			offset:     uint32(start),
			length:     uint32(length),
			originSize: uint32(length),
		}
		header.SetMetaLocation(extent)
	}
	err = w.Sync(ctx, items...)
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
func (w *ObjectWriter) Sync(ctx context.Context, items ...WriteOptions) error {
	w.buffer.SetDataOptions(items...)
	// if a compact task is rollbacked, it may leave a written file in fs
	// here we just delete it and write again
	err := w.object.fs.Write(ctx, w.buffer.GetData())
	if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
		if err = w.object.fs.Delete(ctx, w.nameStr); err != nil {
			return err
		}
		return w.object.fs.Write(ctx, w.buffer.GetData())
	}
	return err
}

func (w *ObjectWriter) AddBlock(block BlockObject) {
	w.Lock()
	defer w.Unlock()
	block.BlockHeader().SetBlockID(w.lastId)
	w.blocks = append(w.blocks, block)
	w.lastId++
}

func (w *ObjectWriter) GetBlock(id uint32) BlockObject {
	w.Lock()
	defer w.Unlock()
	return w.blocks[id]
}

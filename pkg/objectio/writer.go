package objectio

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"sync"
)

type ObjectWriter struct {
	sync.RWMutex
	object *Object
	blocks map[uint64]*Block
	buffer *ObjectBuffer
	name   string
}

func NewObjectWriter(name string) (*ObjectWriter, error) {
	writer := &ObjectWriter{
		name:   name,
		buffer: NewObjectBuffer(name),
		blocks: make(map[uint64]*Block),
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

func (w *ObjectWriter) Write(id *common.ID, batch *batch.Batch) error {
	block := NewBlock(id, batch)
	w.AddBlock(block)
	//block := w.GetBlock(id, batch)
	for i, vec := range batch.Vecs {
		buf, err := vec.Show()
		if err != nil {
			return err
		}
		offset, length, err := w.buffer.Write(buf)
		if err != nil {
			return err
		}
		block.columns[i].meta.location = &Extent{
			offset:     uint32(offset),
			length:     uint32(length),
			originSize: uint32(length),
		}
	}
	return nil
}

func (w *ObjectWriter) WriteEnd() error {
	var err error
	w.RLock()
	defer w.RUnlock()
	extents := make([]Extent, 0)
	for _, block := range w.blocks {
		meta, err := block.ShowMeta()
		if err != nil {
			return err
		}
		offset, length, err := w.buffer.Write(meta)
		if err != nil {
			return err
		}
		extents = append(extents, Extent{
			offset:     uint32(offset),
			length:     uint32(length),
			originSize: uint32(length),
		})
	}
	var buf bytes.Buffer
	for _, extent := range extents {
		if err = binary.Write(&buf, binary.BigEndian, extent.Offset()); err != nil {
			return err
		}
		if err = binary.Write(&buf, binary.BigEndian, extent.Length()); err != nil {
			return err
		}
		if err = binary.Write(&buf, binary.BigEndian, extent.OriginSize()); err != nil {
			return err
		}
	}
	if err = binary.Write(&buf, binary.BigEndian, uint8(0)); err != nil {
		return err
	}
	if err = binary.Write(&buf, binary.BigEndian, uint32(len(extents))); err != nil {
		return err
	}
	if err = binary.Write(&buf, binary.BigEndian, uint64(Magic)); err != nil {
		return err
	}
	_, _, err = w.buffer.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return err
}

// Sync is for testing
func (w *ObjectWriter) Sync(dir string) error {
	var err error
	w.object, err = NewObject("local", dir)
	if err != nil {
		return err
	}
	err = w.object.oFile.Write(nil, w.buffer.GetData())
	if err != nil {
		return err
	}
	return err
}

func (w *ObjectWriter) AddBlock(block *Block) {
	w.Lock()
	defer w.Unlock()
	if w.blocks[block.header.blockId] != nil {
		panic(any("Write duplicate block"))
	}
	w.blocks[block.header.blockId] = block
}

func (w *ObjectWriter) GetBlock(id *common.ID, batch *batch.Batch) *Block {
	w.Lock()
	defer w.Unlock()
	block := w.blocks[id.BlockID]
	if block != nil {
		return block
	}
	block = NewBlock(id, batch)
	w.blocks[id.BlockID] = block
	return block
}

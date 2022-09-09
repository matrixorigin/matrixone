package objectio

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"sync"
)

type ObjectWriter struct {
	sync.RWMutex
	object *Object
	blocks map[int]*Block
	buffer *ObjectBuffer
	name   string
	lastId int
}

func NewObjectWriter(name string) (Writer, error) {
	writer := &ObjectWriter{
		name:   name,
		buffer: NewObjectBuffer(name),
		blocks: make(map[int]*Block),
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

func (w *ObjectWriter) Write(batch *batch.Batch) (int, error) {
	block := NewBlock(batch)
	w.AddBlock(block)
	for i, vec := range batch.Vecs {
		buf, err := vec.Show()
		if err != nil {
			return 0, err
		}
		offset, length, err := w.buffer.Write(buf)
		if err != nil {
			return 0, err
		}
		block.columns[i].meta.location = Extent{
			id:         block.header.blockId,
			offset:     uint32(offset),
			length:     uint32(length),
			originSize: uint32(length),
		}
	}
	return block.fd, nil
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
	block.columns[idx].meta.bloomFilter.offset = uint32(offset)
	block.columns[idx].meta.bloomFilter.length = uint32(length)
	block.columns[idx].meta.bloomFilter.originSize = uint32(length)
	return err
}

func (w *ObjectWriter) WriteEnd() ([]Extent, error) {
	var err error
	w.RLock()
	defer w.RUnlock()
	extents := make([]Extent, 0)
	for _, block := range w.blocks {
		meta, err := block.MarshalMeta()
		if err != nil {
			return nil, err
		}
		offset, length, err := w.buffer.Write(meta)
		if err != nil {
			return nil, err
		}
		extents = append(extents, Extent{
			id:         block.header.blockId,
			offset:     uint32(offset),
			length:     uint32(length),
			originSize: uint32(length),
		})
	}
	var buf bytes.Buffer
	for _, extent := range extents {
		if err = binary.Write(&buf, binary.BigEndian, extent.Offset()); err != nil {
			return nil, err
		}
		if err = binary.Write(&buf, binary.BigEndian, extent.Length()); err != nil {
			return nil, err
		}
		if err = binary.Write(&buf, binary.BigEndian, extent.OriginSize()); err != nil {
			return nil, err
		}
	}
	if err = binary.Write(&buf, binary.BigEndian, uint8(0)); err != nil {
		return nil, err
	}
	if err = binary.Write(&buf, binary.BigEndian, uint32(len(extents))); err != nil {
		return nil, err
	}
	if err = binary.Write(&buf, binary.BigEndian, uint64(Magic)); err != nil {
		return nil, err
	}
	_, _, err = w.buffer.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return extents, err
}

// Sync is for testing
func (w *ObjectWriter) Sync(dir string) error {
	var err error
	w.object, err = NewObject(w.name, dir)
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
	block.fd = w.lastId
	w.blocks[block.fd] = block
	w.lastId++
}

func (w *ObjectWriter) GetBlock(fd int) *Block {
	w.Lock()
	defer w.Unlock()
	return w.blocks[fd]
}

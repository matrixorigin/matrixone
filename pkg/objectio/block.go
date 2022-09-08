package objectio

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

type Block struct {
	id      uint64
	header  *BlockHeader
	columns []*ColumnBlock
	data    *batch.Batch
}

func NewBlock(batch *batch.Batch) *Block {
	header := &BlockHeader{
		columnCount: uint16(len(batch.Attrs)),
	}
	block := &Block{
		header:  header,
		data:    batch,
		columns: make([]*ColumnBlock, len(batch.Attrs)),
	}
	for i := range block.columns {
		block.columns[i] = NewColumnBlock(uint16(i))
	}
	return block
}

func (b *Block) ShowMeta() ([]byte, error) {
	var (
		err    error
		buffer bytes.Buffer
	)
	// write header
	if err = binary.Write(&buffer, binary.BigEndian, b.header.tableId); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, b.header.segmentId); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, b.header.blockId); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, b.header.columnCount); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}
	reserved := make([]byte, 34)
	if err = binary.Write(&buffer, binary.BigEndian, reserved); err != nil {
		return nil, err
	}
	// write columns meta
	for _, column := range b.columns {
		columnMeta, err := column.ShowMeta()
		if err != nil {
			return nil, err
		}
		if err = binary.Write(&buffer, binary.BigEndian, columnMeta); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (b *Block) UnShowMeta(data []byte) error {
	var err error
	cache := bytes.NewBuffer(data)
	b.header = &BlockHeader{}
	if err = binary.Read(cache, binary.BigEndian, &b.header.tableId); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &b.header.segmentId); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &b.header.blockId); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &b.header.columnCount); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &b.header.checksum); err != nil {
		return err
	}
	reserved := make([]byte, 34)
	if err = binary.Read(cache, binary.BigEndian, &reserved); err != nil {
		return err
	}
	b.columns = make([]*ColumnBlock, b.header.columnCount)
	for i, _ := range b.columns {
		b.columns[i] = NewColumnBlock(uint16(i))
		err = b.columns[i].UnShowMeta(cache.Bytes())
		if err != nil {
			return err
		}
	}
	return err
}

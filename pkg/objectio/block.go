package objectio

import (
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Block struct {
	header  *BlockHeader
	columns []*ColumnBlock
	data    *batch.Batch
}

func NewBlock(id *common.ID, batch *batch.Batch) *Block {
	header := &BlockHeader{
		tableId:     id.TableID,
		segmentId:   id.SegmentID,
		blockId:     id.BlockID,
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

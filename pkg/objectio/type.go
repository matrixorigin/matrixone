package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// Writer is to virtualize batches into multiple blocks
// and write them into filefservice at one time
type Writer interface {
	// Write writes one batch to the Buffer at a time,
	// one batch corresponds to a virtual block,
	// and returns the handle of the block.
	Write(batch *batch.Batch) (BlockObject, error)

	// WriteIndex is the index of the column in the block written to the block's handle.
	// fd is the handle of the block
	// idx is the column to which the index is written
	// buf is the data to write to the index
	WriteIndex(fd int, idx uint16, buf []byte) error

	// WriteEnd is to write multiple batches written to
	// the buffer to the fileservice at one time
	WriteEnd() (map[int]BlockObject, error)
}

// Reader is to read data from fileservice
type Reader interface {
	// Read is to read columns data of a block from fileservice at one time
	// extent is location of the block meta
	// idxs is the column serial number of the data to be read
	Read(extent Extent, idxs []uint16) (*fileservice.IOVector, error)

	// ReadMeta is the meta that reads a block
	// extent is location of the block meta
	ReadMeta(extent Extent) (*Block, error)

	// ReadIndex is the index data of the read columns
	ReadIndex(extent Extent, idxs []uint16) (*fileservice.IOVector, error)
}

type BlockObject interface {
	GetColumn(idx uint16) (ColumnObject, error)
	GetRows() (uint32, error)
	GetMeta() *BlockMeta
	GetExtent() Extent
}

type ColumnObject interface {
	GetData() (*fileservice.IOVector, error)
	GetIndex() (*fileservice.IOVector, error)
	GetMeta() *ColumnMeta
}

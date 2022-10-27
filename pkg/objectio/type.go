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
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"

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
	// block is the handle of the block
	// idx is the column to which the index is written
	// buf is the data to write to the index
	WriteIndex(block BlockObject, index IndexData) error

	// WriteEnd is to write multiple batches written to
	// the buffer to the fileservice at one time
	WriteEnd() ([]BlockObject, error)
}

// Reader is to read data from fileservice
type Reader interface {
	// Read is to read columns data of a block from fileservice at one time
	// extent is location of the block meta
	// idxs is the column serial number of the data to be read
	Read(extents Extent, idxs []uint16, m *mpool.MPool) (*fileservice.IOVector, error)

	// ReadMeta is the meta that reads a block
	// extent is location of the block meta
	ReadMeta(extent []Extent, m *mpool.MPool) ([]BlockObject, error)

	// ReadIndex is the index data of the read columns
	ReadIndex(extent Extent, idxs []uint16, dataType IndexDataType, m *mpool.MPool) ([]IndexData, error)

	// ReadAllMeta is read the meta of all blocks in an object
	ReadAllMeta(fileSize int64, m *mpool.MPool) ([]BlockObject, error)
}

// BlockObject is a batch written to fileservice
type BlockObject interface {
	// GetColumn gets a ColumnObject with idx
	GetColumn(idx uint16) (ColumnObject, error)

	// GetRows gets the rows of the BlockObject
	GetRows() (uint32, error)

	// GetMeta gets the meta of the BlockObject
	GetMeta() BlockMeta

	// GetExtent gets the metadata location of BlockObject in fileservice
	GetExtent() Extent

	// GetID is to get the serial number of the block in the object
	GetID() uint32
}

// ColumnObject is a vector in a batch written to fileservice
type ColumnObject interface {
	// GetData gets the data of ColumnObject
	// Returns an IOVector, the caller needs to traverse the IOVector
	// to get all the structures required for data generation
	GetData(m *mpool.MPool) (*fileservice.IOVector, error)

	// GetIndex gets the index of ColumnObject
	GetIndex(dataType IndexDataType, m *mpool.MPool) (IndexData, error)

	// GetMeta gets the metadata of ColumnObject
	GetMeta() *ColumnMeta
}

var (
	endian = binary.LittleEndian
)

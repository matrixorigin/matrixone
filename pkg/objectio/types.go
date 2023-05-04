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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type WriteType int8

const (
	WriteTS WriteType = iota
)

const ZoneMapSize = index.ZMSize

type ZoneMap = index.ZM
type StaticFilter = index.StaticFilter

var NewZM = index.NewZM
var BuildZM = index.BuildZM

type WriteOptions struct {
	Type WriteType
	Val  any
}

type ReadBlockOptions struct {
	Id    uint16
	Idxes map[uint16]bool
}

// Writer is to virtualize batches into multiple blocks
// and write them into filefservice at one time
type Writer interface {
	// Write writes one batch to the Buffer at a time,
	// one batch corresponds to a virtual block,
	// and returns the handle of the block.
	Write(batch *batch.Batch) (BlockObject, error)

	// Write metadata for every column of all blocks
	WriteObjectMeta(ctx context.Context, totalRow uint32, metas []ColumnMeta)

	// WriteEnd is to write multiple batches written to
	// the buffer to the fileservice at one time
	WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error)
}

// Reader is to read data from fileservice
type Reader interface {
	// Read is to read columns data of a block from fileservice at one time
	// extent is location of the block meta
	// idxs is the column serial number of the data to be read
	Read(ctx context.Context,
		extent *Extent, idxs []uint16,
		id uint32,
		m *mpool.MPool,
		readFunc CacheConstructorFactory) (*fileservice.IOVector, error)

	ReadAll(
		ctx context.Context,
		extent *Extent,
		idxs []uint16,
		m *mpool.MPool,
		readFunc CacheConstructorFactory,
	) (*fileservice.IOVector, error)

	ReadBlocks(ctx context.Context,
		extent *Extent,
		ids map[uint32]*ReadBlockOptions,
		m *mpool.MPool,
		readFunc CacheConstructorFactory) (*fileservice.IOVector, error)

	// ReadMeta is the meta that reads a block
	// extent is location of the block meta
	ReadMeta(ctx context.Context, extent *Extent, m *mpool.MPool) (ObjectMeta, error)

	// ReadAllMeta is read the meta of all blocks in an object
	ReadAllMeta(ctx context.Context, m *mpool.MPool) (ObjectMeta, error)

	GetObject() *Object
}

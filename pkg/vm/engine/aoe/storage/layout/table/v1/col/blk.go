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

package col

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ro "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
	"sync/atomic"
)

type IColumnBlock interface {
	common.IRef
	GetID() uint64
	GetMeta() *metadata.Block
	GetRowCount() uint64
	RegisterPart(part IColumnPart)
	GetType() base.BlockType
	GetColType() types.Type
	GetIndexHolder() *index.BlockIndexHolder
	GetColIdx() int
	GetSegmentFile() base.ISegmentFile
	CloneWithUpgrade(iface.IBlock) IColumnBlock
	String() string
	Size() uint64
	GetVector() vector.IVector
	LoadVectorWrapper() (*vector.VectorWrapper, error)
	ForceLoad(*bytes.Buffer, *bytes.Buffer) (*ro.Vector, error)
	Prefetch() error
	GetVectorReader() dbi.IVectorReader
}

type columnBlock struct {
	sync.RWMutex
	common.RefHelper
	colIdx      int
	meta        *metadata.Block
	segFile     base.ISegmentFile
	indexHolder *index.BlockIndexHolder
	typ         base.BlockType
}

func (blk *columnBlock) GetSegmentFile() base.ISegmentFile {
	return blk.segFile
}

func (blk *columnBlock) GetIndexHolder() *index.BlockIndexHolder {
	return blk.indexHolder
}

func (blk *columnBlock) GetColIdx() int {
	return blk.colIdx
}

func (blk *columnBlock) GetColType() types.Type {
	return blk.meta.Segment.Table.Schema.ColDefs[blk.colIdx].Type
}

func (blk *columnBlock) GetMeta() *metadata.Block {
	return blk.meta
}

func (blk *columnBlock) GetType() base.BlockType {
	return blk.typ
}

func (blk *columnBlock) GetRowCount() uint64 {
	return atomic.LoadUint64(&blk.meta.Count)
}

func (blk *columnBlock) GetID() uint64 {
	return blk.meta.Id
}

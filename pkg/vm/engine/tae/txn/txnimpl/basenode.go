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

package txnimpl

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

const MaxNodeRows = 10000

type InsertNode interface {
	Close() error
	Append(data *containers.Batch, offset uint32) (appended uint32, err error)
	RangeDelete(start, end uint32) error
	IsRowDeleted(row uint32) bool
	IsPersisted() bool
	PrintDeletes() string
	GetColumnDataByIds([]int) (*model.BlockView, error)
	GetColumnDataById(int) (*model.ColumnView, error)
	Prefetch(idxes []uint16) error
	FillBlockView(view *model.BlockView, colIdxes []int) (err error)
	FillColumnView(*model.ColumnView) error
	Window(start, end uint32) (*containers.Batch, error)
	GetSpace() uint32
	Rows() uint32
	GetValue(col int, row uint32) (any, bool, error)
	MakeCommand(uint32) (txnif.TxnCmd, error)
	AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dbid uint64, dest *common.ID) *appendInfo
	RowsWithoutDeletes() uint32
	LengthWithDeletes(appended, toAppend uint32) uint32
	OffsetWithDeletes(count uint32) uint32
	GetAppends() []*appendInfo
	GetTxn() txnif.AsyncTxn
	GetPersistedLoc() (objectio.Location, objectio.Location)
}

type appendInfo struct {
	seq              uint32
	srcOff, srcLen   uint32
	dbid             uint64
	dest             common.ID
	destOff, destLen uint32
}

const (
	AppendInfoSize int64 = int64(unsafe.Sizeof(appendInfo{}))
)

func EncodeAppendInfo(info *appendInfo) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(info)), AppendInfoSize)
}

func (info *appendInfo) GetDest() *common.ID {
	return &info.dest
}
func (info *appendInfo) GetDBID() uint64 {
	return info.dbid
}
func (info *appendInfo) GetSrcOff() uint32 {
	return info.srcOff
}
func (info *appendInfo) GetSrcLen() uint32 {
	return info.srcLen
}
func (info *appendInfo) GetDestOff() uint32 {
	return info.destOff
}
func (info *appendInfo) GetDestLen() uint32 {
	return info.destLen
}
func (info *appendInfo) Desc() string {
	return info.dest.BlockString()
}
func (info *appendInfo) String() string {
	s := fmt.Sprintf("[From=[%d:%d];To=%s[%d:%d]]",
		info.srcOff, info.srcLen+info.srcOff, info.dest.BlockString(), info.destOff, info.destLen+info.destOff)
	return s
}
func (info *appendInfo) WriteTo(w io.Writer) (n int64, err error) {
	_, err = w.Write(EncodeAppendInfo(info))
	n = AppendInfoSize
	return
}
func (info *appendInfo) ReadFrom(r io.Reader) (n int64, err error) {
	_, err = r.Read(EncodeAppendInfo(info))
	n = AppendInfoSize
	return
}

type memoryNode struct {
	common.RefHelper
	bnode *baseNode
	//data resides in.
	data    *containers.Batch
	rows    uint32
	appends []*appendInfo
}

func newMemoryNode(node *baseNode) *memoryNode {
	return &memoryNode{
		bnode:   node,
		appends: make([]*appendInfo, 0),
	}
}

func (n *memoryNode) GetSpace() uint32 {
	return MaxNodeRows - n.rows
}

func (n *memoryNode) PrepareAppend(data *containers.Batch, offset uint32) uint32 {
	left := uint32(data.Length()) - offset
	nodeLeft := MaxNodeRows - n.rows
	if left <= nodeLeft {
		return left
	}
	return nodeLeft
}

func (n *memoryNode) Append(data *containers.Batch, offset uint32) (an uint32, err error) {
	schema := n.bnode.table.GetLocalSchema()
	if n.data == nil {
		opts := containers.Options{}
		opts.Capacity = data.Length() - int(offset)
		if opts.Capacity > int(MaxNodeRows) {
			opts.Capacity = int(MaxNodeRows)
		}
		n.data = containers.BuildBatch(schema.AllNames(), schema.AllTypes(), opts)
	}

	from := uint32(n.data.Length())
	an = n.PrepareAppend(data, offset)
	for _, attr := range data.Attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := n.data.Vecs[def.Idx]
		// logutil.Infof("destVec: %s, %d, %d", destVec.String(), cnt, data.Length())
		destVec.ExtendWithOffset(data.Vecs[def.Idx], int(offset), int(an))
	}
	n.rows = uint32(n.data.Length())
	err = n.FillPhyAddrColumn(from, an)
	return
}

func (n *memoryNode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	col, err := model.PreparePhyAddrData(catalog.PhyAddrColumnType, n.bnode.meta.MakeKey(), startRow, length)
	if err != nil {
		return
	}
	defer col.Close()
	vec := n.data.Vecs[n.bnode.table.GetLocalSchema().PhyAddrKey.Idx]
	vec.Extend(col)
	return
}

type persistedNode struct {
	common.RefHelper
	bnode   *baseNode
	rows    uint32
	deletes *roaring.Bitmap
	//ZM and BF index for primary key
	pkIndex indexwrapper.Index
	//ZM and BF index for all columns
	indexes map[int]indexwrapper.Index
}

func newPersistedNode(bnode *baseNode) *persistedNode {
	node := &persistedNode{
		bnode: bnode,
	}
	node.OnZeroCB = node.close
	if bnode.meta.HasPersistedData() {
		node.init()
	}
	return node
}

func (n *persistedNode) close() {
	for i, index := range n.indexes {
		index.Close()
		n.indexes[i] = nil
	}
	n.indexes = nil
}

func (n *persistedNode) init() {
	n.indexes = make(map[int]indexwrapper.Index)
	schema := n.bnode.meta.GetSchema()
	pkIdx := -1
	if schema.HasPK() {
		pkIdx = schema.GetSingleSortKeyIdx()
	}
	for i := range schema.ColDefs {
		index := indexwrapper.NewImmutableIndex()
		if err := index.ReadFrom(
			n.bnode.indexCache,
			n.bnode.fs,
			n.bnode.meta.GetMetaLoc(),
			schema.ColDefs[i]); err != nil {
			panic(err)
		}
		n.indexes[i] = index
		if i == pkIdx {
			n.pkIndex = index
		}
	}
	location := n.bnode.meta.GetMetaLoc()
	n.rows = uint32(tables.ReadPersistedBlockRow(location))

}

func (n *persistedNode) Rows() uint32 {
	return n.rows
}

type baseNode struct {
	indexCache model.LRUCache
	fs         *objectio.ObjectFS
	//scheduler is used to flush insertNode into S3/FS.
	scheduler tasks.TaskScheduler
	//meta for this uncommitted standalone block.
	meta    *catalog.BlockEntry
	table   *txnTable
	storage struct {
		mnode *memoryNode
		pnode *persistedNode
	}
}

func newBaseNode(
	tbl *txnTable,
	fs *objectio.ObjectFS,
	indexCache model.LRUCache,
	sched tasks.TaskScheduler,
	meta *catalog.BlockEntry,
) *baseNode {
	return &baseNode{
		indexCache: indexCache,
		fs:         fs,
		scheduler:  sched,
		meta:       meta,
		table:      tbl,
	}
}

func (n *baseNode) IsPersisted() bool {
	return n.meta.HasPersistedData()
}

func (n *baseNode) GetTxn() txnif.AsyncTxn {
	return n.table.store.txn
}

func (n *baseNode) GetPersistedLoc() (objectio.Location, objectio.Location) {
	return n.meta.GetMetaLoc(), n.meta.GetDeltaLoc()
}

func (n *baseNode) Rows() uint32 {
	if n.storage.mnode != nil {
		return n.storage.mnode.rows
	} else if n.storage.pnode != nil {
		return n.storage.pnode.Rows()
	}
	panic(moerr.NewInternalErrorNoCtx(
		fmt.Sprintf("bad insertNode %s", n.meta.String())))
}

func (n *baseNode) TryUpgrade() (err error) {
	//TODO::update metaloc and deltaloc
	if n.storage.mnode != nil {
		n.storage.mnode = nil
	}
	if n.storage.pnode == nil {
		n.storage.pnode = newPersistedNode(n)
		n.storage.pnode.Ref()
	}
	return
}

func (n *baseNode) LoadPersistedColumnData(colIdx int) (vec containers.Vector, err error) {
	def := n.table.GetLocalSchema().ColDefs[colIdx]
	location := n.meta.GetMetaLoc()
	return tables.LoadPersistedColumnData(
		n.fs,
		nil,
		def,
		location)
}

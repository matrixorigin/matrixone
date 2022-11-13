package tables

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
)

type persistedNode struct {
	common.RefHelper
	block   *baseBlock
	pkIndex indexwrapper.Index
	indexes map[int]indexwrapper.Index
}

func newPersistedNode(block *baseBlock) *persistedNode {
	node := &persistedNode{
		block: block,
	}
	node.initIndexes(block.meta.GetSchema())
	return node
}

func (node *persistedNode) close() {
	for i, index := range node.indexes {
		index.Close()
		node.indexes[i] = nil
	}
	node.indexes = nil
	return
}

func (node *persistedNode) initIndexes(schema *catalog.Schema) {
	pkIdx := -1
	if schema.HasPK() {
		pkIdx = schema.GetSingleSortKeyIdx()
	}
	for i := range schema.ColDefs {
		index := indexwrapper.NewImmutableIndex()
		if err := index.ReadFrom(
			node.block.bufMgr,
			node.block.fs,
			node.block.meta.AsCommonID(),
			node.block.meta.GetMetaLoc(),
			schema.ColDefs[i]); err != nil {
			panic(err)
		}
		node.indexes[i] = index
		if i == pkIdx {
			node.pkIndex = index
		}
	}
}

func (node *persistedNode) Pin() *common.PinnedItem[*persistedNode] {
	node.Ref()
	return &common.PinnedItem[*persistedNode]{
		Val: node,
	}
}

func (node *persistedNode) Rows() uint32 {
	location := node.block.GetMeta().(catalog.BlockEntry).GetMetaLoc()
	return uint32(ReadPersistedBlockRow(location))
}

func (node *persistedNode) BatchDedup(
	keys containers.Vector,
	skipFn func(row uint32) error) (sels *roaring.Bitmap, err error) {
	return node.pkIndex.BatchDedup(keys, skipFn)
}

func (node *persistedNode) Dedup(key any) (err error) {
	return node.pkIndex.Dedup(key)
}

func (node *persistedNode) ContainsKey(key any) (ok bool, err error) {
	if err = node.pkIndex.Dedup(key); err == nil {
		return
	}
	if !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
		return
	}
	ok = true
	err = nil
	return
}

func (node *persistedNode) GetColumnDataWindow(
	from uint32,
	to uint32,
	colIdx int,
	buffer *bytes.Buffer,
) (vec containers.Vector, err error) {
	var data containers.Vector
	if data, err = node.block.LoadPersistedColumnData(
		colIdx,
		buffer); err != nil {
		return
	}
	if to-from == uint32(data.Length()) {
		vec = data
	} else {
		vec = data.CloneWindow(int(from), int(to-from), common.DefaultAllocator)
		data.Close()
	}
	return
}

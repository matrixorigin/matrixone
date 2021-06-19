package index

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"

	"github.com/pilosa/pilosa/roaring"
)

type IndexInfo struct {
	*bmgr.Node
	Cols *roaring.Bitmap
}

func (info *IndexInfo) ContainsCol(v uint64) bool {
	return info.Cols.Contains(v)
}

func (info *IndexInfo) ContainsOnlyCol(v uint64) bool {
	return info.Cols.Contains(v) && info.Cols.Count() == 1
}

func (info *IndexInfo) AllCols() []uint64 {
	return info.Cols.Slice()
}

// TODO: Just for index framework implementation placeholder
type Index interface {
	buf.IMemoryNode
	Type() base.IndexType
	Eq(interface{}) bool
	Ne(interface{}) bool
	Lt(interface{}) bool
	Le(interface{}) bool
	Gt(interface{}) bool
	Ge(interface{}) bool
	Btw(interface{}) bool
}

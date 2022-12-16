package gc

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"sync/atomic"
)

type TableEntry struct {
	tid    uint64
	blocks []common.ID
	delete []common.ID
	drop   bool
}

type ObjectEntry struct {
	refs  atomic.Int64
	table TableEntry
	drop  bool
}

func NewObjectEntry() *ObjectEntry {
	return &ObjectEntry{
		table: TableEntry{
			blocks: make([]common.ID, 0),
			delete: make([]common.ID, 0),
			drop:   false,
		},
	}
}

func (o *ObjectEntry) AddBlock(block common.ID) {
	o.table.tid = block.TableID
	o.table.blocks = append(o.table.blocks, block)
}

func (o *ObjectEntry) DelBlock(block common.ID) {
	o.table.tid = block.TableID
	o.table.delete = append(o.table.delete, block)
}

func (o *ObjectEntry) DropTable() {
	o.table.drop = true
}

func (o *ObjectEntry) Refs(n int) {
	o.refs.Add(int64(n))
}

func (o *ObjectEntry) UnRefs(n int) {
	o.refs.Add(int64(0 - n))
}

func (o *ObjectEntry) MergeEntry(entry ObjectEntry) {
	if o.table.drop || entry.table.drop {
		o.table.delete = nil
		o.table.blocks = nil
		return
	}
	refs := len(entry.table.blocks)
	unRefs := len(entry.table.delete)
	if refs > 0 {
		o.table.blocks = append(o.table.blocks, entry.table.blocks...)
		o.Refs(refs)
	}

	if unRefs > 0 {
		o.table.delete = append(o.table.delete, entry.table.delete...)
		o.UnRefs(unRefs)
	}
}

func (o *ObjectEntry) AllowGC() bool {
	if o.table.drop || o.refs.Load() < 1 {
		o.table.delete = nil
		o.table.blocks = nil
		return true
	}
	return false
}

package gc

import (
	"bytes"
	"fmt"
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
	o.Refs(1)
}

func (o *ObjectEntry) DelBlock(block common.ID) {
	o.table.tid = block.TableID
	o.table.delete = append(o.table.delete, block)
	o.UnRefs(1)
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

func (o *ObjectEntry) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString("entry:[")
	_, _ = w.WriteString(fmt.Sprintf("tid: %d, isdrop: %t refs: %d ", o.table.tid, o.table.drop, o.refs.Load()))
	_, _ = w.WriteString("block:[")
	for _, block := range o.table.blocks {
		_, _ = w.WriteString(fmt.Sprintf(" %v", block.String()))
	}
	_, _ = w.WriteString("]")
	_, _ = w.WriteString("delete:[")
	for _, id := range o.table.delete {
		_, _ = w.WriteString(fmt.Sprintf(" %v", id.String()))
	}
	_, _ = w.WriteString("]")
	_, _ = w.WriteString("]\n")
	return w.String()
}

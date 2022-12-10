package gc

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"sync"
)

type GcEntry struct {
	sync.Mutex
	table  map[string][]common.ID
	delete map[string][]common.ID
	fs     *objectio.ObjectFS
}

func NewGcTable(fs *objectio.ObjectFS) *GcEntry {
	table := &GcEntry{
		table:  make(map[string][]common.ID),
		delete: make(map[string][]common.ID),
		fs:     fs,
	}
	return table
}

func (t *GcEntry) AddBlock(id common.ID, name string) {
	t.Lock()
	defer t.Unlock()
	blockList := t.table[name]
	if blockList != nil {
		blockList = make([]common.ID, 0)
	}

	t.table[name] = blockList
}

func (t *GcEntry) DeleteBlock(id common.ID, name string) {
	t.Lock()
	defer t.Unlock()
	blockList := t.delete[name]
	if blockList != nil {
		blockList = make([]common.ID, 0)
	}

	blockList = append(blockList, id)
	t.table[name] = blockList
}

func (t *GcEntry) String() string {
	t.Lock()
	defer t.Unlock()
	if len(t.table) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("table:[")
	for name, ids := range t.table {
		_, _ = w.WriteString(fmt.Sprintf(" %v", name))
		_, _ = w.WriteString("block:[")
		for _, id := range ids {
			_, _ = w.WriteString(fmt.Sprintf(" %v", id.String()))
		}
		_, _ = w.WriteString("]\n")
	}
	_, _ = w.WriteString("]\n")
	if len(t.delete) != 0 {
		_, _ = w.WriteString("delete:[")
		for name, ids := range t.delete {
			_, _ = w.WriteString(fmt.Sprintf(" %v", name))
			_, _ = w.WriteString("block:[")
			for _, id := range ids {
				_, _ = w.WriteString(fmt.Sprintf(" %v", id.String()))
			}
			_, _ = w.WriteString("]\n")
		}
		_, _ = w.WriteString("]\n")
	}
	return w.String()
}

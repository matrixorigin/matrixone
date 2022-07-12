package entry

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type Entry struct {
	Entry entry.Entry
	Info  *entry.Info
	Lsn   uint64
	wg    sync.WaitGroup
}

func NewEntry(e entry.Entry) *Entry {
	return &Entry{
		Entry: e,
	}
}

func (e *Entry) WaitDone() {
	e.wg.Wait()
}

func (e *Entry) GetSize() int {
	return e.Entry.TotalSize()
}

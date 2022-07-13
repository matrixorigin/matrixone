package entry

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type Entry struct {
	Entry entry.Entry
	Info  *entry.Info
	Lsn   uint64
	err   error
	wg    sync.WaitGroup
}

func NewEntry(e entry.Entry) *Entry {
	en := &Entry{
		Entry: e,
		wg:    sync.WaitGroup{},
	}
	en.wg.Add(1)
	return en
}

func (e *Entry) WaitDone() error {
	e.wg.Wait()
	return e.err
}

func (e *Entry) DoneWithErr(err error) {
	e.err = err
	e.wg.Done()
}

func (e *Entry) GetSize() int {
	return e.Entry.TotalSize()
}

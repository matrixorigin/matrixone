package logstore

import "sync"

type AsyncEntry interface {
	Entry
	GetError() error
	WaitDone() error
	DoneWithErr(error)
}

var (
	_asyncEntPool = sync.Pool{New: func() interface{} {
		e := &AsyncBaseEntry{
			BaseEntry: *newBaseEntry(),
		}
		return e
	}}
)

func NewAsyncBaseEntry() *AsyncBaseEntry {
	e := _asyncEntPool.Get().(*AsyncBaseEntry)
	e.wg.Add(1)
	return e
}

type AsyncBaseEntry struct {
	BaseEntry
	wg  sync.WaitGroup
	err error
}

func (e *AsyncBaseEntry) reset() {
	e.wg = sync.WaitGroup{}
	e.BaseEntry.reset()
}

func (e *AsyncBaseEntry) DoneWithErr(err error) {
	e.err = err
	e.wg.Done()
}

func (e *AsyncBaseEntry) WaitDone() error {
	e.wg.Wait()
	return e.err
}

func (e *AsyncBaseEntry) GetError() error {
	return e.err
}

func (e *AsyncBaseEntry) IsAsync() bool {
	return true
}

func (e *AsyncBaseEntry) Free() {
	if e == nil {
		return
	}
	e.reset()
	_asyncEntPool.Put(e)
}

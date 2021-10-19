package wal

import "sync"

var (
	_entryPool = sync.Pool{New: func() interface{} {
		return &Entry{}
	}}
)

type Entry struct {
	sync.WaitGroup
	Id      uint64
	Payload Payload
}

func GetEntry(id uint64) *Entry {
	e := _entryPool.Get().(*Entry)
	e.Id = id
	e.Add(1)
	return e
}

func (e *Entry) reset() {
	e.Payload = nil
	e.Id = 0
	e.WaitGroup = sync.WaitGroup{}
}

func (e *Entry) Free() {
	e.reset()
	_entryPool.Put(e)
}

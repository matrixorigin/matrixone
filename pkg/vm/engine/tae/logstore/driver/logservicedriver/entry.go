package logservicedriver

import (
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

type meta struct {
	appended uint64
	addr     map[uint64]uint64
}

type recordEntry struct {
	meta    *meta
	entries []*entry.Entry
	size    int

	payloads []byte

	//for read
	record    *logservice.LogRecord
	marshaled bool
}

func (r *recordEntry) append(e *entry.Entry) {
	r.entries = append(r.entries, e)
	r.size += e.GetSize()
}

func (r *recordEntry) makeRecord() logservice.LogRecord {
	return logservice.LogRecord{} //TODO
}

func (r *recordEntry) readEntry(lsn uint64) *entry.Entry {
	return nil //TODO
}

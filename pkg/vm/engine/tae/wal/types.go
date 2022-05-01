package wal

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

const (
	GroupC uint32 = iota + 10
	GroupUC
)

type Index struct {
	LSN uint64
	CSN uint32
}

type LogEntry entry.Entry

type Driver interface {
	Checkpoint(indexes []*Index) error
	AppendEntry(uint32, LogEntry) (uint64, error)
	LoadEntry(groupId uint32, lsn uint64) (LogEntry, error)
	Close() error
}

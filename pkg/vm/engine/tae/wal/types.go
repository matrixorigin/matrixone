package wal

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

const (
	GroupC uint32 = iota + 10
	GroupUC
	GroupCatalog
)

type Index struct {
	LSN  uint64
	CSN  uint32
	Size uint32
}

type LogEntry entry.Entry

type Driver interface {
	GetCheckpointed() uint64
	Checkpoint(indexes []*Index) (LogEntry, error)
	AppendEntry(uint32, LogEntry) (uint64, error)
	LoadEntry(groupId uint32, lsn uint64) (LogEntry, error)
	Compact() error
	Close() error
}

func (index *Index) Clone() *Index {
	if index == nil {
		return nil
	}
	return &Index{
		LSN:  index.LSN,
		CSN:  index.CSN,
		Size: index.Size,
	}
}
func (index *Index) String() string {
	if index == nil {
		return "<nil index>"
	}
	return fmt.Sprintf("<Index[%d:%d/%d]>", index.LSN, index.CSN, index.Size)
}

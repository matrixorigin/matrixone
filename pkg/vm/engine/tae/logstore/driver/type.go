package driver

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"

type Driver interface {
	Append(*entry.Entry) error
	Truncate(lsn uint64) error
	GetTruncated() (lsn uint64, err error)
	Read(lsn uint64) (*entry.Entry, error)
	Close() error
	// Replay(h ApplyHandle) error
}

type ApplyHandle = func(*entry.Entry)

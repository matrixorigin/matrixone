package driver

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"

type Driver interface {
	Append(*entry.Entry)
	Truncate(lsn uint64) error
	GetTruncated() (lsn uint64)
	Read(lsn uint64) *entry.Entry
	Close() error
}

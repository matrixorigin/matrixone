package driver

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

type Driver interface {
	Append(entry.Entry) (lsn uint64)
	Truncate(lsn uint64)
	GetTruncated() (lsn uint64)
	Read(lsn uint64) entry.Entry
	Close() error
}

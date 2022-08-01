package store

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

const (
	GroupC = entry.GTCustomizedStart + iota
	GroupCKP
	GroupInternal
	GroupUC
)

type Store interface {
	Append(gid uint32, entry entry.Entry) (lsn uint64, err error)
	FuzzyCheckpoint(gid uint32, idxes []*Index) (ckpEntry entry.Entry, err error)
	RangeCheckpoint(gid uint32, start, end uint64) (ckpEntry entry.Entry, err error)
	Load(gid uint32, lsn uint64) (entry.Entry, error)

	GetCurrSeqNum(gid uint32) (lsn uint64)
	GetSynced(gid uint32) (lsn uint64)
	GetPendding(gid uint32) (cnt uint64)
	GetCheckpointed(gid uint32) (lsn uint64)

	Replay(h ApplyHandle) error
	Close() error
}

type ApplyHandle = func(group uint32, commitId uint64, payload []byte, typ uint16, info any)

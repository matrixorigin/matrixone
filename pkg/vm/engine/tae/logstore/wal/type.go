package wal

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	GroupC = entry.GTCustomizedStart + iota
	GroupCKP
	GroupInternal
	GroupUC
)

type Wal interface {
	Append(gid uint32, entry entry.Entry) (lsn uint64, err error)
	Checkpoint(idxes []*wal.Index) (ckpEntry entry.Entry)
	Load(gid uint32, lsn uint64) entry.Entry

	GetCurrSeqNum(gid uint32) (lsn uint64)
	GetSynced(gid uint32) (lsn uint64)
	GetPendding(gid uint32) (cnt uint64)
	GetCheckpointed(gid uint32) (lsn uint64)
}

//wal commit uncommit
//store fuzzy ckp

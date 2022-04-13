package store

import (
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type StoreCfg struct {
	RotateChecker  RotateChecker
	HistoryFactory HistoryFactory
}

type RotateChecker interface {
	PrepareAppend(VFile, int) (bool, error)
}

type VFile interface {
	sync.Locker
	RLock()
	RUnlock()
	SizeLocked() int
	Destroy() error
	Id() int
	Name() string
	String() string
	InCheckpoint(map[uint32]*common.ClosedIntervals) bool
	InCommits(map[uint32]*common.ClosedIntervals) bool
	InTxnCommits(map[uint32]map[uint64]uint64, map[uint32]*common.ClosedIntervals) bool
	MergeCheckpoint(map[uint32]*common.ClosedIntervals)
	MergeTidCidMap(map[uint32]map[uint64]uint64)
	Replay(*replayer, ReplayObserver) error
	Load(groupId uint32, lsn uint64) (entry.Entry, error)
	LoadMeta() error
	FreeMeta()
	OnReplay(r *replayer)
}

type FileAppender interface {
	Prepare(int, interface{}) error
	Write([]byte) (int, error)
	Commit() error
	Rollback()
	Sync() error
	Revert()
}

type FileReader interface {
	// io.Reader
	// ReadAt([]byte, FileAppender) (int, error)
}

type ReplayObserver interface {
	OnNewEntry(int)
	OnNewCommit(*entry.Info)
	OnNewCheckpoint(*entry.Info)
	OnNewTxn(*entry.Info)
	OnNewUncommit(addrs []*VFileAddress)
}

type ReplayHandle = func(VFile, ReplayObserver) error

type History interface {
	String() string
	Append(VFile)
	Extend(...VFile)
	Entries() int
	EntryIds() []int
	GetEntry(int) VFile
	DropEntry(int) (VFile, error)
	OldestEntry() VFile
	Empty() bool
	Replay(*replayer, ReplayObserver) error
	TryTruncate() error
}

type ApplyHandle = func(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error)

type File interface {
	io.Closer
	sync.Locker
	RLock()
	RUnlock()
	FileReader

	Sync() error
	GetAppender() FileAppender
	Replay(*replayer, ReplayObserver) error
	GetHistory() History
	TryTruncate(int64) error
	Load(ver int, groupId uint32, lsn uint64) (entry.Entry, error)
}

type Store interface {
	io.Closer
	Sync() error
	Replay(ApplyHandle) error
	GetCheckpointed(uint32) uint64
	GetSynced(uint32) uint64
	AppendEntry(groupId uint32, e entry.Entry) (uint64, error)
	TryCompact() error
	TryTruncate(int64) error
	Load(groupId uint32, lsn uint64) (entry.Entry, error)
}

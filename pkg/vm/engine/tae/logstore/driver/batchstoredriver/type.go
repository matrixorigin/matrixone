package batchstoredriver

import (
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

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

	// Replay(*replayer, ReplayObserver) error
	OnReplayCommitted()

	Load(lsn uint64) (*entry.Entry, error)
	LoadByOffset(offset int) (*entry.Entry, error)
}

type FileAppender interface {
	Prepare(int,uint64) (any,error)
	Write([]byte) (int, error)
	Commit() error
}

type FileReader any

// io.Reader
// ReadAt([]byte, FileAppender) (int, error)

// type ReplayObserver interface {
// 	OnNewEntry(int)
// 	OnLogInfo(*entry.Info)
// }

// type ReplayHandle = func(VFile, ReplayObserver) error

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
	// Replay(*replayer, ReplayObserver) error
	// TryTruncate(*compactor) error
}

type ApplyHandle = func(group uint32, commitId uint64, payload []byte, typ uint16, info any)

type File interface {
	io.Closer
	sync.Locker
	RLock()
	RUnlock()
	FileReader

	GetEntryByVersion(version int) (VFile, error)
	Sync() error
	GetAppender() FileAppender
	// Replay(*replayer, ReplayObserver) error
	GetHistory() History
	Load(ver int, groupId uint32, lsn uint64) (*entry.Entry, error)
}

type Store interface {
	io.Closer
	Append(*entry.Entry) error
	Truncate(lsn uint64) error
	GetTruncated() (lsn uint64, err error)
	Read(lsn uint64) (*entry.Entry, error)
	Close() error
	// Replay(ApplyHandle) error
	GetSynced(uint32) uint64
	GetCurrSeqNum(uint32) uint64
}

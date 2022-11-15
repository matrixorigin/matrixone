// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batchstoredriver

import (
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
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
	Close() error
	Id() int
	Name() string
	String() string

	Replay(*replayer) error
	OnReplayCommitted()

	Load(lsn uint64) (*entry.Entry, error)
	LoadByOffset(offset int) (*entry.Entry, error)
}

type FileAppender interface {
	Prepare(int, uint64) (any, error)
	Write([]byte) (int, error)
	Commit() error
}

type FileReader any

type ReplayObserver interface {
	onTruncatedFile(id int)
}

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
	Close()
}

type File interface {
	io.Closer
	sync.Locker
	RLock()
	RUnlock()
	FileReader

	GetEntryByVersion(version int) (VFile, error)
	Sync() error
	GetAppender() FileAppender
	Replay(*replayer) error
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
	Replay(driver.ApplyHandle) error
	GetSynced(uint32) uint64
	GetCurrSeqNum(uint32) uint64
}

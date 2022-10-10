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

package catalog

import (
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

// JXM TODO:
// Generic BaseEntry can't work in go 1.19 because of compiler bug:
// https://github.com/golang/go/issues/54671
// Refactor catalog and use generic BaseEntry after go 1.19.1 release.
type BaseEntry interface {
	RLock()
	RUnlock()

	String() string
	StringLocked() string
	PPString(common.PPLevel, int, string) string

	GetTxn() txnif.TxnReader
	GetID() uint64
	GetIndexes() []*wal.Index
	GetLogIndex() *wal.Index

	GetLatestNodeLocked() txnif.MVCCNode
	IsVisible(ts types.TS, mu *sync.RWMutex) (ok bool, err error)

	HasCommittedNodeInRange(minTs, MaxTs types.TS) bool
	IsCreating() bool
	IsCommitting() bool
	DeleteBefore(ts types.TS) bool

	WriteOneNodeTo(w io.Writer) (n int64, err error)
	ReadOneNodeFrom(r io.Reader) (n int64, err error)
	CloneCommittedInRange(start, end types.TS) (ret BaseEntry)

	PrepareCommit() error
	PrepareRollback() (bool, error)
	ApplyCommit(index *wal.Index) error
	ApplyRollback(index *wal.Index) error
}

func CompareUint64(left, right uint64) int {
	if left > right {
		return 1
	} else if left < right {
		return -1
	}
	return 0
}

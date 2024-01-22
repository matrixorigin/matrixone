// Copyright 2024 Matrix Origin
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

package trace

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type Service interface {
}

type TxnTrace struct {
	sync.RWMutex

	TxnID       []byte
	SnapshotTS  timestamp.Timestamp
	CommitTS    timestamp.Timestamp
	FinalStatus txn.TxnStatus

	CreateBy struct {
		AccountID   int32
		SessionID   string
		StatementID string
		CN          string
	}

	Points struct {
		CreateAt     time.Time
		DetermineAt  time.Time
		WaitActiveAt time.Time
		ActiveAt     time.Time
	}

	Lock struct {
		WaitFor [][]byte
	}
}

// Storage used to store the txn trace.
type Storage interface {
	NewWriteBatch() WriteBatch
	Set(key, value []byte, sync bool) error
	Write(wb WriteBatch, sync bool) error
	Delete(key []byte, sync bool) error
	// RangeDelete remove data in [start,end)
	RangeDelete(start, end []byte, sync bool) error
	Scan(start, end []byte, handleFunc func(key, value []byte) (bool, error)) error
	PrefixScan(prefix []byte, handleFunc func(key, value []byte) (bool, error)) error
	// Seek returns max(-inf, upperBound)
	Seek(lowerBound []byte) ([]byte, []byte, error)
	// SeekAndLT returns min[lowerBound, upperBound)
	SeekAndLT(lowerBound, upperBound []byte) ([]byte, []byte, error)
	// SeekLT returns max(-inf, upperBound)
	SeekLT(upperBound []byte) ([]byte, []byte, error)
	// SeekLTAndGE returns max[lowerBound, upperBound)
	SeekLTAndGE(upperBound, lowerBound []byte) ([]byte, []byte, error)
	Sync() error
	Close() error
}

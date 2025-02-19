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

package wal

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

const (
	GroupCKP      = entry.GTCKp
	GroupInternal = entry.GTInternal
	GroupFiles    = entry.GTFiles
)

const (
	GroupUserTxn = entry.GTCustomized + iota
	GroupC
)

type ReplayObserver interface {
	OnTimeStamp(ts types.TS)
}

type LogEntry = entry.Entry

type Store interface {
	AppendEntry(gid uint32, entry entry.Entry) (lsn uint64, err error)

	// it always checkpoint the GroupUserTxn group
	RangeCheckpoint(start, end uint64, files ...string) (ckpEntry entry.Entry, err error)

	// only used in the test
	// it returns the next lsn of group `GroupUserTxn`
	GetLSNWatermark() (lsn uint64)
	// only used in the test
	// it returns the remaining entries of group `GroupUserTxn` to be checkpointed
	// GetLSNWatermark() - GetCheckpointed()
	GetPenddingCnt() (cnt uint64)
	// only used in the test
	// it returns the last lsn of group `GroupUserTxn` that has been checkpointed
	GetCheckpointed() (lsn uint64)
	// only used in the test
	// it returns the truncated dsn
	GetTruncated() uint64

	Replay(
		ctx context.Context,
		h ApplyHandle,
		modeGetter func() driver.ReplayMode,
		opt *driver.ReplayOption,
	) error

	Close() error
}

type ApplyHandle = func(group uint32, commitId uint64, payload []byte, typ uint16, info any) driver.ReplayEntryState

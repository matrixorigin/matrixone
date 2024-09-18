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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
)

const (
	GroupPrepare = entry.GTCustomized + iota
	GroupC
)

type ReplayObserver interface {
	OnTimeStamp(ts types.TS)
}

type LogEntry entry.Entry

type Driver interface {
	GetCheckpointed() uint64
	RangeCheckpoint(start, end uint64, files ...string) (e LogEntry, err error)
	AppendEntry(uint32, LogEntry) (uint64, error)
	LoadEntry(groupID uint32, lsn uint64) (LogEntry, error)
	GetCurrSeqNum() uint64
	GetPenddingCnt() uint64
	Replay(handle store.ApplyHandle) error
	Start()
	Close() error

	GetTruncated() uint64
}

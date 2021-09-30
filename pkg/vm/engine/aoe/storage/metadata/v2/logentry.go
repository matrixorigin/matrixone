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

package metadata

import (
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	"sync"
)

type LogEntryType = logstore.EntryType
type LogEntry = logstore.Entry
type LogEntryMeta = logstore.EntryMeta

const (
	ETCreateTable LogEntryType = iota + logstore.ETCustomizeStart
	ETSoftDeleteTable
	ETHardDeleteTable
	ETCreateSegment
	ETUpgradeSegment
	ETDropSegment
	ETCreateBlock
	ETUpgradeBlock
	ETDropBlock
)

type IEntry interface {
	sync.Locker
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	CommitLocked(uint64)
	ToLogEntry(LogEntryType) LogEntry
}

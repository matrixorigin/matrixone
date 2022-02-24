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
	// "encoding/binary"
	"sync"

	"github.com/jiangxinmeng1/logstore/pkg/entry"
)

type LogEntryType = uint16
type LogEntry = entry.Entry

const (
	ETCreateDatabase LogEntryType = iota + entry.ETCustomizedStart
	ETSoftDeleteDatabase
	ETHardDeleteDatabase
	ETReplaceDatabase
	ETSplitDatabase
	ETCreateTable
	ETSoftDeleteTable
	ETHardDeleteTable
	ETAddIndice
	ETDropIndice
	ETCreateSegment
	ETUpgradeSegment
	ETDropSegment
	ETCreateBlock
	ETUpgradeBlock
	ETDropBlock
	ETDatabaseSnapshot
	ETDatabaseReplaced
	ETTransaction
)

type IEntry interface {
	sync.Locker
	Unmarshal([]byte) error
	Marshal() ([]byte, error)
	CommitLocked(uint64)
	ToLogEntry(LogEntryType) LogEntry
}


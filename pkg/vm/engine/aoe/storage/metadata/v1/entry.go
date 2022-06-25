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

package metadata

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
)

type databaseLogEntry struct {
	*BaseEntry
	Id       uint64    `json:"oid"`
	Database *Database `json:"db"`
	LogRange *LogRange `json:"logrange"`
}

type dbReplaceLogEntry struct {
	LoopProcessor `json:"-"`
	commitId      uint64            `json:"-"`
	Index         uint64            `json:"index"`
	Replaced      *databaseLogEntry `json:"replaced"`
	Replacer      []*Database       `json:"replacer"`
}

type segmentCheckpoint struct {
	Blocks     []*blockLogEntry
	NeedReplay bool
	LogEntry   segmentLogEntry
}

type tableCheckpoint struct {
	Segments   []*segmentCheckpoint
	NeedReplay bool
	LogEntry   tableLogEntry
}

type databaseCheckpoint struct {
	Tables     map[uint64]*tableCheckpoint
	NeedReplay bool
	LogEntry   databaseLogEntry
}

type catalogLogEntry struct {
	Databases map[uint64]*databaseCheckpoint
	SafeIds   map[uint64]uint64
	Range     *common.Range
}

func newDatabaseLogEntry(shardId, index uint64) *databaseLogEntry {
	logRange := new(LogRange)
	logRange.ShardId = shardId
	logRange.Range.Right = index
	e := &databaseLogEntry{
		Database: &Database{
			TableSet: make(map[uint64]*Table),
		},
		LogRange: logRange,
	}
	return e
}

func (e *databaseLogEntry) Marshal() ([]byte, error) {
	buf, err := json.Marshal(e)
	return buf, err
}

func (e *databaseLogEntry) Unmarshal(buf []byte) error {
	err := json.Unmarshal(buf, e)
	if err != nil {
		return err
	}
	if e.Database != nil {
		e.Database.coarseStats = new(coarseStats)
	}
	return nil
}

func (e *databaseLogEntry) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case ETDatabaseSnapshot:
		break
	default:
		panic("not supported")
	}
	buf, _ := e.Marshal()
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	err := logEntry.Unmarshal(buf)
	if err != nil {
		panic(err)
	}
	return logEntry
}

func newDbReplaceLogEntry() *dbReplaceLogEntry {
	e := &dbReplaceLogEntry{
		Replacer: make([]*Database, 0),
	}
	e.TableFn = e.onTable
	e.SegmentFn = e.onSegment
	e.BlockFn = e.onBlock
	return e
}

func (e *dbReplaceLogEntry) AddReplacer(db *Database) {
	e.Replacer = append(e.Replacer, db)
}

func (e *dbReplaceLogEntry) onTable(table *Table) error {
	table.Lock()
	defer table.Unlock()
	table.CommitLocked(e.commitId)
	return nil
}

func (e *dbReplaceLogEntry) onSegment(segment *Segment) error {
	segment.Lock()
	defer segment.Unlock()
	segment.CommitLocked(e.commitId)
	return nil
}

func (e *dbReplaceLogEntry) onBlock(block *Block) error {
	block.Lock()
	defer block.Unlock()
	block.CommitLocked(e.commitId)
	return nil
}

func (e *dbReplaceLogEntry) Lock()   {}
func (e *dbReplaceLogEntry) Unlock() {}

func (e *dbReplaceLogEntry) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *dbReplaceLogEntry) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, e)
}

func (e *dbReplaceLogEntry) ToLogEntry(eType LogEntryType) LogEntry {
	switch eType {
	case ETReplaceDatabase:
	case ETSplitDatabase:
		break
	default:
		panic("not supported")
	}
	buf, err := e.Marshal()
	if err != nil {
		panic(err)
	}
	logEntry := logstore.NewAsyncBaseEntry()
	logEntry.Meta.SetType(eType)
	err = logEntry.Unmarshal(buf)
	if err != nil {
		panic(err)
	}
	return logEntry
}

func (e *dbReplaceLogEntry) CommitLocked(commitId uint64) {
	if e.Replaced != nil {
		e.Replaced.Lock()
		e.Replaced.CommitLocked(commitId)
		e.Replaced.Unlock()
	}
	e.commitId = commitId
	for _, db := range e.Replacer {
		db.RLock()
		err := db.RecurLoopLocked(e)
		if err != nil {
			panic(err)
		}
		db.RUnlock()
		db.Lock()
		db.CommitLocked(commitId)
		db.Unlock()
	}
}

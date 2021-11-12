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

type DatabaseListener interface {
	OnDatabaseCreated(created *Database)
	OnDatabaseSoftDeleted(deleted *Database)
	OnDatabaseHardDeleted(deleted *Database)
	OnDatabaseSplitted(stale *Database, created []*Database)
	OnDatabaseReplaced(stale *Database, created *Database)
	OnDatabaseCompacted(stale *Database)
}

type TableListener interface {
	OnTableCreated(created *Table)
	OnTableSoftDeleted(deleted *Table)
	OnTableHardDeleted(deleted *Table)
	OnTableSplitted(stale *Table, created []*Table)
	OnTableReplaced(stale *Table, created *Table)
	OnTableCompacted(stale *Table)
}

type SegmentListener interface {
	OnSegmentCreated(created *Segment)
	OnSegmentUpgraded(upgraded *Segment, prev *CommitInfo)
}

type BlockListener interface {
	OnBlockCreated(created *Block)
	OnBlockUpgraded(upgraded *Block)
}

type BaseDatabaseListener struct {
	DatabaseCreatedFn     func(*Database)
	DatabaseSoftDeletedFn func(*Database)
	DatabaseHardDeletedFn func(*Database)
	DatabaseCompactedFn   func(*Database)
	DatabaseSplittedFn    func(*Database, []*Database)
	DatabaseReplacedFn    func(*Database, *Database)
}

type BaseTableListener struct {
	SplittedFn                                   func(*Table, []*Table)
	ReplacedFn                                   func(*Table, *Table)
	TableCreatedFn, SoftDeletedFn, HardDeletedFn func(*Table)
	TableCompactedFn                             func(*Table)
}

type BaseSegmentListener struct {
	SegmentCreatedFn  func(*Segment)
	SegmentUpgradedFn func(*Segment, *CommitInfo)
}

type BaseBlockListener struct {
	BlockCreatedFn, BlockUpgradedFn func(*Block)
}

func (l *BaseDatabaseListener) OnDatabaseCreated(database *Database) {
	if l == nil || l.DatabaseCreatedFn == nil {
		return
	}
	l.DatabaseCreatedFn(database)
}

func (l *BaseDatabaseListener) OnDatabaseSoftDeleted(database *Database) {
	if l == nil || l.DatabaseSoftDeletedFn == nil {
		return
	}
	l.DatabaseSoftDeletedFn(database)
}

func (l *BaseDatabaseListener) OnDatabaseHardDeleted(database *Database) {
	if l == nil || l.DatabaseHardDeletedFn == nil {
		return
	}
	l.DatabaseHardDeletedFn(database)
}

func (l *BaseDatabaseListener) OnDatabaseSplitted(stale *Database, created []*Database) {
	if l == nil || l.DatabaseSplittedFn == nil {
		return
	}
	l.DatabaseSplittedFn(stale, created)
}

func (l *BaseDatabaseListener) OnDatabaseReplaced(stale *Database, created *Database) {
	if l == nil || l.DatabaseReplacedFn == nil {
		return
	}
	l.DatabaseReplacedFn(stale, created)
}

func (l *BaseDatabaseListener) OnDatabaseCompacted(stale *Database) {
	if l == nil || l.DatabaseCompactedFn == nil {
		return
	}
	l.DatabaseCompactedFn(stale)
}

func (l *BaseTableListener) OnTableCompacted(table *Table) {
	if l == nil || l.TableCompactedFn == nil {
		return
	}
	l.TableCompactedFn(table)
}

func (l *BaseTableListener) OnTableCreated(table *Table) {
	if l == nil || l.TableCreatedFn == nil {
		return
	}
	l.TableCreatedFn(table)
}

func (l *BaseTableListener) OnTableSoftDeleted(table *Table) {
	if l == nil || l.SoftDeletedFn == nil {
		return
	}
	l.SoftDeletedFn(table)
}

func (l *BaseTableListener) OnTableHardDeleted(table *Table) {
	if l == nil || l.HardDeletedFn == nil {
		return
	}
	l.HardDeletedFn(table)
}

func (l *BaseTableListener) OnTableSplitted(stale *Table, created []*Table) {
	if l == nil || l.SplittedFn == nil {
		return
	}
	l.SplittedFn(stale, created)
}

func (l *BaseTableListener) OnTableReplaced(stale *Table, created *Table) {
	if l == nil || l.ReplacedFn == nil {
		return
	}
	l.ReplacedFn(stale, created)
}

func (l *BaseSegmentListener) OnSegmentCreated(segment *Segment) {
	if l == nil || l.SegmentCreatedFn == nil {
		return
	}
	l.SegmentCreatedFn(segment)
}

func (l *BaseSegmentListener) OnSegmentUpgraded(segment *Segment, prev *CommitInfo) {
	if l == nil || l.SegmentUpgradedFn == nil {
		return
	}
	l.SegmentUpgradedFn(segment, prev)
}

func (l *BaseBlockListener) OnBlockCreated(block *Block) {
	if l == nil || l.BlockCreatedFn == nil {
		return
	}
	l.BlockCreatedFn(block)
}

func (l *BaseBlockListener) OnBlockUpgraded(block *Block) {
	if l == nil || l.BlockUpgradedFn == nil {
		return
	}
	l.BlockUpgradedFn(block)
}

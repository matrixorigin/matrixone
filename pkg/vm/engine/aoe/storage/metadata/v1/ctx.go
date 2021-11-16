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

type writeCtx struct {
	exIndex *LogIndex
	tranId  uint64
	inTran  bool
	txn     *TxnCtx
}

type createDatabaseCtx struct {
	writeCtx
	database *Database
	name     string
}

type dropDatabaseCtx struct {
	writeCtx
	database *Database
}

type deleteDatabaseCtx struct {
	writeCtx
	database *Database
}

type addDatabaseCtx struct {
	writeCtx
	database *Database
}

type addTableCtx struct {
	writeCtx
	table *Table
}

type createTableCtx struct {
	writeCtx
	schema   *Schema
	table    *Table
	database *Database
}

type dropTableCtx struct {
	writeCtx
	table *Table
}

type deleteTableCtx struct {
	writeCtx
	table *Table
}

type createSegmentCtx struct {
	writeCtx
	segment *Segment
	table   *Table
}

type upgradeSegmentCtx struct {
	writeCtx
	segment  *Segment
	exIndice []*LogIndex
	size     int64
}

type createBlockCtx struct {
	writeCtx
	segment *Segment
	block   *Block
}

type upgradeBlockCtx struct {
	writeCtx
	block    *Block
	exIndice []*LogIndex
}

type replaceTableCtx struct {
	writeCtx
	table   *Table
	discard bool
}

type replaceDatabaseCtx struct {
	writeCtx
	view *databaseLogEntry
}

type addReplaceCommitCtx struct {
	writeCtx
	database *Database
	discard  bool
}

type splitDBCtx struct {
	writeCtx
	spec        *ShardSplitSpec
	renameTable RenameTableFactory
	dbSpecs     []*DBSpec
}

func newDeleteTableCtx(table *Table, tranId uint64) *deleteTableCtx {
	return &deleteTableCtx{
		writeCtx: writeCtx{
			tranId: tranId,
		},
		table: table,
	}
}

func newCreateSegmentCtx(table *Table, tranId uint64) *createSegmentCtx {
	return &createSegmentCtx{
		writeCtx: writeCtx{
			tranId: tranId,
		},
		table: table,
	}
}

func newUpgradeSegmentCtx(segment *Segment, size int64, exIndice []*LogIndex, tranId uint64) *upgradeSegmentCtx {
	return &upgradeSegmentCtx{
		writeCtx: writeCtx{
			tranId: tranId,
		},
		segment:  segment,
		exIndice: exIndice,
		size:     size,
	}
}

func newCreateBlockCtx(segment *Segment, tranId uint64) *createBlockCtx {
	return &createBlockCtx{
		writeCtx: writeCtx{
			tranId: tranId,
		},
		segment: segment,
	}
}

func newUpgradeBlockCtx(block *Block, exIndice []*LogIndex, tranId uint64) *upgradeBlockCtx {
	return &upgradeBlockCtx{
		writeCtx: writeCtx{
			tranId: tranId,
		},
		block:    block,
		exIndice: exIndice,
	}
}

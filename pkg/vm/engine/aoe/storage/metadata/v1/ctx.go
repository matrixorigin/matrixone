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
	inTran  bool
}

type addTableCtx struct {
	writeCtx
	table *Table
}

type createTableCtx struct {
	writeCtx
	schema *Schema
	table  *Table
}

type dropTableCtx struct {
	writeCtx
	name  string
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

type replaceShardCtx struct {
	writeCtx
	view *catalogLogEntry
	news []*addTableCtx
	olds []*replaceTableCtx
	// news map[uint64]*createTableCtx
	// olds map[uint64]*replaceTableCtx
}

func newAddTableCtx(table *Table, inTran bool) *addTableCtx {
	return &addTableCtx{
		writeCtx: writeCtx{
			inTran: inTran,
		},
		table: table,
	}
}

func newCreateTableCtx(schema *Schema, exIndex *LogIndex) *createTableCtx {
	return &createTableCtx{
		writeCtx: writeCtx{
			exIndex: exIndex,
		},
		schema: schema,
	}
}

func newDropTableCtx(name string, exIndex *LogIndex) *dropTableCtx {
	return &dropTableCtx{
		writeCtx: writeCtx{
			exIndex: exIndex,
		},
		name: name,
	}
}

func newDeleteTableCtx(table *Table) *deleteTableCtx {
	return &deleteTableCtx{
		table: table,
	}
}

func newCreateSegmentCtx(table *Table) *createSegmentCtx {
	return &createSegmentCtx{
		table: table,
	}
}

func newUpgradeSegmentCtx(segment *Segment, exIndice []*LogIndex) *upgradeSegmentCtx {
	return &upgradeSegmentCtx{
		segment:  segment,
		exIndice: exIndice,
	}
}

func newCreateBlockCtx(segment *Segment) *createBlockCtx {
	return &createBlockCtx{
		segment: segment,
	}
}

func newUpgradeBlockCtx(block *Block, exIndice []*LogIndex) *upgradeBlockCtx {
	return &upgradeBlockCtx{
		block:    block,
		exIndice: exIndice,
	}
}

func newReplaceTableCtx(table *Table, exIndex *LogIndex, inTran bool) *replaceTableCtx {
	return &replaceTableCtx{
		table: table,
		writeCtx: writeCtx{
			inTran:  inTran,
			exIndex: exIndex,
		},
	}
}

func newReplaceShardCtx(view *catalogLogEntry) *replaceShardCtx {
	ctx := &replaceShardCtx{
		writeCtx: writeCtx{
			inTran: true,
		},
		view: view,
		news: make([]*addTableCtx, 0, 10),
		olds: make([]*replaceTableCtx, 0, 10),
		// news: make(map[uint64]*createTableCtx),
		// olds: make(map[uint64]*replaceTableCtx),
	}
	return ctx
}

func (ctx *replaceShardCtx) addNew(nCtx *addTableCtx) {
	ctx.news = append(ctx.news, nCtx)
}

func (ctx *replaceShardCtx) addReplace(oCtx *replaceTableCtx) {
	ctx.olds = append(ctx.olds, oCtx)
}

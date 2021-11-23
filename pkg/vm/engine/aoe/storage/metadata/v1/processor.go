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

type Processor interface {
	OnDatabase(database *Database) error
	OnTable(table *Table) error
	OnSegment(segment *Segment) error
	OnBlock(block *Block) error
}

type LoopProcessor struct {
	DatabaseFn func(*Database) error
	TableFn    func(*Table) error
	SegmentFn  func(*Segment) error
	BlockFn    func(*Block) error
}

func (p *LoopProcessor) OnDatabase(database *Database) error {
	if p.DatabaseFn != nil {
		return p.DatabaseFn(database)
	}
	return nil
}

func (p *LoopProcessor) OnTable(table *Table) error {
	if p.TableFn != nil {
		return p.TableFn(table)
	}
	return nil
}

func (p *LoopProcessor) OnSegment(segment *Segment) error {
	if p.SegmentFn != nil {
		return p.SegmentFn(segment)
	}
	return nil
}

func (p *LoopProcessor) OnBlock(block *Block) error {
	if p.BlockFn != nil {
		return p.BlockFn(block)
	}
	return nil
}

type reallocIdProcessor struct {
	LoopProcessor
	allocator *Sequence
	tranId    uint64
	trace     *Addresses
}

func newReAllocIdProcessor(allocator *Sequence, tranId uint64) *reallocIdProcessor {
	p := &reallocIdProcessor{
		allocator: allocator,
		tranId:    tranId,
	}
	p.DatabaseFn = p.onDatabase
	p.TableFn = p.onTable
	p.SegmentFn = p.onSegment
	p.BlockFn = p.onBlock
	p.trace = new(Addresses)
	p.trace.Database = make(map[uint64]uint64)
	p.trace.Table = make(map[uint64]uint64)
	p.trace.Segment = make(map[uint64]uint64)
	p.trace.Block = make(map[uint64]uint64)
	p.trace.ShardId = make(map[uint64]uint64)
	return p
}

func (p *reallocIdProcessor) onDatabase(db *Database) error {
	old := db.Id
	db.Id = p.allocator.NextDatabaseId()
	p.trace.Database[old] = db.Id

	db.CommitInfo.CommitId = p.tranId
	db.CommitInfo.TranId = p.tranId
	if db.CommitInfo.LogIndex != nil {
		p.trace.ShardId[db.CommitInfo.LogIndex.ShardId] = db.Id
		db.CommitInfo.LogIndex.ShardId = db.Id
	}
	return nil
}

func (p *reallocIdProcessor) onTable(table *Table) error {
	old := table.Id
	table.Id = p.allocator.NextTableId()
	p.trace.Table[old] = table.Id
	table.CommitInfo.CommitId = p.tranId
	table.CommitInfo.TranId = p.tranId
	if table.CommitInfo.LogIndex != nil {
		table.CommitInfo.LogIndex.ShardId = p.trace.ShardId[table.CommitInfo.LogIndex.ShardId]
	}
	return nil
}

func (p *reallocIdProcessor) onSegment(segment *Segment) error {
	old := segment.Id
	segment.Id = p.allocator.NextSegmentId()
	p.trace.Segment[old] = segment.Id
	segment.CommitInfo.CommitId = p.tranId
	segment.CommitInfo.TranId = p.tranId
	if segment.CommitInfo.LogIndex != nil {
		segment.CommitInfo.LogIndex.ShardId = p.trace.ShardId[segment.CommitInfo.LogIndex.ShardId]
	}
	return nil
}

func (p *reallocIdProcessor) onBlock(block *Block) error {
	old := block.Id
	block.Id = p.allocator.NextBlockId()
	p.trace.Block[old] = block.Id
	block.CommitInfo.CommitId = p.tranId
	block.CommitInfo.TranId = p.tranId
	if block.CommitInfo.LogIndex != nil {
		block.CommitInfo.LogIndex.ShardId = p.trace.ShardId[block.CommitInfo.LogIndex.ShardId]
	}
	if block.CommitInfo.LogRange != nil {
		block.CommitInfo.LogRange.ShardId = p.trace.ShardId[block.CommitInfo.LogIndex.ShardId]
	}
	return nil
}

func newBlockProcessor(fn func(block *Block) error) *LoopProcessor {
	return &LoopProcessor{
		BlockFn: fn,
	}
}

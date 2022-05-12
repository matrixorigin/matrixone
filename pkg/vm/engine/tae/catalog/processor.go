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

package catalog

type Processor interface {
	OnDatabase(database *DBEntry) error
	OnTable(table *TableEntry) error
	OnPostSegment(segment *SegmentEntry) error
	OnSegment(segment *SegmentEntry) error
	OnBlock(block *BlockEntry) error
}

type LoopProcessor struct {
	DatabaseFn    func(*DBEntry) error
	TableFn       func(*TableEntry) error
	SegmentFn     func(*SegmentEntry) error
	BlockFn       func(*BlockEntry) error
	PostSegmentFn func(*SegmentEntry) error
}

func (p *LoopProcessor) OnDatabase(database *DBEntry) error {
	if p.DatabaseFn != nil {
		return p.DatabaseFn(database)
	}
	return nil
}

func (p *LoopProcessor) OnTable(table *TableEntry) error {
	if p.TableFn != nil {
		return p.TableFn(table)
	}
	return nil
}

func (p *LoopProcessor) OnPostSegment(segment *SegmentEntry) error {
	if p.PostSegmentFn != nil {
		return p.PostSegmentFn(segment)
	}
	return nil
}

func (p *LoopProcessor) OnSegment(segment *SegmentEntry) error {
	if p.SegmentFn != nil {
		return p.SegmentFn(segment)
	}
	return nil
}

func (p *LoopProcessor) OnBlock(block *BlockEntry) error {
	if p.BlockFn != nil {
		return p.BlockFn(block)
	}
	return nil
}

package metadata

type LoopProcessor interface {
	OnTable(table *Table) error
	OnSegment(segment *Segment) error
	OnBlock(block *Block) error
}

type loopProcessor struct {
	TableFn   func(*Table) error
	SegmentFn func(*Segment) error
	BlockFn   func(*Block) error
}

func (p *loopProcessor) OnTable(table *Table) error {
	if p.TableFn != nil {
		return p.TableFn(table)
	}
	return nil
}

func (p *loopProcessor) OnSegment(segment *Segment) error {
	if p.SegmentFn != nil {
		return p.SegmentFn(segment)
	}
	return nil
}

func (p *loopProcessor) OnBlock(block *Block) error {
	if p.BlockFn != nil {
		return p.BlockFn(block)
	}
	return nil
}

type reallocIdProcessor struct {
	loopProcessor
	allocator *Sequence
}

func newReAllocIdProcessor(allocator *Sequence) *reallocIdProcessor {
	p := &reallocIdProcessor{
		allocator: allocator,
	}
	p.TableFn = p.onTable
	p.SegmentFn = p.onSegment
	p.BlockFn = p.onBlock
	return p
}

func (p *reallocIdProcessor) onTable(table *Table) error {
	table.Id = p.allocator.NextTableId()
	table.CommitInfo.CommitId = p.allocator.NextUncommitId()
	return nil
}

func (p *reallocIdProcessor) onSegment(segment *Segment) error {
	segment.Id = p.allocator.NextSegmentId()
	segment.CommitInfo.CommitId = p.allocator.NextUncommitId()
	return nil
}

func (p *reallocIdProcessor) onBlock(block *Block) error {
	block.Id = p.allocator.NextBlockId()
	block.CommitInfo.CommitId = p.allocator.NextUncommitId()
	return nil
}

func newBlockProcessor(fn func(block *Block) error) *loopProcessor {
	return &loopProcessor{
		BlockFn: fn,
	}
}

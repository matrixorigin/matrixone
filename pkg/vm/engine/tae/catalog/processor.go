package catalog

type Processor interface {
	OnDatabase(database *DBEntry) error
	OnTable(table *TableEntry) error
	OnSegment(segment *SegmentEntry) error
	OnBlock(block *BlockEntry) error
}

type LoopProcessor struct {
	DatabaseFn func(*DBEntry) error
	TableFn    func(*TableEntry) error
	SegmentFn  func(*SegmentEntry) error
	BlockFn    func(*BlockEntry) error
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

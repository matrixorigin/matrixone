package metadata

type DatabaseListener interface {
	OnDatabaseCreated(created *Database)
	OnDatabaseSoftDeleted(deleted *Database)
	OnDatabaseHardDeleted(deleted *Database)
	OnDatabaseSplitted(stale *Database, created []*Database)
	OnDatabaseReplaced(stale *Database, created *Database)
}

type TableListener interface {
	OnTableCreated(created *Table)
	OnTableSoftDeleted(deleted *Table)
	OnTableHardDeleted(deleted *Table)
	OnTableSplitted(stale *Table, created []*Table)
	OnTableReplaced(stale *Table, created *Table)
}

type SegmentListener interface {
	OnSegmentCreated(created *Segment)
	OnSegmentUpgraded(upgraded *Segment, prev *CommitInfo)
}

type BlockListener interface {
	OnBlockCreated(created *Block)
	OnBlockUpgraded(upgraded *Block)
}

type BaseTableListener struct {
	SplittedFn                                   func(*Table, []*Table)
	ReplacedFn                                   func(*Table, *Table)
	TableCreatedFn, SoftDeletedFn, HardDeletedFn func(*Table)
}

type BaseSegmentListener struct {
	SegmentCreatedFn  func(*Segment)
	SegmentUpgradedFn func(*Segment, *CommitInfo)
}

type BaseBlockListener struct {
	BlockCreatedFn, BlockUpgradedFn func(*Block)
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

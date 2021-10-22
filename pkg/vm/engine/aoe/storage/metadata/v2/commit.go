package metadata

import "sync"

type commitPipeline struct {
	mu      *sync.RWMutex
	catalog *Catalog
}

func newCommitPipeline(catalog *Catalog) *commitPipeline {
	return &commitPipeline{
		mu:      catalog.RWMutex,
		catalog: catalog,
	}
}

func (p *commitPipeline) prepare(ctx interface{}) (LogEntry, error) {
	switch v := ctx.(type) {
	case *createTableCtx:
		return p.catalog.prepareCreateTable(v)
	case *dropTableCtx:
		return v.table.prepareSoftDelete(v)
	case *deleteTableCtx:
		return v.table.prepareHardDelete(v)
	case *createSegmentCtx:
		return v.table.prepareCreateSegment(v)
	case *upgradeSegmentCtx:
		return v.segment.prepareUpgrade(v)
	case *createBlockCtx:
		return v.segment.prepareCreateBlock(v)
	case *upgradeBlockCtx:
		return v.block.prepareUpgrade(v)
	}
	panic("not supported")
}

func (p *commitPipeline) commit(entry LogEntry) error {
	if err := entry.WaitDone(); err != nil {
		return err
	}
	return nil
}

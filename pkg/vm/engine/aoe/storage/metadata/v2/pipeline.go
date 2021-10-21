package metadata

import "sync"

type CtxT = LogEntryType

type writePipeline struct {
	mu      *sync.RWMutex
	catalog *Catalog
}

func newWritePipeline(catalog *Catalog) *writePipeline {
	return &writePipeline{
		mu:      catalog.RWMutex,
		catalog: catalog,
	}
}

func (p *writePipeline) prepare(ctx interface{}) (LogEntry, error) {
	switch v := ctx.(type) {
	case *createTableCtx:
		return p.catalog.prepareCreateTable(v)
	case *dropTableCtx:
		return v.table.prepareSoftDelete(v)
	}
	panic("not supported")
}

func (p *writePipeline) commit(entry LogEntry) error {
	if err := entry.WaitDone(); err != nil {
		return err
	}
	return nil
}

package engine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"time"
)

func (a *aoeReader) NewFilter() engine.Filter {
	return nil
}

func (a *aoeReader) NewSummarizer() engine.Summarizer {
	return nil
}

func (a *aoeReader) NewSparseFilter() engine.SparseFilter {
	return NewAoeSparseFilter(a.reader, a)
}

func (a *aoeReader) Read(refCount []uint64, attrs []string) (*batch.Batch, error) {
	if a.prv != nil {
		a.prv.use = false
	}
	tim := time.Now()
	bat := a.reader.GetBatch(refCount, attrs)
	a.dequeue += time.Since(tim).Milliseconds()
	a.prv = bat
	if bat == nil {
		logutil.Infof("dequeue latency: %d ", a.dequeue)
		return nil, nil
	}
	return bat.bat, nil
}

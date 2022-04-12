package engine

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	if a.reader == nil {
		return nil, nil
	}
	dequeue := time.Now()
	bat := a.reader.GetBatch(refCount, attrs, a)
	a.dequeue += time.Since(dequeue).Milliseconds()
	if a.prv != nil && bat != nil {
		enqueue := time.Now()
		a.reader.PutBuffer(a.prv, a.workerid)
		a.enqueue += time.Since(enqueue).Milliseconds()
	}
	a.prv = bat
	if bat == nil {
		logutil.Infof("readerid: %d, dequeue latency: %d, enqueue latency: %d , workerid: %d",
			a.id, a.dequeue, a.enqueue, a.workerid)
		return nil, nil
	}
	return bat.bat, nil
}

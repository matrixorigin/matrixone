package engine

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine"
)

func (a aoeReader) NewFilter() engine.Filter {
	return nil
}

func (a aoeReader) NewSummarizer() engine.Summarizer {
	return nil
}

func (a aoeReader) NewSparseFilter() engine.SparseFilter {
	return nil
}

func (a aoeReader) Read(refCount []uint64, attrs []string) (*batch.Batch, error) {
	if a.blocks == nil || len(*(a.blocks)) == 0 {
		return nil, nil
	}

	if len(a.cds) == 0 {
		a.cds = make([]*bytes.Buffer, len(attrs))
		a.dds = make([]*bytes.Buffer, len(attrs))
		for i := range attrs {
			(a.cds)[i] = bytes.NewBuffer(make([]byte, 1<<20))
			(a.dds)[i] = bytes.NewBuffer(make([]byte, 1<<20))
		}
	}

	(*(a.blocks))[0].Prefetch(attrs)

	bat, err := (*(a.blocks))[0].Read(refCount, attrs, (a.cds), (a.dds))
	if err != nil {
		return nil, err
	}
	n := vector.Length(bat.Vecs[0])
	if n > cap(a.zs) {
		a.zs = make([]int64, n)
	}
	bat.Zs = a.zs
	for i := 0; i < n; i++ {
		bat.Zs[i] = 1
	}
	if len(*(a.blocks)) > 1 {
		*(a.blocks) = append((*(a.blocks))[:1], (*(a.blocks))[2:]...)
	} else if len(*(a.blocks)) == 1 {
		*(a.blocks) = (*(a.blocks))[0:0]
	}
	return bat, nil
}

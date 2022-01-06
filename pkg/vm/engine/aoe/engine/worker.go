package engine

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (w *worker) ID() int32 {
	return w.id
}

func (w *worker) Start(refCount []uint64, attrs []string)  {
	for i :=0; i < len(w.blocks); i++ {
		if len(w.cds) == 0 {
			w.cds = make([]*bytes.Buffer, len(attrs))
			w.dds = make([]*bytes.Buffer, len(attrs))
			for i := range attrs {
				w.cds[i] = bytes.NewBuffer(make([]byte, 1<<20))
				w.dds[i] = bytes.NewBuffer(make([]byte, 1<<20))
			}
		}

		if i < len(w.blocks) - 1 {
			w.blocks[i+1].Prefetch(attrs)
		}

		bat, err := w.blocks[i].Read(refCount, attrs, w.cds, w.dds)
		if err != nil {
			panic("error")
		}
		n := vector.Length(bat.Vecs[0])
		if n > cap(w.zs) {
			w.zs = make([]int64, n)
		}
		bat.Zs = w.zs[:n]
		for i := 0; i < n; i++ {
			bat.Zs[i] = 1
		}
		w.storeReader.SetBatch(bat)
	}
	w.storeReader.RemoveWorker(w.id)
}

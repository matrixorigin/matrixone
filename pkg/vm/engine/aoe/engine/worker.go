package engine

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"time"
)

func (w *worker) ID() int32 {
	return w.id
}

func (w *worker) Alloc(attrs []string) *batData{
	num := 4
	if len(w.batDatas) == 0 {
		tim := time.Now()
		w.batDatas = make([]*batData, num)
		for i := 0; i < num; i++ {
			cds := make([]*bytes.Buffer, len(attrs))
			dds := make([]*bytes.Buffer, len(attrs))
			for a := range attrs {
				cds[a] = bytes.NewBuffer(make([]byte, 1<<20))
				dds[a] = bytes.NewBuffer(make([]byte, 1<<20))
			}
			w.batDatas[i] = &batData{
				bat: nil,
				cds: cds,
				dds: dds,
				use: false,
				workerid: w.id,
			}
		}
		logutil.Infof("workerId: %d, make latency: %d", w.id, time.Since(tim).Milliseconds())
	}
	for {
		t := time.Now()
		for j := range w.batDatas {
			if w.batDatas[j] == nil {
				logutil.Infof("batDatas is nil")
			}
			if !w.batDatas[j].use {
				w.batDatas[j].use = true
				w.allocTime += time.Since(t).Milliseconds()
				return w.batDatas[j]
			}
		}
		time.Sleep(time.Microsecond * 500)
	}
}

func (w *worker) Start(refCount []uint64, attrs []string)  {

	for i :=0; i < len(w.blocks); i++ {
		if i < len(w.blocks) - 1 {
			w.blocks[i+1].Prefetch(attrs)
		}
		data := w.Alloc(attrs)
		bat, err := w.blocks[i].Read(refCount, attrs, data.cds, data.dds)
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
		data.bat = bat
		tim := time.Now()
		w.storeReader.SetBatch(data)
		w.enqueue += time.Since(tim).Milliseconds()
	}
	logutil.Infof("workerId: %d, alloc latency: %d, enqueue latency: %d", w.id, w.allocTime, w.enqueue)
	w.storeReader.RemoveWorker(w.id)
}

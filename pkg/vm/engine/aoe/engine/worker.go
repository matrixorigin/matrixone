// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func (w *worker) ID() int32 {
	return w.id
}

func (w *worker) alloc(attrs []string) *batData {
	if len(w.batDatas) == 0 {
		tim := time.Now()
		w.batDatas = make([]*batData, w.bufferCount)
		for i := 0; i < w.bufferCount; i++ {
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
				id:  int8(i),
			}
		}
		for j := range w.batDatas {
			if !w.batDatas[j].use {
				w.storeReader.PutBuffer(w.batDatas[j], w.id)
			}
		}
		logutil.Infof("workerId: %d, make latency: %d", w.id, time.Since(tim).Milliseconds())
	}
	for {
		bat := w.storeReader.GetBuffer(w.id)
		return bat
	}
}

func (w *worker) Start(refCount []uint64, attrs []string) {
	for i := 0; i < len(w.blocks); i++ {
		if i < len(w.blocks)-1 {
			w.blocks[i+1].Prefetch(attrs)
		}
		t := time.Now()
		data := w.alloc(attrs)
		w.allocLatency += time.Since(t).Milliseconds()
		now := time.Now()
		bat, err := w.blocks[i].Read(refCount, attrs, data.cds, data.dds)
		w.readLatency += time.Since(now).Milliseconds()
		if err != nil {
			panic("error")
		}
		n := vector.Length(bat.Vecs[0])
		if n > cap(data.zs) {
			data.zs = make([]int64, n)
		}
		bat.Zs = data.zs[:n]
		for i := 0; i < n; i++ {
			bat.Zs[i] = 1
		}
		data.bat = bat
		enqueue := time.Now()
		w.storeReader.SetBatch(data, w.id)
		w.enqueue += time.Since(enqueue).Microseconds()
	}
	logutil.Infof("workerId: %d, alloc latency: %d ms, enqueue latency: %d us, read latency: %d ms",
		w.id, w.allocLatency, w.enqueue, w.readLatency)
	w.storeReader.SetBatch(nil, w.id)
	w.storeReader.CloseRhs(w.id)
}

// Copyright 2024 Matrix Origin
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

package shuffle

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ShufflePool struct {
	bucketNum int32
	holders   atomic.Int32
	batches   []*batch.Batch
	locks     []sync.Mutex
}

func NewShufflePool(bucketNum int32) *ShufflePool {
	sp := &ShufflePool{
		bucketNum: bucketNum,
		locks:     make([]sync.Mutex, bucketNum)}
	return sp
}

func (sp *ShufflePool) Hold() {
	sp.holders.Add(1)
}

func (sp *ShufflePool) Ending() {
	sp.holders.Add(-1)
}

func (sp *ShufflePool) Reset(m *mpool.MPool) {
	h := sp.holders.Load()
	if h > 0 {
		return
	}
	for i := range sp.batches {
		if sp.batches[i] != nil {
			sp.batches[i].Clean(m)
		}
	}
	sp.batches = nil
}

func (sp *ShufflePool) Print() { // only for debug
	for i := range sp.batches {
		bat := sp.batches[i]
		if bat == nil {
			logutil.Infof("shuffle pool %p batches[%v] is nil", sp, i)
		} else {
			logutil.Infof("shuffle pool %p batches[%v] rowcnt %v", sp, i, bat.RowCount())
		}
	}
}

// shuffle operator is ending, release buf and sending remaining batches
func (sp *ShufflePool) GetEndingBatch(buf *batch.Batch, proc *process.Process) *batch.Batch {
	if buf != nil {
		buf.Clean(proc.Mp())
	}
	h := sp.holders.Load()
	if h > 0 {
		return nil
	}
	for i := range sp.batches {
		bat := sp.batches[i]
		if bat != nil && bat.RowCount() > 0 {
			sp.batches[i] = nil
			return bat
		}
	}
	return nil
}

// if there is full batch (>8192 rows) in pool, return it and put buf in the place to continue writing into pool
func (sp *ShufflePool) GetFullBatch(buf *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	var err error
	if buf != nil {
		buf.CleanOnlyData()
	}
	for i := range sp.batches {
		bat := sp.batches[i]
		if bat != nil && bat.RowCount() >= colexec.DefaultBatchSize {
			sp.locks[i].Lock()
			defer sp.locks[i].Unlock()
			//find a full batch, put buf in place
			if buf == nil {
				buf, err = proc.NewBatchFromSrc(bat, colexec.DefaultBatchSize)
				if err != nil {
					return nil, err
				}
			}
			buf.ShuffleIDX = bat.ShuffleIDX
			sp.batches[i] = buf
			return bat, nil
		}
	}
	//no full batch in pool
	return buf, nil
}

func (sp *ShufflePool) putBatchIntoShuffledPoolsBySels(srcBatch *batch.Batch, sels [][]int64, proc *process.Process) error {
	var err error
	for i := range sp.batches {
		currentSels := sels[i]
		if len(currentSels) > 0 {
			sp.locks[i].Lock()
			bat := sp.batches[i]
			if bat == nil {
				bat, err = proc.NewBatchFromSrc(srcBatch, colexec.DefaultBatchSize)
				if err != nil {
					sp.locks[i].Unlock()
					return err
				}
				bat.ShuffleIDX = int32(i)
				sp.batches[i] = bat
			}
			for vecIndex := range bat.Vecs {
				v := bat.Vecs[vecIndex]
				v.SetSorted(false)
				err = v.Union(srcBatch.Vecs[vecIndex], currentSels, proc.Mp())
				if err != nil {
					sp.locks[i].Unlock()
					return err
				}
			}
			bat.AddRowCount(len(currentSels))
			sp.locks[i].Unlock()
		}
	}
	return nil
}

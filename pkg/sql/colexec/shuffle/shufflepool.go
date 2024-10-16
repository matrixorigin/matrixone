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

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ShufflePool struct {
	bucketNum    int32
	maxHolders   int32
	holders      int32
	finished     int32
	batches      []*batch.Batch
	lock         sync.Mutex
	locks        []sync.Mutex
	fullBatchIdx []int
}

func NewShufflePool(bucketNum int32, maxHolders int32) *ShufflePool {
	sp := &ShufflePool{bucketNum: bucketNum, maxHolders: maxHolders}
	sp.holders = 0
	sp.finished = 0
	sp.batches = make([]*batch.Batch, sp.bucketNum)
	sp.locks = make([]sync.Mutex, bucketNum)
	sp.fullBatchIdx = make([]int, 0, bucketNum)
	return sp
}

func (sp *ShufflePool) Hold() {
	sp.lock.Lock()
	defer sp.lock.Unlock()
	sp.holders++
	if sp.holders > sp.maxHolders {
		panic("shuffle pool too many holders!")
	}
}

func (sp *ShufflePool) Ending() bool {
	sp.lock.Lock()
	defer sp.lock.Unlock()
	sp.finished++
	if sp.finished > sp.maxHolders || sp.finished > sp.holders {
		panic("shuffle pool too many finished!")
	}
	return sp.finished == sp.maxHolders
}

func (sp *ShufflePool) Reset(m *mpool.MPool, force bool) {
	sp.lock.Lock()
	defer sp.lock.Unlock()
	if force {
		logutil.Warnf("shuffle pool force reset, maxHolders %v, holders %v, finished %v", sp.maxHolders, sp.holders, sp.finished)
		return
	}
	if sp.maxHolders != sp.holders || sp.maxHolders != sp.finished {
		logutil.Errorf("shuffle pool reset with invalid state! maxHolders %v, holders %v, finished %v", sp.maxHolders, sp.holders, sp.finished)
		panic("shuffle pool reset with invalid state! ")
	}
	for i := range sp.batches {
		if sp.batches[i] != nil {
			sp.batches[i].Clean(m)
		}
	}
	sp.fullBatchIdx = sp.fullBatchIdx[:0]
	sp.holders = 0
	sp.finished = 0
}

func (sp *ShufflePool) Print() { // only for debug
	sp.lock.Lock()
	defer sp.lock.Unlock()
	logutil.Warnf("shuffle pool print, maxHolders %v, holders %v, finished %v", sp.maxHolders, sp.holders, sp.finished)
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
	sp.lock.Lock()
	defer sp.lock.Unlock()
	if sp.finished < sp.maxHolders {
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
	sp.lock.Lock()
	defer sp.lock.Unlock()

	length := len(sp.fullBatchIdx)
	if length == 0 {
		return buf, nil
	}
	fullIdx := sp.fullBatchIdx[length-1]
	sp.fullBatchIdx = sp.fullBatchIdx[:length-1]
	sp.locks[fullIdx].Lock()
	defer sp.locks[fullIdx].Unlock()

	bat := sp.batches[fullIdx]
	//find a full batch, put buf in place
	if buf == nil {
		buf, err = proc.NewBatchFromSrc(bat, colexec.DefaultBatchSize)
		if err != nil {
			return nil, err
		}
	}
	buf.ShuffleIDX = bat.ShuffleIDX
	sp.batches[fullIdx] = buf
	return bat, nil

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
			if bat.RowCount() > colexec.DefaultBatchSize-512 && sp.lock.TryLock() {
				found := false
				for _, j := range sp.fullBatchIdx {
					if i == j {
						//already in full batch index
						found = true
						break
					}
				}
				if !found {
					sp.fullBatchIdx = append(sp.fullBatchIdx, i)
				}
				sp.lock.Unlock()
			}
			sp.locks[i].Unlock()
		}
	}
	return nil
}

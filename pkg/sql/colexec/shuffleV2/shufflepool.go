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

type ShufflePoolV2 struct {
	bucketNum     int32
	maxHolders    int32
	holders       int32
	finished      int32
	batches       []*batch.Batch
	holderLock    sync.Mutex
	batchLocks    []sync.Mutex
	allHoldersEnd chan bool
	waiters       []chan bool
}

func NewShufflePool(bucketNum int32, maxHolders int32) *ShufflePoolV2 {
	sp := &ShufflePoolV2{bucketNum: bucketNum, maxHolders: maxHolders}
	sp.holders = 0
	sp.finished = 0
	sp.batches = make([]*batch.Batch, sp.bucketNum)
	sp.batchLocks = make([]sync.Mutex, bucketNum)
	sp.allHoldersEnd = make(chan bool, 1)
	sp.waiters = make([]chan bool, bucketNum)
	for i := range sp.waiters {
		sp.waiters[i] = make(chan bool, 1)
	}
	return sp
}

func (sp *ShufflePoolV2) Hold() {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	sp.holders++
	if sp.holders > sp.maxHolders {
		panic("shuffle pool too many holders!")
	}
}

func (sp *ShufflePoolV2) Ending() {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	sp.finished++
	if sp.finished > sp.maxHolders || sp.finished > sp.holders {
		panic("shuffle pool too many finished!")
	}
	if sp.finished == sp.maxHolders {
		sp.allHoldersEnd <- true
	}
}

func (sp *ShufflePoolV2) Reset(m *mpool.MPool) {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	if sp.maxHolders != sp.holders || sp.maxHolders != sp.finished {
		return // still some other shuffle operators working
	}
	for i := range sp.batches {
		if sp.batches[i] != nil {
			sp.batches[i].Clean(m)
		}
	}
	sp.holders = 0
	sp.finished = 0
}

func (sp *ShufflePoolV2) Print() { // only for debug
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
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
func (sp *ShufflePoolV2) GetEndingBatch(buf *batch.Batch, shuffleIDX int32) (*batch.Batch, bool) {
	switch {
	case <-sp.waiters[shuffleIDX]:
		bat := sp.GetFullBatch(buf, shuffleIDX)
		return bat, false
	case <-sp.allHoldersEnd:
		sp.allHoldersEnd <- true
		return sp.batches[shuffleIDX], true
	}
	panic("error get ending batch!")
}

// if there is full batch  in pool, return it and put buf in the place to continue writing into pool
func (sp *ShufflePoolV2) GetFullBatch(buf *batch.Batch, shuffleIDX int32) *batch.Batch {
	sp.batchLocks[shuffleIDX].Lock()
	defer sp.batchLocks[shuffleIDX].Unlock()
	bat := sp.batches[shuffleIDX]
	if bat.RowCount() < colexec.DefaultBatchSize-512 { // not full
		return nil
	}
	//find a full batch, put buf in place
	if buf != nil {
		buf.CleanOnlyData()
		buf.ShuffleIDX = bat.ShuffleIDX
		sp.batches[shuffleIDX] = buf
	}
	return bat
}

func (sp *ShufflePoolV2) initBatch(srcBatch *batch.Batch, proc *process.Process, shuffleIDX int32) error {
	bat := sp.batches[shuffleIDX]
	if bat == nil {
		var err error
		bat, err = proc.NewBatchFromSrc(srcBatch, colexec.DefaultBatchSize)
		if err != nil {
			return err
		}
		bat.ShuffleIDX = shuffleIDX
		sp.batches[shuffleIDX] = bat
	}
	return nil
}

func (sp *ShufflePoolV2) putAllBatchIntoPoolByShuffleIdx(srcBatch *batch.Batch, proc *process.Process, shuffleIDX int32) error {
	sp.batchLocks[shuffleIDX].Lock()
	defer sp.batchLocks[shuffleIDX].Unlock()
	var err error
	sp.batches[shuffleIDX], err = sp.batches[shuffleIDX].AppendWithCopy(proc.Ctx, proc.Mp(), srcBatch)
	if err != nil {
		return err
	}
	if sp.batches[shuffleIDX].RowCount() > colexec.DefaultBatchSize-512 && len(sp.waiters[shuffleIDX]) == 0 {
		sp.waiters[shuffleIDX] <- true
	}
	return nil
}

func (sp *ShufflePoolV2) putBatchIntoShuffledPoolsBySels(srcBatch *batch.Batch, sels [][]int32, proc *process.Process) error {
	var err error
	for i := range sp.batches {
		currentSels := sels[i]
		if len(currentSels) > 0 {
			sp.batchLocks[i].Lock()
			err = sp.initBatch(srcBatch, proc, int32(i))
			if err != nil {
				sp.batchLocks[i].Unlock()
				return err
			}
			bat := sp.batches[i]
			for vecIndex := range bat.Vecs {
				v := bat.Vecs[vecIndex]
				v.SetSorted(false)
				err = v.UnionInt32(srcBatch.Vecs[vecIndex], currentSels, proc.Mp())
				if err != nil {
					sp.batchLocks[i].Unlock()
					return err
				}
			}
			bat.AddRowCount(len(currentSels))
			if bat.RowCount() > colexec.DefaultBatchSize-512 && len(sp.waiters[i]) == 0 {
				sp.waiters[i] <- true
			}
			sp.batchLocks[i].Unlock()
		}
	}
	return nil
}

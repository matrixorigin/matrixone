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

package shuffleV2

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ShufflePoolV2 struct {
	waitFactor    int32
	bucketNum     int32
	maxHolders    int32
	holders       int32
	finished      int32
	stoppers      int32
	batches       []*batch.Batch
	holderLock    sync.Mutex
	batchLocks    []sync.Mutex
	writeWaiters  []chan bool
	endingWaiters []chan bool
	batchWaiters  []chan bool
	inputCNT      []int64 //for debug
	outputCNT     []int64
	inputTotal    int64
	outputTotal   int64
}

func NewShufflePool(bucketNum int32, maxHolders int32) *ShufflePoolV2 {
	sp := &ShufflePoolV2{bucketNum: bucketNum, maxHolders: maxHolders}
	sp.waitFactor = sp.bucketNum / 2
	if sp.waitFactor > 64 {
		sp.waitFactor = 64
	}
	sp.holders = 0
	sp.finished = 0
	sp.stoppers = 0
	sp.batches = make([]*batch.Batch, sp.bucketNum)
	sp.batchLocks = make([]sync.Mutex, bucketNum)
	sp.endingWaiters = make([]chan bool, bucketNum)
	sp.batchWaiters = make([]chan bool, bucketNum)
	sp.writeWaiters = make([]chan bool, bucketNum)
	for i := range sp.writeWaiters {
		sp.writeWaiters[i] = make(chan bool, 1)
	}
	for i := range sp.batchWaiters {
		sp.batchWaiters[i] = make(chan bool, 1)
	}
	for i := range sp.endingWaiters {
		sp.endingWaiters[i] = make(chan bool, 1)
	}
	sp.inputCNT = make([]int64, bucketNum)
	sp.outputCNT = make([]int64, bucketNum)
	sp.inputTotal = 0
	sp.outputTotal = 0
	return sp
}

func (sp *ShufflePoolV2) hold() {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	sp.holders++
	if sp.holders > sp.maxHolders {
		panic("shuffle pool too many holders!")
	}
}

func (sp *ShufflePoolV2) stopWriting() {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	sp.stoppers++
	if sp.stoppers > sp.holders || sp.stoppers > sp.maxHolders {
		panic("shuffle pool too many stoppers!")
	}

	if sp.stoppers == sp.maxHolders {
		for i := range sp.endingWaiters {
			sp.endingWaiters[i] <- true
		}
	}
}

func (sp *ShufflePoolV2) Reset(m *mpool.MPool) {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	sp.finished++
	if sp.maxHolders != sp.holders || sp.maxHolders != sp.finished {
		return // still some other shuffle operators working
	}

	maxCNT := sp.inputCNT[0]
	minCNT := sp.inputCNT[0]
	for i := range sp.inputCNT {
		sp.inputTotal += sp.inputCNT[i]
		sp.outputTotal += sp.outputCNT[i]
		if sp.inputCNT[i] > maxCNT {
			maxCNT = sp.inputCNT[i]
		}
		if sp.inputCNT[i] < minCNT {
			minCNT = sp.inputCNT[i]
		}
	}

	logutil.Infof("shuffle pool stats: bucket num %v, input %v, output %v, average %v, max %v, min %v",
		sp.bucketNum, sp.inputTotal, sp.outputTotal, sp.inputTotal/int64(sp.bucketNum), maxCNT, minCNT)

	for i := range sp.batches {
		if sp.batches[i] != nil {
			if sp.batches[i].RowCount() > 0 {
				logutil.Warnf("shuffle pool reset, batch %v rowcnt %v, maybe something wrong!", i, sp.batches[i].RowCount())
			}
			sp.batches[i].Clean(m)
		}
	}
	sp.holders = 0
	sp.finished = 0
	sp.stoppers = 0
	sp.endingWaiters = nil
	sp.writeWaiters = nil
	sp.batchWaiters = nil
	sp.inputCNT = nil
	sp.outputCNT = nil
	sp.inputTotal = 0
	sp.outputTotal = 0
}

func (sp *ShufflePoolV2) Print() { // only for debug
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	logutil.Warnf("shuffle pool print, maxHolders %v, holders %v, finished %v, stop writing %v", sp.maxHolders, sp.holders, sp.finished, sp.stoppers)
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
func (sp *ShufflePoolV2) getEndingBatch(buf *batch.Batch, shuffleIDX int32, proc *process.Process) *batch.Batch {
	for {
		bat := sp.getFullBatch(buf, shuffleIDX)
		if bat != nil && bat.RowCount() > 0 {
			return bat
		}
		select {
		case <-sp.batchWaiters[shuffleIDX]:
			bat = sp.getFullBatch(buf, shuffleIDX)
			if bat != nil && bat.RowCount() > 0 {
				return bat
			}
		case <-sp.endingWaiters[shuffleIDX]:
			sp.endingWaiters[shuffleIDX] <- true
			bat = sp.batches[shuffleIDX]
			sp.batches[shuffleIDX] = nil
			if bat != nil {
				sp.outputCNT[shuffleIDX] += int64(bat.RowCount())
			}
			return bat
		case <-proc.Ctx.Done():
			return nil
		}
	}
}

// if there is full batch  in pool, return it and put buf in the place to continue writing into pool
func (sp *ShufflePoolV2) getFullBatch(buf *batch.Batch, shuffleIDX int32) *batch.Batch {
	sp.batchLocks[shuffleIDX].Lock()
	defer sp.batchLocks[shuffleIDX].Unlock()
	bat := sp.batches[shuffleIDX]
	if bat == nil || bat.RowCount() < colexec.DefaultBatchSize { // not full
		return nil
	}
	//find a full batch, put buf in place
	if buf != nil {
		buf.CleanOnlyData()
		buf.ShuffleIDX = bat.ShuffleIDX
	}
	sp.batches[shuffleIDX] = buf
	if len(sp.writeWaiters[shuffleIDX]) == 0 {
		sp.writeWaiters[shuffleIDX] <- true
	}
	sp.outputCNT[shuffleIDX] += int64(bat.RowCount())
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

func (sp *ShufflePoolV2) waitIfTooLarge(proc *process.Process, shuffleIDX int32) bool {
	for {
		if sp.batches[shuffleIDX] != nil && sp.batches[shuffleIDX].RowCount() > int(colexec.DefaultBatchSize*sp.waitFactor) {
			// batch too large, need to wait
			sp.batchLocks[shuffleIDX].Unlock()
			select {
			case <-sp.writeWaiters[shuffleIDX]:
				sp.batchLocks[shuffleIDX].Lock()
			case <-proc.Ctx.Done():
				return true
			}
		} else {
			break
		}
	}
	if len(sp.writeWaiters[shuffleIDX]) == 0 {
		sp.writeWaiters[shuffleIDX] <- true
	}
	return false
}

func (sp *ShufflePoolV2) putAllBatchIntoPoolByShuffleIdx(srcBatch *batch.Batch, proc *process.Process, shuffleIDX int32) error {
	sp.batchLocks[shuffleIDX].Lock()
	defer sp.batchLocks[shuffleIDX].Unlock()
	var err error
	sp.waitIfTooLarge(proc, shuffleIDX)
	sp.batches[shuffleIDX], err = sp.batches[shuffleIDX].AppendWithCopy(proc.Ctx, proc.Mp(), srcBatch)
	if err != nil {
		return err
	}
	sp.inputCNT[shuffleIDX] += int64(srcBatch.RowCount())
	if sp.batches[shuffleIDX].RowCount() >= colexec.DefaultBatchSize && len(sp.batchWaiters[shuffleIDX]) == 0 {
		sp.batchWaiters[shuffleIDX] <- true
	}
	return nil
}

func (sp *ShufflePoolV2) putBatchIntoShuffledPoolsBySels(srcBatch *batch.Batch, sels [][]int32, proc *process.Process) error {
	var err error
	for i := range sp.batches {
		currentSels := sels[i]
		if len(currentSels) > 0 {
			sp.batchLocks[i].Lock()
			sp.waitIfTooLarge(proc, int32(i))
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
			sp.inputCNT[i] += int64(len(currentSels))
			bat.AddRowCount(len(currentSels))
			if bat.RowCount() >= colexec.DefaultBatchSize && len(sp.batchWaiters[i]) == 0 {
				sp.batchWaiters[i] <- true
			}

			sp.batchLocks[i].Unlock()
		}
	}
	return nil
}

func (sp *ShufflePoolV2) statsDirectlySentBatch(srcBatch *batch.Batch) {
	sp.inputCNT[srcBatch.ShuffleIDX] += int64(srcBatch.RowCount())
	sp.outputCNT[srcBatch.ShuffleIDX] += int64(srcBatch.RowCount())
}

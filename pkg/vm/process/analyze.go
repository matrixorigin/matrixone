// Copyright 2021 Matrix Origin
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

package process

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"sync"
)

func init() {
	reuse.CreatePool[AnalyzeInfo](
		newAnalyzeInfo,
		resetAnalyzeInfo,
		reuse.DefaultOptions[AnalyzeInfo]().WithEnableChecker(),
	)
}

func (a AnalyzeInfo) TypeName() string {
	return "compile.anaylzeinfo"
}

func newAnalyzeInfo() *AnalyzeInfo {
	a := &AnalyzeInfo{}
	a.mu = &sync.Mutex{}
	return a
}

func (a *AnalyzeInfo) Reset() {
	nodeId := a.NodeId
	resetAnalyzeInfo(a)
	a.NodeId = nodeId
}

func resetAnalyzeInfo(a *AnalyzeInfo) {
	a.NodeId = 0
	a.InputBlocks = 0
	a.InputRows = 0
	a.OutputRows = 0
	a.TimeConsumed = 0
	a.WaitTimeConsumed = 0
	a.InputSize = 0
	a.OutputSize = 0
	a.MemorySize = 0
	a.DiskIO = 0
	a.S3IOByte = 0
	a.S3IOInputCount = 0
	a.S3IOOutputCount = 0
	a.NetworkIO = 0
	a.ScanTime = 0
	a.InsertTime = 0
	a.mu.Lock()
	defer a.mu.Unlock()
	a.TimeConsumedArrayMajor = a.TimeConsumedArrayMajor[:0]
	a.TimeConsumedArrayMinor = a.TimeConsumedArrayMinor[:0]
}

//func (a *operatorAnalyzer) Start() {
//	a.start = time.Now()
//}
//
//func (a *operatorAnalyzer) Stop() {
//	if a.analInfo != nil {
//		atomic.AddInt64(&a.analInfo.WaitTimeConsumed, int64(a.wait/time.Nanosecond))
//		consumeTime := int64((time.Since(a.start) - a.wait - a.childrenCallDuration) / time.Nanosecond)
//		atomic.AddInt64(&a.analInfo.TimeConsumed, consumeTime)
//		a.analInfo.AddSingleParallelTimeConsumed(a.parallelMajor, a.parallelIdx, consumeTime)
//	}
//}
//
//func (a *operatorAnalyzer) Alloc(size int64) {
//	if a.analInfo != nil {
//		atomic.AddInt64(&a.analInfo.MemorySize, size)
//	}
//}
//
//func (a *operatorAnalyzer) InputBlock() {
//	if a.analInfo != nil {
//		atomic.AddInt64(&a.analInfo.InputBlocks, 1)
//	}
//}
//
//func (a *operatorAnalyzer) Input(bat *batch.Batch, isFirst bool) {
//	if a.analInfo != nil && bat != nil && isFirst {
//		atomic.AddInt64(&a.analInfo.InputSize, int64(bat.Size()))
//		atomic.AddInt64(&a.analInfo.InputRows, int64(bat.RowCount()))
//	}
//}
//
//func (a *operatorAnalyzer) Output(bat *batch.Batch, isLast bool) {
//	if a.analInfo != nil && bat != nil && isLast {
//		atomic.AddInt64(&a.analInfo.OutputSize, int64(bat.Size()))
//		atomic.AddInt64(&a.analInfo.OutputRows, int64(bat.RowCount()))
//	}
//}
//
//func (a *operatorAnalyzer) WaitStop(start time.Time) {
//	a.wait += time.Since(start)
//}
//
//func (a *operatorAnalyzer) ChildrenCallStop(start time.Time) {
//	a.childrenCallDuration += time.Since(start)
//}
//
//func (a *operatorAnalyzer) DiskIO(bat *batch.Batch) {
//	if a.analInfo != nil && bat != nil {
//		atomic.AddInt64(&a.analInfo.DiskIO, int64(bat.Size()))
//	}
//}
//
//func (a *operatorAnalyzer) S3IOByte(bat *batch.Batch) {
//	if a.analInfo != nil && bat != nil {
//		atomic.AddInt64(&a.analInfo.S3IOByte, int64(bat.Size()))
//	}
//}
//
//func (a *operatorAnalyzer) S3IOInputCount(count int) {
//	if a.analInfo != nil {
//		atomic.AddInt64(&a.analInfo.S3IOInputCount, int64(count))
//	}
//}
//
//func (a *operatorAnalyzer) S3IOOutputCount(count int) {
//	if a.analInfo != nil {
//		atomic.AddInt64(&a.analInfo.S3IOOutputCount, int64(count))
//	}
//}
//
//func (a *operatorAnalyzer) Network(bat *batch.Batch) {
//	if a.analInfo != nil && bat != nil {
//		atomic.AddInt64(&a.analInfo.NetworkIO, int64(bat.Size()))
//	}
//}
//
//func (a *operatorAnalyzer) AddScanTime(t time.Time) {
//	if a.analInfo != nil {
//		atomic.AddInt64(&a.analInfo.ScanTime, int64(time.Since(t)))
//	}
//}
//
//func (a *operatorAnalyzer) AddInsertTime(t time.Time) {
//	if a.analInfo != nil {
//		atomic.AddInt64(&a.analInfo.InsertTime, int64(time.Since(t)))
//	}
//}

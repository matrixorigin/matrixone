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

package process

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// Analyze analyzes information for operator
type Analyzer interface {
	Start()
	Stop()
	ChildrenCallStop(time time.Time)
	Alloc(int64)
	Input(batch *batch.Batch)
	Output(*batch.Batch)
	WaitStop(time.Time)
	Network(*batch.Batch)
	AddScanTime(t time.Time)
	AddInsertTime(t time.Time)
	ServiceInvokeTime(t time.Time)
	GetOpStats() *OperatorStats
	Reset()

	InputBlock()
	DiskIO(*batch.Batch)   // delete it, unused
	S3IOByte(*batch.Batch) // delete it, unused
	S3IOInputCount(int)    // delete it, unused
	S3IOOutputCount(int)   // delete it, unused
}

// Operator Resource operatorAnalyzerV1
type operatorAnalyzerV1 struct {
	nodeIdx              int
	isFirst              bool
	isLast               bool
	start                time.Time
	wait                 time.Duration
	childrenCallDuration time.Duration
	opStats              *OperatorStats
}

var _ Analyzer = &operatorAnalyzerV1{}

func NewAnalyzer(idx int, isFirst bool, isLast bool, operatorName string) Analyzer {
	return &operatorAnalyzerV1{
		nodeIdx:              idx,
		isFirst:              isFirst,
		isLast:               isLast,
		wait:                 0,
		childrenCallDuration: 0,
		opStats:              NewOperatorStats(operatorName),
	}
}

func (opAlyzr *operatorAnalyzerV1) Reset() {
	opAlyzr.wait = 0
	opAlyzr.childrenCallDuration = 0
	opAlyzr.opStats.Reset()
}

func (opAlyzr *operatorAnalyzerV1) Start() {
	opAlyzr.start = time.Now()
	opAlyzr.wait = 0
	opAlyzr.childrenCallDuration = 0
}

func (opAlyzr *operatorAnalyzerV1) Stop() {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}

	// Calculate waiting time and total time consumption
	waitDuration := opAlyzr.wait
	totalDuration := time.Since(opAlyzr.start) - waitDuration - opAlyzr.childrenCallDuration

	// Check if the time consumption is legal
	if totalDuration < 0 {
		panic("Time consumed by the operator cannot be less than 0")
	}

	// Update the statistical information of the operation analyzer
	opAlyzr.opStats.TotalWaitTimeConsumed += waitDuration.Nanoseconds()
	opAlyzr.opStats.TotalTimeConsumed += totalDuration.Nanoseconds()
	opAlyzr.opStats.CallCount++
}

func (opAlyzr *operatorAnalyzerV1) Alloc(size int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}
	opAlyzr.opStats.TotalMemorySize += size
}

func (opAlyzr *operatorAnalyzerV1) InputBlock() {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}
	opAlyzr.opStats.TotalInputBlocks += 1
}

func (opAlyzr *operatorAnalyzerV1) Input(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}

	if bat != nil && opAlyzr.isFirst {
		opAlyzr.opStats.TotalInputSize += int64(bat.Size())
		opAlyzr.opStats.TotalInputRows += int64(bat.RowCount())
	}
}

func (opAlyzr *operatorAnalyzerV1) Output(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}

	if bat != nil && opAlyzr.isLast {
		opAlyzr.opStats.TotalOutputSize += int64(bat.Size())
		opAlyzr.opStats.TotalOutputRows += int64(bat.RowCount())
	}
}

func (opAlyzr *operatorAnalyzerV1) WaitStop(start time.Time) {
	opAlyzr.wait += time.Since(start)
}

func (opAlyzr *operatorAnalyzerV1) ChildrenCallStop(start time.Time) {
	opAlyzr.childrenCallDuration += time.Since(start)
}

func (opAlyzr *operatorAnalyzerV1) DiskIO(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.TotalDiskIO += int64(bat.Size())
	}
}

func (opAlyzr *operatorAnalyzerV1) S3IOByte(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.TotalS3IOByte += int64(bat.Size())
	}
}

func (opAlyzr *operatorAnalyzerV1) S3IOInputCount(count int) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}
	opAlyzr.opStats.TotalS3InputCount += int64(count)
}

func (opAlyzr *operatorAnalyzerV1) S3IOOutputCount(count int) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}
	opAlyzr.opStats.TotalS3OutputCount += int64(count)
}

func (opAlyzr *operatorAnalyzerV1) Network(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.TotalNetworkIO += int64(bat.Size())
	}
}

func (opAlyzr *operatorAnalyzerV1) AddScanTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}
	opAlyzr.opStats.TotalScanTime += int64(time.Since(t))
}

func (opAlyzr *operatorAnalyzerV1) AddInsertTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}
	opAlyzr.opStats.TotalInsertTime += int64(time.Since(t))
}

func (opAlyzr *operatorAnalyzerV1) ServiceInvokeTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}
	opAlyzr.opStats.TotalServiceTime += int64(time.Since(t))
}

func (opAlyzr *operatorAnalyzerV1) GetOpStats() *OperatorStats {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzerV1.Stop: operatorAnalyzerV1.opStats is nil")
	}
	return opAlyzr.opStats
}

type OperatorStats struct {
	OperatorName          string `json:"OperatorName"`
	CallCount             int    `json:"CallCount"`
	TotalTimeConsumed     int64  `json:"TotalTimeConsumed"`
	TotalWaitTimeConsumed int64  `json:"TotalWaitTimeConsumed"`
	TotalMemorySize       int64  `json:"TotalMemorySize"`
	TotalInputRows        int64  `json:"TotalInputRows"`
	TotalOutputRows       int64  `json:"TotalOutputRows"`
	TotalInputSize        int64  `json:"TotalInputSize"`
	TotalInputBlocks      int64  `json:"TotalInputBlocks"`
	TotalOutputSize       int64  `json:"TotalOutputSize"`
	TotalDiskIO           int64  `json:"TotalDiskIO"`
	TotalS3IOByte         int64  `json:"TotalS3IOByte"`
	TotalS3InputCount     int64  `json:"TotalS3InputCount"`
	TotalS3OutputCount    int64  `json:"TotalS3OutputCount"`
	TotalNetworkIO        int64  `json:"TotalNetworkIO"`
	TotalScanTime         int64  `json:"TotalScanTime"`
	TotalInsertTime       int64  `json:"TotalInsertTime"`
	TotalServiceTime      int64  `json:"TotalServiceTime"`
}

func NewOperatorStats(operatorName string) *OperatorStats {
	return &OperatorStats{
		OperatorName: operatorName,
	}
}

func (ps *OperatorStats) Reset() {
	ps.CallCount = 0
	ps.TotalTimeConsumed = 0
	ps.TotalWaitTimeConsumed = 0
	ps.TotalMemorySize = 0
	ps.TotalInputRows = 0
	ps.TotalOutputRows = 0
	ps.TotalInputSize = 0
	ps.TotalInputBlocks = 0
	ps.TotalOutputSize = 0
	ps.TotalDiskIO = 0
	ps.TotalS3IOByte = 0
	ps.TotalS3InputCount = 0
	ps.TotalS3OutputCount = 0
	ps.TotalNetworkIO = 0
	ps.TotalScanTime = 0
	ps.TotalInsertTime = 0
	ps.TotalServiceTime = 0
}

// Merge statistical information from another OperatorStats
func (ps *OperatorStats) Merge(other *OperatorStats) {
	ps.CallCount += other.CallCount
	ps.TotalTimeConsumed += other.TotalTimeConsumed
	ps.TotalWaitTimeConsumed += other.TotalWaitTimeConsumed
	ps.TotalMemorySize += other.TotalMemorySize
	ps.TotalInputRows += other.TotalInputRows
	ps.TotalOutputRows += other.TotalOutputRows
	ps.TotalInputSize += other.TotalInputSize
	ps.TotalInputBlocks += other.TotalInputBlocks
	ps.TotalOutputSize += other.TotalOutputSize
	ps.TotalDiskIO += other.TotalDiskIO
	ps.TotalS3IOByte += other.TotalS3IOByte
	ps.TotalS3InputCount += other.TotalS3InputCount
	ps.TotalS3OutputCount += other.TotalS3OutputCount
	ps.TotalNetworkIO += other.TotalNetworkIO
	ps.TotalScanTime += other.TotalScanTime
	ps.TotalInsertTime += other.TotalInsertTime
	ps.TotalServiceTime += other.TotalServiceTime
}

func (ps *OperatorStats) String() string {
	return fmt.Sprintf(" Call Count: %d, "+
		"TimeConsumed: %d ms, "+
		"WaitTimeConsumed: %d ms,"+
		"InputRows: %d, "+
		"OutputRows: %d, "+
		"InputSize: %d bytes, "+
		"InputBlocks: %d, "+
		"OutputSize: %d bytes, "+
		"MemorySize: %d bytes, "+
		"DiskIO: %d bytes, "+
		"S3IOByte: %d bytes, "+
		"S3InputCount: %d, "+
		"S3OutputCount: %d, "+
		"NetworkIO: %d bytes, "+
		"ScanTime: %d ms "+
		"ServiceTime: %d ms",
		ps.CallCount,
		ps.TotalTimeConsumed,
		ps.TotalWaitTimeConsumed,
		ps.TotalInputRows,
		ps.TotalOutputRows,
		ps.TotalInputSize,
		ps.TotalInputBlocks,
		ps.TotalOutputSize,
		ps.TotalMemorySize,
		ps.TotalDiskIO,
		ps.TotalS3IOByte,
		ps.TotalS3InputCount,
		ps.TotalS3OutputCount,
		ps.TotalNetworkIO,
		ps.TotalScanTime,
		ps.TotalServiceTime)
}

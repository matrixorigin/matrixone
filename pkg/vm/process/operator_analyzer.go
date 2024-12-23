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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

type MetricType int

const (
	OpScanTime      MetricType = 0
	OpInsertTime    MetricType = 1
	OpIncrementTime MetricType = 2
	OpWaitLockTime  MetricType = 3
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
	AddWrittenRows(int64)
	AddDeletedRows(int64)
	AddScanTime(t time.Time)
	AddInsertTime(t time.Time)
	AddIncrementTime(t time.Time)
	AddWaitLockTime(t time.Time)
	AddS3RequestCount(counter *perfcounter.CounterSet)
	AddFileServiceCacheInfo(counter *perfcounter.CounterSet)
	AddDiskIO(counter *perfcounter.CounterSet)

	GetOpCounterSet() *perfcounter.CounterSet
	GetOpStats() *OperatorStats

	InputBlock()
	ScanBytes(*batch.Batch)

	Reset()
}

// Operator Resource operatorAnalyzer
type operatorAnalyzer struct {
	nodeIdx              int
	isFirst              bool
	isLast               bool
	start                time.Time
	wait                 time.Duration
	childrenCallDuration time.Duration
	crs                  *perfcounter.CounterSet
	opStats              *OperatorStats
}

var _ Analyzer = &operatorAnalyzer{}

// NewAnalyzer is used to provide resource statistics services for physical plann operators
func NewAnalyzer(idx int, isFirst bool, isLast bool, operatorName string) Analyzer {
	return &operatorAnalyzer{
		nodeIdx:              idx,
		isFirst:              isFirst,
		isLast:               isLast,
		wait:                 0,
		childrenCallDuration: 0,
		crs:                  new(perfcounter.CounterSet),
		opStats:              NewOperatorStats(operatorName),
	}
}

// NewTempAnalyzer is used to provide resource statistics services for non operator logic
func NewTempAnalyzer() Analyzer {
	return &operatorAnalyzer{
		wait:    0,
		crs:     new(perfcounter.CounterSet),
		opStats: NewOperatorStats("temp Analyzer"),
	}
}

// GetOpCounterSet returns the current CounterSet and resets it.
// This method should be used when you want to start fresh with the performance counters.
func (opAlyzr *operatorAnalyzer) GetOpCounterSet() *perfcounter.CounterSet {
	opAlyzr.crs.Reset()
	return opAlyzr.crs
}

func (opAlyzr *operatorAnalyzer) Reset() {
	opAlyzr.wait = 0
	opAlyzr.childrenCallDuration = 0
	opAlyzr.crs.Reset()
	opAlyzr.opStats.Reset()
}

func (opAlyzr *operatorAnalyzer) Start() {
	opAlyzr.start = time.Now()
	opAlyzr.wait = 0
	opAlyzr.childrenCallDuration = 0
}

func (opAlyzr *operatorAnalyzer) Stop() {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Stop: operatorAnalyzer.opStats is nil")
	}

	// Calculate waiting time and total time consumption
	waitDuration := opAlyzr.wait

	opDuration := time.Since(opAlyzr.start)
	totalDuration := opDuration - waitDuration - opAlyzr.childrenCallDuration

	// Check if the time consumption is legal
	if totalDuration < 0 {
		str := fmt.Sprintf("opName:%s, opDuration: %v, waitDuration:%v, childrenCallDuration:%v , start:%v, end:%v\n",
			opAlyzr.opStats.OperatorName,
			opDuration,
			waitDuration,
			opAlyzr.childrenCallDuration,
			opAlyzr.start,
			time.Now())
		panic("Time consumed by the operator cannot be less than 0, " + str)
	}

	// Update the statistical information of the operation analyzer
	opAlyzr.opStats.WaitTimeConsumed += waitDuration.Nanoseconds()
	opAlyzr.opStats.TimeConsumed += totalDuration.Nanoseconds()
	opAlyzr.opStats.CallNum++
}

func (opAlyzr *operatorAnalyzer) Alloc(size int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Alloc: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.MemorySize += size
}

func (opAlyzr *operatorAnalyzer) InputBlock() {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.InputBlock: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.InputBlocks += 1
}

// If the operator input batch is First, then the InputSize and InputRows will be counted
func (opAlyzr *operatorAnalyzer) Input(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Input: operatorAnalyzer.opStats is nil")
	}

	if bat != nil && opAlyzr.isFirst {
		opAlyzr.opStats.InputSize += int64(bat.Size())
		opAlyzr.opStats.InputRows += int64(bat.RowCount())
	}
}

// If the operator input batch is Last, then the OutputSize and OutputRows will be counted
func (opAlyzr *operatorAnalyzer) Output(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Output: operatorAnalyzer.opStats is nil")
	}

	if bat != nil && opAlyzr.isLast {
		opAlyzr.opStats.OutputSize += int64(bat.Size())
		opAlyzr.opStats.OutputRows += int64(bat.RowCount())
	}
}

func (opAlyzr *operatorAnalyzer) WaitStop(start time.Time) {
	opAlyzr.wait += time.Since(start)
}

func (opAlyzr *operatorAnalyzer) ChildrenCallStop(start time.Time) {
	opAlyzr.childrenCallDuration += time.Since(start)
}

func (opAlyzr *operatorAnalyzer) ScanBytes(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.S3IOByte: operatorAnalyzer.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.ScanBytes += int64(bat.Size())
	}
}

func (opAlyzr *operatorAnalyzer) Network(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Network: operatorAnalyzer.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.NetworkIO += int64(bat.Size())
	}
}

func (opAlyzr *operatorAnalyzer) AddWrittenRows(rowCount int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.WrittenRows: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.WrittenRows += rowCount
}

func (opAlyzr *operatorAnalyzer) AddDeletedRows(rowCount int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddDeletedRows: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.DeletedRows += rowCount
}

func (opAlyzr *operatorAnalyzer) AddScanTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddScanTime: operatorAnalyzer.opStats is nil")
	}
	duration := time.Since(t)
	opAlyzr.opStats.AddOpMetric(OpScanTime, duration.Nanoseconds())
}

func (opAlyzr *operatorAnalyzer) AddInsertTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddInsertTime: operatorAnalyzer.opStats is nil")
	}
	duration := time.Since(t)
	opAlyzr.opStats.AddOpMetric(OpInsertTime, duration.Nanoseconds())
}

func (opAlyzr *operatorAnalyzer) AddIncrementTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddIncrementTime: operatorAnalyzer.opStats is nil")
	}
	duration := time.Since(t)
	opAlyzr.opStats.AddOpMetric(OpIncrementTime, duration.Nanoseconds())
}

func (opAlyzr *operatorAnalyzer) AddWaitLockTime(t time.Time) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddWaitLockTime: operatorAnalyzer.opStats is nil")
	}
	duration := time.Since(t)
	opAlyzr.opStats.AddOpMetric(OpWaitLockTime, duration.Nanoseconds())
}

func (opAlyzr *operatorAnalyzer) AddS3RequestCount(counter *perfcounter.CounterSet) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddS3RequestCount: operatorAnalyzer.opStats is nil")
	}

	opAlyzr.opStats.S3List += counter.FileService.S3.List.Load()
	opAlyzr.opStats.S3Head += counter.FileService.S3.Head.Load()
	opAlyzr.opStats.S3Put += counter.FileService.S3.Put.Load()
	opAlyzr.opStats.S3Get += counter.FileService.S3.Get.Load()
	opAlyzr.opStats.S3Delete += counter.FileService.S3.Delete.Load()
	opAlyzr.opStats.S3DeleteMul += counter.FileService.S3.DeleteMulti.Load()
}

func (opAlyzr *operatorAnalyzer) AddFileServiceCacheInfo(counter *perfcounter.CounterSet) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddFileServiceCacheInfo: operatorAnalyzer.opStats is nil")
	}

	opAlyzr.opStats.CacheRead += counter.FileService.Cache.Read.Load()
	opAlyzr.opStats.CacheHit += counter.FileService.Cache.Hit.Load()
	opAlyzr.opStats.CacheMemoryRead += counter.FileService.Cache.Memory.Read.Load()
	opAlyzr.opStats.CacheMemoryHit += counter.FileService.Cache.Memory.Hit.Load()
	opAlyzr.opStats.CacheDiskRead += counter.FileService.Cache.Disk.Read.Load()
	opAlyzr.opStats.CacheDiskHit += counter.FileService.Cache.Disk.Hit.Load()
	opAlyzr.opStats.CacheRemoteRead += counter.FileService.Cache.Remote.Read.Load()
	opAlyzr.opStats.CacheRemoteHit += counter.FileService.Cache.Remote.Hit.Load()
}

func (opAlyzr *operatorAnalyzer) AddDiskIO(counter *perfcounter.CounterSet) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddDiskIO: operatorAnalyzer.opStats is nil")
	}

	opAlyzr.opStats.DiskIO += counter.FileService.FileWithChecksum.Read.Load()
	opAlyzr.opStats.DiskIO += counter.FileService.FileWithChecksum.Write.Load()
}

func (opAlyzr *operatorAnalyzer) GetOpStats() *OperatorStats {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.GetOpStats(): operatorAnalyzer.opStats is nil")
	}
	return opAlyzr.opStats
}

type OperatorStats struct {
	OperatorName     string `json:"-"`
	CallNum          int    `json:"CallCount,omitempty"`
	TimeConsumed     int64  `json:"TimeConsumed,omitempty"`
	WaitTimeConsumed int64  `json:"WaitTimeConsumed,omitempty"`
	MemorySize       int64  `json:"MemorySize,omitempty"`
	InputRows        int64  `json:"InputRows,omitempty"`
	InputSize        int64  `json:"InputSize,omitempty"`
	OutputRows       int64  `json:"OutputRows,omitempty"`
	OutputSize       int64  `json:"OutputSize,omitempty"`
	NetworkIO        int64  `json:"NetworkIO,omitempty"`
	DiskIO           int64  `json:"DiskIO,omitempty"`

	InputBlocks int64 `json:"-"`
	ScanBytes   int64 `json:"-"`
	WrittenRows int64 `json:"WrittenRows,omitempty"` // WrittenRows Used to estimate S3input
	DeletedRows int64 `json:"DeletedRows,omitempty"` // DeletedRows Used to estimate S3input

	S3List      int64 `json:"S3List,omitempty"`
	S3Head      int64 `json:"S3Head,omitempty"`
	S3Put       int64 `json:"S3Put,omitempty"`
	S3Get       int64 `json:"S3Get,omitempty"`
	S3Delete    int64 `json:"S3Delete,omitempty"`
	S3DeleteMul int64 `json:"S3DeleteMul,omitempty"`

	CacheRead       int64 `json:"CacheRead,omitempty"`
	CacheHit        int64 `json:"CacheHit,omitempty"`
	CacheMemoryRead int64 `json:"CacheMemoryRead,omitempty"`
	CacheMemoryHit  int64 `json:"CacheMemoryHit,omitempty"`
	CacheDiskRead   int64 `json:"CacheDiskRead,omitempty"`
	CacheDiskHit    int64 `json:"CacheDiskHit,omitempty"`
	CacheRemoteRead int64 `json:"CacheRemoteRead,omitempty"`
	CacheRemoteHit  int64 `json:"CacheRemoteHit,omitempty"`

	OperatorMetrics map[MetricType]int64 `json:"OperatorMetrics,omitempty"`
}

func NewOperatorStats(operatorName string) *OperatorStats {
	return &OperatorStats{
		OperatorName: operatorName,
	}
}

func (ps *OperatorStats) AddOpMetric(key MetricType, value int64) {
	if ps.OperatorMetrics == nil {
		ps.OperatorMetrics = make(map[MetricType]int64)
	}
	ps.OperatorMetrics[key] += value
}

func (ps *OperatorStats) GetMetricByKey(metricType MetricType) int64 {
	if ps.OperatorMetrics == nil {
		return 0
	}
	return ps.OperatorMetrics[metricType]
}

func (ps *OperatorStats) Reset() {
	*ps = OperatorStats{}
}

func (ps *OperatorStats) String() string {
	// Use strings.Builder for efficient string concatenation
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(" CallNum:%d "+
		"TimeCost:%dns "+
		"WaitTime:%dns "+
		"InRows:%d "+
		"OutRows:%d "+
		"InSize:%dbytes "+
		"InBlock:%d "+
		"OutSize:%dbytes "+
		"MemSize:%dbytes "+
		"ScanBytes:%dbytes "+
		"NetworkIO:%dbytes "+
		"DiskIO:%dbytes ",
		ps.CallNum,
		ps.TimeConsumed,
		ps.WaitTimeConsumed,
		ps.InputRows,
		ps.OutputRows,
		ps.InputSize,
		ps.InputBlocks,
		ps.OutputSize,
		ps.MemorySize,
		ps.ScanBytes,
		ps.NetworkIO,
		ps.DiskIO))

	// Collect S3 stats in a slice for efficient concatenation
	dynamicAttrs := []string{}
	if ps.WrittenRows > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("WrittenRows:%d ", ps.WrittenRows))
	}
	if ps.DeletedRows > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("DeletedRows:%d ", ps.DeletedRows))
	}
	if ps.S3List > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("S3List:%d ", ps.S3List))
	}
	if ps.S3Head > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("S3Head:%d ", ps.S3Head))
	}
	if ps.S3Put > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("S3Put:%d ", ps.S3Put))
	}
	if ps.S3Get > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("S3Get:%d ", ps.S3Get))
	}
	if ps.S3Delete > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("S3Delete:%d ", ps.S3Delete))
	}
	if ps.S3DeleteMul > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("S3DeleteMul:%d ", ps.S3DeleteMul))
	}
	//---------------------------------------------------------------------------------------------
	if ps.CacheRead > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("CacheRead:%d ", ps.CacheRead))
	}
	if ps.CacheHit > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("CacheHit:%d ", ps.CacheHit))
	}
	if ps.CacheMemoryRead > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("CacheMemoryRead:%d ", ps.CacheMemoryRead))
	}
	if ps.CacheMemoryHit > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("CacheMemoryHit:%d ", ps.CacheMemoryHit))
	}
	if ps.CacheDiskRead > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("CacheDiskRead:%d ", ps.CacheDiskRead))
	}
	if ps.CacheDiskHit > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("CacheDiskHit:%d ", ps.CacheDiskHit))
	}
	if ps.CacheRemoteRead > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("CacheRemoteRead:%d ", ps.CacheRemoteRead))
	}
	if ps.CacheRemoteHit > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("CacheRemoteHit:%d ", ps.CacheRemoteHit))
	}

	// Join and append S3 stats if any
	if len(dynamicAttrs) > 0 {
		sb.WriteString(strings.Join(dynamicAttrs, ""))
	}

	// Convert OperationMetrics map to a formatted string
	var metricsStr string
	if len(ps.OperatorMetrics) > 0 {
		for k, v := range ps.OperatorMetrics {
			metricName := "Unknown"
			switch k {
			case OpScanTime:
				metricName = "ScanTime"
			case OpInsertTime:
				metricName = "InsertTime"
			case OpIncrementTime:
				metricName = "IncrementTime"
			case OpWaitLockTime:
				metricName = "WaitLockTime"
			}
			metricsStr += fmt.Sprintf("%s:%dns ", metricName, v)
		}
	}

	sb.WriteString(metricsStr)
	return sb.String()
}

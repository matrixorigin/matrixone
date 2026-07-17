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
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
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
	SetMemUsed(int64)
	Spill(int64)
	SpillRows(int64)
	Input(batch *batch.Batch)
	Output(*batch.Batch)
	WaitStop(time.Time)
	WaitStopKind(time.Time, resource.WaitKind)
	Network(*batch.Batch)
	AddWrittenRows(int64)
	AddDeletedRows(int64)
	AddScanTime(t time.Time)
	AddInsertTime(t time.Time)
	AddIncrementTime(t time.Time)
	AddWaitLockTime(t time.Time)
	AddParquetProfile(stats ParquetProfileStats)

	GetOpCounterSet() *perfcounter.CounterSet
	GetOpStats() *OperatorStats

	InputBlock()
	ScanBytes(*batch.Batch)

	Reset()
}

type ParquetProfileStats struct {
	Files                             int64
	RowGroups                         int64
	RowsRead                          int64
	BytesRead                         int64
	PrefetchBytes                     int64
	ProjectedColumns                  int64
	TotalColumns                      int64
	SelectedFiles                     int64
	SelectedFileBytes                 int64
	IcebergMetadataBytes              int64
	IcebergManifestListBytes          int64
	IcebergManifestBytes              int64
	IcebergManifestsSelected          int64
	IcebergManifestsPruned            int64
	IcebergDataFilesSelected          int64
	IcebergDataFilesPruned            int64
	IcebergDataFileBytesSelected      int64
	IcebergDataFileBytesPruned        int64
	IcebergPlanningCacheHits          int64
	IcebergPlanningCacheMiss          int64
	IcebergDeleteFilesRead            int64
	IcebergDeleteRowsFiltered         int64
	IcebergPositionDeleteRowsFiltered int64
	IcebergEqualityDeleteRowsFiltered int64
	IcebergDeleteApplyPeakMemoryBytes int64
	OpenTime                          int64
	ReadPageTime                      int64
	MapTime                           int64
	RowModeTime                       int64
	PeakBatchBytes                    int64
}

// MeasureWait records exactly one synchronous blocking boundary, including its
// error and panic exits. Callers must keep CPU work outside fn.
func MeasureWait[T any](analyzer Analyzer, kind resource.WaitKind, fn func() (T, error)) (T, error) {
	start := time.Now()
	defer analyzer.WaitStopKind(start, kind)
	return fn()
}

// MeasureFilesystemWait records the complete duration of a storage boundary,
// including error and panic exits.
func MeasureFilesystemWait[T any](analyzer Analyzer, fn func() (T, error)) (T, error) {
	return MeasureWait(analyzer, resource.WaitFilesystem, fn)
}

// MeasureFilesystemWaitErr is the error-only form of MeasureFilesystemWait.
func MeasureFilesystemWaitErr(analyzer Analyzer, fn func() error) error {
	_, err := MeasureFilesystemWait(analyzer, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

// MeasureOutputWaitErr records a protocol/output callback boundary.
func MeasureOutputWaitErr(analyzer Analyzer, fn func() error) error {
	start := time.Now()
	defer analyzer.WaitStopKind(start, resource.WaitOutput)
	return fn()
}

func (stats ParquetProfileStats) Empty() bool {
	return stats == ParquetProfileStats{}
}

func (stats *ParquetProfileStats) Add(delta ParquetProfileStats) {
	stats.Files += delta.Files
	stats.RowGroups += delta.RowGroups
	stats.RowsRead += delta.RowsRead
	stats.BytesRead += delta.BytesRead
	stats.PrefetchBytes += delta.PrefetchBytes
	stats.ProjectedColumns += delta.ProjectedColumns
	stats.TotalColumns += delta.TotalColumns
	stats.SelectedFiles += delta.SelectedFiles
	stats.SelectedFileBytes += delta.SelectedFileBytes
	stats.IcebergMetadataBytes += delta.IcebergMetadataBytes
	stats.IcebergManifestListBytes += delta.IcebergManifestListBytes
	stats.IcebergManifestBytes += delta.IcebergManifestBytes
	stats.IcebergManifestsSelected += delta.IcebergManifestsSelected
	stats.IcebergManifestsPruned += delta.IcebergManifestsPruned
	stats.IcebergDataFilesSelected += delta.IcebergDataFilesSelected
	stats.IcebergDataFilesPruned += delta.IcebergDataFilesPruned
	stats.IcebergDataFileBytesSelected += delta.IcebergDataFileBytesSelected
	stats.IcebergDataFileBytesPruned += delta.IcebergDataFileBytesPruned
	stats.IcebergPlanningCacheHits += delta.IcebergPlanningCacheHits
	stats.IcebergPlanningCacheMiss += delta.IcebergPlanningCacheMiss
	stats.IcebergDeleteFilesRead += delta.IcebergDeleteFilesRead
	stats.IcebergDeleteRowsFiltered += delta.IcebergDeleteRowsFiltered
	stats.IcebergPositionDeleteRowsFiltered += delta.IcebergPositionDeleteRowsFiltered
	stats.IcebergEqualityDeleteRowsFiltered += delta.IcebergEqualityDeleteRowsFiltered
	stats.IcebergDeleteApplyPeakMemoryBytes = max(stats.IcebergDeleteApplyPeakMemoryBytes, delta.IcebergDeleteApplyPeakMemoryBytes)
	stats.OpenTime += delta.OpenTime
	stats.ReadPageTime += delta.ReadPageTime
	stats.MapTime += delta.MapTime
	stats.RowModeTime += delta.RowModeTime
	stats.PeakBatchBytes = max(stats.PeakBatchBytes, delta.PeakBatchBytes)
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
	crsActive            bool
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

// GetOpCounterSet starts a new producer-owned counter interval. Any previous
// interval is harvested first, so callers cannot lose a partial operation by
// returning on its error path.
func (opAlyzr *operatorAnalyzer) GetOpCounterSet() *perfcounter.CounterSet {
	opAlyzr.harvestCounterSet()
	opAlyzr.crs.Reset()
	opAlyzr.crsActive = true
	return opAlyzr.crs
}

func (opAlyzr *operatorAnalyzer) Reset() {
	opAlyzr.wait = 0
	opAlyzr.childrenCallDuration = 0
	opAlyzr.crs.Reset()
	opAlyzr.crsActive = false
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

	// The counter interval is terminal even when the producer returned an error
	// or panicked. Harvest it before publishing the Call statistics.
	opAlyzr.harvestCounterSet()

	// Calculate producer-local exclusive active time. Child calls and waits are
	// subtracted only from the Call interval that observed them.
	waitDuration := opAlyzr.wait
	opDuration := time.Since(opAlyzr.start)
	if opDuration < 0 || waitDuration < 0 || opAlyzr.childrenCallDuration < 0 {
		opAlyzr.opStats.ResourceQuality |= resource.QualityInvariantFailure
	} else {
		active, flags := resource.ExclusiveActive(
			uint64(opDuration.Nanoseconds()),
			uint64(waitDuration.Nanoseconds()),
			uint64(opAlyzr.childrenCallDuration.Nanoseconds()),
		)
		opAlyzr.opStats.ResourceQuality |= flags
		opAlyzr.opStats.TimeConsumed = addResourceInt64(
			opAlyzr.opStats.TimeConsumed, active, &opAlyzr.opStats.ResourceQuality)
	}

	// Update the statistical information of the operation analyzer
	if waitDuration >= 0 {
		opAlyzr.opStats.WaitTimeConsumed = addResourceInt64(
			opAlyzr.opStats.WaitTimeConsumed,
			uint64(waitDuration.Nanoseconds()),
			&opAlyzr.opStats.ResourceQuality,
		)
	}
	opAlyzr.opStats.CallNum++
}

func addResourceInt64(current int64, delta uint64, flags *resource.QualityFlags) int64 {
	if current < 0 || delta > math.MaxInt64 || current > math.MaxInt64-int64(delta) {
		*flags |= resource.QualityInvariantFailure
		return math.MaxInt64
	}
	return current + int64(delta)
}

func (opAlyzr *operatorAnalyzer) Alloc(size int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Alloc: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.MemorySize += size
}

func (opAlyzr *operatorAnalyzer) SetMemUsed(size int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.SetMemUsed: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.MemorySize = max(opAlyzr.opStats.MemorySize, size)
}

func (opAlyzr *operatorAnalyzer) Spill(size int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddSpillSize: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.SpillSize += size
}

func (opAlyzr *operatorAnalyzer) SpillRows(rows int64) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.SpillRows: operatorAnalyzer.opStats is nil")
	}
	opAlyzr.opStats.SpillRows += rows
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

	if bat != nil {
		opAlyzr.opStats.InputSize += int64(bat.Size())
		opAlyzr.opStats.InputRows += int64(bat.RowCount())
	}
}

// If the operator input batch is Last, then the OutputSize and OutputRows will be counted
func (opAlyzr *operatorAnalyzer) Output(bat *batch.Batch) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.Output: operatorAnalyzer.opStats is nil")
	}

	if bat != nil {
		opAlyzr.opStats.OutputSize += int64(bat.Size())
		opAlyzr.opStats.OutputRows += int64(bat.RowCount())
	}
}

func (opAlyzr *operatorAnalyzer) WaitStop(start time.Time) {
	opAlyzr.WaitStopKind(start, resource.WaitOther)
}

func (opAlyzr *operatorAnalyzer) WaitStopKind(start time.Time, kind resource.WaitKind) {
	duration := time.Since(start)
	if duration < 0 || kind >= resource.WaitKindCount {
		opAlyzr.opStats.ResourceQuality |= resource.QualityInvariantFailure
		return
	}
	opAlyzr.wait += duration
	opAlyzr.opStats.ResourceWaitNS[kind] = addResourceInt64(
		opAlyzr.opStats.ResourceWaitNS[kind],
		uint64(duration.Nanoseconds()),
		&opAlyzr.opStats.ResourceQuality,
	)
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

func (opAlyzr *operatorAnalyzer) harvestCounterSet() {
	if !opAlyzr.crsActive {
		return
	}
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.harvestCounterSet: operatorAnalyzer.opStats is nil")
	}
	counter := opAlyzr.crs
	opAlyzr.opStats.S3List += counter.FileService.S3.List.Load()
	opAlyzr.opStats.S3Head += counter.FileService.S3.Head.Load()
	opAlyzr.opStats.S3Put += counter.FileService.S3.Put.Load()
	opAlyzr.opStats.S3Get += counter.FileService.S3.Get.Load()
	opAlyzr.opStats.S3Delete += counter.FileService.S3.Delete.Load()
	opAlyzr.opStats.S3DeleteMul += counter.FileService.S3.DeleteMulti.Load()
	opAlyzr.opStats.CacheRead += counter.FileService.Cache.Read.Load()
	opAlyzr.opStats.CacheHit += counter.FileService.Cache.Hit.Load()
	opAlyzr.opStats.CacheMemoryRead += counter.FileService.Cache.Memory.Read.Load()
	opAlyzr.opStats.CacheMemoryHit += counter.FileService.Cache.Memory.Hit.Load()
	opAlyzr.opStats.CacheDiskRead += counter.FileService.Cache.Disk.Read.Load()
	opAlyzr.opStats.CacheDiskHit += counter.FileService.Cache.Disk.Hit.Load()
	opAlyzr.opStats.CacheRemoteRead += counter.FileService.Cache.Remote.Read.Load()
	opAlyzr.opStats.CacheRemoteHit += counter.FileService.Cache.Remote.Hit.Load()
	opAlyzr.opStats.ReadSize += counter.FileService.ReadSize.Load()
	opAlyzr.opStats.S3ReadSize += counter.FileService.S3ReadSize.Load()
	opAlyzr.opStats.S3WriteSize += counter.FileService.S3WriteSize.Load()
	opAlyzr.opStats.DiskReadSize += counter.FileService.DiskReadSize.Load()
	counter.Reset()
	opAlyzr.crsActive = false
}

func (opAlyzr *operatorAnalyzer) AddParquetProfile(stats ParquetProfileStats) {
	if opAlyzr.opStats == nil {
		panic("operatorAnalyzer.AddParquetProfile: operatorAnalyzer.opStats is nil")
	}

	opAlyzr.opStats.ParquetFiles += stats.Files
	opAlyzr.opStats.ParquetRowGroups += stats.RowGroups
	opAlyzr.opStats.ParquetRowsRead += stats.RowsRead
	opAlyzr.opStats.ParquetBytesRead += stats.BytesRead
	opAlyzr.opStats.ParquetPrefetchBytes += stats.PrefetchBytes
	opAlyzr.opStats.ParquetProjectedColumns += stats.ProjectedColumns
	opAlyzr.opStats.ParquetTotalColumns += stats.TotalColumns
	opAlyzr.opStats.ParquetSelectedFiles += stats.SelectedFiles
	opAlyzr.opStats.ParquetSelectedFileBytes += stats.SelectedFileBytes
	opAlyzr.opStats.IcebergMetadataBytes += stats.IcebergMetadataBytes
	opAlyzr.opStats.IcebergManifestListBytes += stats.IcebergManifestListBytes
	opAlyzr.opStats.IcebergManifestBytes += stats.IcebergManifestBytes
	opAlyzr.opStats.IcebergManifestsSelected += stats.IcebergManifestsSelected
	opAlyzr.opStats.IcebergManifestsPruned += stats.IcebergManifestsPruned
	opAlyzr.opStats.IcebergDataFilesSelected += stats.IcebergDataFilesSelected
	opAlyzr.opStats.IcebergDataFilesPruned += stats.IcebergDataFilesPruned
	opAlyzr.opStats.IcebergDataFileBytesSelected += stats.IcebergDataFileBytesSelected
	opAlyzr.opStats.IcebergDataFileBytesPruned += stats.IcebergDataFileBytesPruned
	opAlyzr.opStats.IcebergPlanningCacheHits += stats.IcebergPlanningCacheHits
	opAlyzr.opStats.IcebergPlanningCacheMiss += stats.IcebergPlanningCacheMiss
	opAlyzr.opStats.IcebergDeleteFilesRead += stats.IcebergDeleteFilesRead
	opAlyzr.opStats.IcebergDeleteRowsFiltered += stats.IcebergDeleteRowsFiltered
	opAlyzr.opStats.IcebergPositionDeleteRowsFiltered += stats.IcebergPositionDeleteRowsFiltered
	opAlyzr.opStats.IcebergEqualityDeleteRowsFiltered += stats.IcebergEqualityDeleteRowsFiltered
	opAlyzr.opStats.IcebergDeleteApplyPeakMemoryBytes = max(opAlyzr.opStats.IcebergDeleteApplyPeakMemoryBytes, stats.IcebergDeleteApplyPeakMemoryBytes)
	opAlyzr.opStats.ParquetOpenTime += stats.OpenTime
	opAlyzr.opStats.ParquetReadPageTime += stats.ReadPageTime
	opAlyzr.opStats.ParquetMapTime += stats.MapTime
	opAlyzr.opStats.ParquetRowModeTime += stats.RowModeTime
	opAlyzr.opStats.ParquetPeakBatchBytes = max(opAlyzr.opStats.ParquetPeakBatchBytes, stats.PeakBatchBytes)
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
	// MemorySize remains an internal producer-local observation. Exact memory
	// accounting is owned by the statement resource summary and must not be
	// serialized as operator explain data.
	MemorySize      int64                         `json:"-"`
	SpillSize       int64                         `json:"SpillSize,omitempty"`
	SpillRows       int64                         `json:"SpillRows,omitempty"`
	InputRows       int64                         `json:"InputRows,omitempty"`
	InputSize       int64                         `json:"InputSize,omitempty"`
	OutputRows      int64                         `json:"OutputRows,omitempty"`
	OutputSize      int64                         `json:"OutputSize,omitempty"`
	NetworkIO       int64                         `json:"NetworkIO,omitempty"`
	DiskIO          int64                         `json:"DiskIO,omitempty"`
	ResourceQuality resource.QualityFlags         `json:"-"`
	ResourceWaitNS  [resource.WaitKindCount]int64 `json:"-"`

	InputBlocks  int64 `json:"-"`
	ScanBytes    int64 `json:"-"`
	ReadSize     int64 `json:"ReadSize,omitempty"`     // ReadSize: actual bytes read from storage layer (excluding rowid tombstone)
	S3ReadSize   int64 `json:"S3ReadSize,omitempty"`   // S3ReadSize: actual bytes read from S3 (excluding rowid tombstone)
	S3WriteSize  int64 `json:"S3WriteSize,omitempty"`  // S3WriteSize: actual bytes accepted by object storage
	DiskReadSize int64 `json:"DiskReadSize,omitempty"` // DiskReadSize: actual bytes read from disk cache (excluding rowid tombstone)
	WrittenRows  int64 `json:"WrittenRows,omitempty"`  // WrittenRows Used to estimate S3input
	DeletedRows  int64 `json:"DeletedRows,omitempty"`  // DeletedRows Used to estimate S3input

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

	ParquetFiles                      int64 `json:"ParquetFiles,omitempty"`
	ParquetRowGroups                  int64 `json:"ParquetRowGroups,omitempty"`
	ParquetRowsRead                   int64 `json:"ParquetRowsRead,omitempty"`
	ParquetBytesRead                  int64 `json:"ParquetBytesRead,omitempty"`
	ParquetPrefetchBytes              int64 `json:"ParquetPrefetchBytes,omitempty"`
	ParquetProjectedColumns           int64 `json:"ParquetProjectedColumns,omitempty"`
	ParquetTotalColumns               int64 `json:"ParquetTotalColumns,omitempty"`
	ParquetSelectedFiles              int64 `json:"ParquetSelectedFiles,omitempty"`
	ParquetSelectedFileBytes          int64 `json:"ParquetSelectedFileBytes,omitempty"`
	IcebergMetadataBytes              int64 `json:"IcebergMetadataBytes,omitempty"`
	IcebergManifestListBytes          int64 `json:"IcebergManifestListBytes,omitempty"`
	IcebergManifestBytes              int64 `json:"IcebergManifestBytes,omitempty"`
	IcebergManifestsSelected          int64 `json:"IcebergManifestsSelected,omitempty"`
	IcebergManifestsPruned            int64 `json:"IcebergManifestsPruned,omitempty"`
	IcebergDataFilesSelected          int64 `json:"IcebergDataFilesSelected,omitempty"`
	IcebergDataFilesPruned            int64 `json:"IcebergDataFilesPruned,omitempty"`
	IcebergDataFileBytesSelected      int64 `json:"IcebergDataFileBytesSelected,omitempty"`
	IcebergDataFileBytesPruned        int64 `json:"IcebergDataFileBytesPruned,omitempty"`
	IcebergPlanningCacheHits          int64 `json:"IcebergPlanningCacheHits,omitempty"`
	IcebergPlanningCacheMiss          int64 `json:"IcebergPlanningCacheMiss,omitempty"`
	IcebergDeleteFilesRead            int64 `json:"IcebergDeleteFilesRead,omitempty"`
	IcebergDeleteRowsFiltered         int64 `json:"IcebergDeleteRowsFiltered,omitempty"`
	IcebergPositionDeleteRowsFiltered int64 `json:"IcebergPositionDeleteRowsFiltered,omitempty"`
	IcebergEqualityDeleteRowsFiltered int64 `json:"IcebergEqualityDeleteRowsFiltered,omitempty"`
	IcebergDeleteApplyPeakMemoryBytes int64 `json:"IcebergDeleteApplyPeakMemoryBytes,omitempty"`
	ParquetOpenTime                   int64 `json:"ParquetOpenTime,omitempty"`
	ParquetReadPageTime               int64 `json:"ParquetReadPageTime,omitempty"`
	ParquetMapTime                    int64 `json:"ParquetMapTime,omitempty"`
	ParquetRowModeTime                int64 `json:"ParquetRowModeTime,omitempty"`
	ParquetPeakBatchBytes             int64 `json:"ParquetPeakBatchBytes,omitempty"`

	OperatorMetrics map[MetricType]int64 `json:"OperatorMetrics,omitempty"`

	BackgroundQueries []*plan.Query `json:"BackgroundQueries,omitempty"`
}

// ResourceDelta returns the producer facts owned by this analyzer. Operator
// memory is deliberately excluded; exact memory comes from the isolated MPool
// domain.
func (ps *OperatorStats) ResourceDelta() resource.Delta {
	var delta resource.Delta
	delta.Quality = ps.ResourceQuality
	add := func(value int64, target *uint64) {
		if value < 0 {
			delta.Quality |= resource.QualityInvariantFailure
			return
		}
		*target = uint64(value)
	}
	add(ps.TimeConsumed, &delta.Usage.ExclusiveActiveNS)
	var classifiedWait int64
	for kind := resource.WaitKind(0); kind < resource.WaitKindCount; kind++ {
		value := ps.ResourceWaitNS[kind]
		if value < 0 || classifiedWait > math.MaxInt64-value {
			delta.Quality |= resource.QualityInvariantFailure
			continue
		}
		classifiedWait += value
		add(value, &delta.Usage.WaitNS[kind])
	}
	if classifiedWait != ps.WaitTimeConsumed {
		delta.Quality |= resource.QualityInvariantFailure
	}
	add(ps.SpillSize, &delta.Usage.SpillBytes)
	add(ps.S3ReadSize, &delta.Usage.S3ReadBytes)
	add(ps.S3WriteSize, &delta.Usage.S3WriteBytes)
	add(ps.S3Head, &delta.Usage.S3Requests[resource.S3Head])
	add(ps.S3Get, &delta.Usage.S3Requests[resource.S3Get])
	add(ps.S3Put, &delta.Usage.S3Requests[resource.S3Put])
	add(ps.S3List, &delta.Usage.S3Requests[resource.S3List])
	add(ps.S3Delete, &delta.Usage.S3Requests[resource.S3Delete])
	add(ps.S3DeleteMul, &delta.Usage.S3Requests[resource.S3DeleteMulti])
	return delta
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
		"SpillSize:%dbytes "+
		"SpillRows:%d "+
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
		ps.SpillSize,
		ps.SpillRows,
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
	if ps.ReadSize > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ReadSize:%dbytes ", ps.ReadSize))
	}
	if ps.S3ReadSize > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("S3ReadSize:%dbytes ", ps.S3ReadSize))
	}
	if ps.DiskReadSize > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("DiskReadSize:%dbytes ", ps.DiskReadSize))
	}
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
	if ps.ParquetFiles > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetFiles:%d ", ps.ParquetFiles))
	}
	if ps.ParquetRowGroups > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetRowGroups:%d ", ps.ParquetRowGroups))
	}
	if ps.ParquetRowsRead > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetRowsRead:%d ", ps.ParquetRowsRead))
	}
	if ps.ParquetBytesRead > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetBytesRead:%dbytes ", ps.ParquetBytesRead))
	}
	if ps.ParquetPrefetchBytes > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetPrefetchBytes:%dbytes ", ps.ParquetPrefetchBytes))
	}
	if ps.ParquetProjectedColumns > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetProjectedColumns:%d ", ps.ParquetProjectedColumns))
	}
	if ps.ParquetTotalColumns > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetTotalColumns:%d ", ps.ParquetTotalColumns))
	}
	if ps.ParquetSelectedFiles > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetSelectedFiles:%d ", ps.ParquetSelectedFiles))
	}
	if ps.ParquetSelectedFileBytes > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetSelectedFileBytes:%dbytes ", ps.ParquetSelectedFileBytes))
	}
	if ps.IcebergMetadataBytes > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergMetadataBytes:%dbytes ", ps.IcebergMetadataBytes))
	}
	if ps.IcebergManifestListBytes > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergManifestListBytes:%dbytes ", ps.IcebergManifestListBytes))
	}
	if ps.IcebergManifestBytes > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergManifestBytes:%dbytes ", ps.IcebergManifestBytes))
	}
	if ps.IcebergManifestsSelected > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergManifestsSelected:%d ", ps.IcebergManifestsSelected))
	}
	if ps.IcebergManifestsPruned > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergManifestsPruned:%d ", ps.IcebergManifestsPruned))
	}
	if ps.IcebergDataFilesSelected > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergDataFilesSelected:%d ", ps.IcebergDataFilesSelected))
	}
	if ps.IcebergDataFilesPruned > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergDataFilesPruned:%d ", ps.IcebergDataFilesPruned))
	}
	if ps.IcebergDataFileBytesSelected > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergDataFileBytesSelected:%dbytes ", ps.IcebergDataFileBytesSelected))
	}
	if ps.IcebergDataFileBytesPruned > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergDataFileBytesPruned:%dbytes ", ps.IcebergDataFileBytesPruned))
	}
	if ps.IcebergPlanningCacheHits > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergPlanningCacheHits:%d ", ps.IcebergPlanningCacheHits))
	}
	if ps.IcebergPlanningCacheMiss > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergPlanningCacheMiss:%d ", ps.IcebergPlanningCacheMiss))
	}
	if ps.IcebergDeleteFilesRead > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergDeleteFilesRead:%d ", ps.IcebergDeleteFilesRead))
	}
	if ps.IcebergDeleteRowsFiltered > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergDeleteRowsFiltered:%d ", ps.IcebergDeleteRowsFiltered))
	}
	if ps.IcebergPositionDeleteRowsFiltered > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergPositionDeleteRowsFiltered:%d ", ps.IcebergPositionDeleteRowsFiltered))
	}
	if ps.IcebergEqualityDeleteRowsFiltered > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergEqualityDeleteRowsFiltered:%d ", ps.IcebergEqualityDeleteRowsFiltered))
	}
	if ps.IcebergDeleteApplyPeakMemoryBytes > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("IcebergDeleteApplyPeakMemoryBytes:%dbytes ", ps.IcebergDeleteApplyPeakMemoryBytes))
	}
	if ps.ParquetOpenTime > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetOpenTime:%dns ", ps.ParquetOpenTime))
	}
	if ps.ParquetReadPageTime > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetReadPageTime:%dns ", ps.ParquetReadPageTime))
	}
	if ps.ParquetMapTime > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetMapTime:%dns ", ps.ParquetMapTime))
	}
	if ps.ParquetRowModeTime > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetRowModeTime:%dns ", ps.ParquetRowModeTime))
	}
	if ps.ParquetPeakBatchBytes > 0 {
		dynamicAttrs = append(dynamicAttrs, fmt.Sprintf("ParquetPeakBatchBytes:%dbytes ", ps.ParquetPeakBatchBytes))
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
	return strings.TrimRight(sb.String(), " ")
}

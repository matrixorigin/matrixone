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

package process

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperatorStatsResourceDelta(t *testing.T) {
	stats := OperatorStats{
		TimeConsumed:     10,
		WaitTimeConsumed: 20,
		SpillSize:        30,
		S3ReadSize:       40,
		S3WriteSize:      41,
		S3Head:           1,
		S3Get:            2,
		S3Put:            3,
		S3List:           4,
		S3Delete:         5,
		S3DeleteMul:      6,
		ResourceWaitNS:   [resource.WaitKindCount]int64{resource.WaitLock: 7, resource.WaitOther: 13},
		OperatorMetrics:  map[MetricType]int64{OpWaitLockTime: 7},
	}
	delta := stats.ResourceDelta()
	assert.Zero(t, delta.Quality)
	assert.Equal(t, uint64(10), delta.Usage.ExclusiveActiveNS)
	assert.Equal(t, uint64(7), delta.Usage.WaitNS[resource.WaitLock])
	assert.Equal(t, uint64(13), delta.Usage.WaitNS[resource.WaitOther])
	assert.Equal(t, uint64(30), delta.Usage.SpillBytes)
	assert.Equal(t, uint64(40), delta.Usage.S3ReadBytes)
	assert.Equal(t, uint64(41), delta.Usage.S3WriteBytes)
	assert.Equal(t, [resource.S3OpCount]uint64{1, 2, 3, 4, 5, 6}, delta.Usage.S3Requests)

	stats.TimeConsumed = -1
	delta = stats.ResourceDelta()
	assert.NotZero(t, delta.Quality&resource.QualityInvariantFailure)
	assert.Zero(t, delta.Usage.ExclusiveActiveNS)
}

func TestOperatorAnalyzerRejectsNegativeObservedIntervals(t *testing.T) {
	opAlyzr := NewAnalyzer(0, false, false, "test").(*operatorAnalyzer)
	opAlyzr.Start()
	opAlyzr.wait = -time.Nanosecond
	opAlyzr.Stop()

	assert.NotZero(t, opAlyzr.opStats.ResourceQuality&resource.QualityInvariantFailure)
}

func TestOperatorAnalyzerRejectsObservedIntervalLargerThanCall(t *testing.T) {
	for _, test := range []struct {
		name  string
		wait  time.Duration
		child time.Duration
	}{
		{name: "wait", wait: time.Hour},
		{name: "child call", child: time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			opAlyzr := NewAnalyzer(0, false, false, "test").(*operatorAnalyzer)
			opAlyzr.Start()
			opAlyzr.start = time.Now().Add(-time.Second)
			opAlyzr.wait = test.wait
			opAlyzr.childrenCallDuration = test.child
			opAlyzr.Stop()

			assert.Zero(t, opAlyzr.opStats.TimeConsumed)
			assert.NotZero(t, opAlyzr.opStats.ResourceQuality&resource.QualityInvariantFailure)
		})
	}
}

func Test_operatorAnalyzer_AddWaitLockTime(t *testing.T) {
	type fields struct {
		nodeIdx              int
		isFirst              bool
		isLast               bool
		start                time.Time
		wait                 time.Duration
		childrenCallDuration time.Duration
		opStats              *OperatorStats
	}
	type args struct {
		t time.Time
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantPanic bool
	}{
		{
			name: "test01",
			fields: fields{
				nodeIdx:              0,
				isFirst:              true,
				isLast:               true,
				start:                time.Now(),
				wait:                 0,
				childrenCallDuration: 0,
				opStats:              nil,
			},
			args: args{
				t: time.Now(),
			},
			wantPanic: true,
		},
		{
			name: "test02",
			fields: fields{
				nodeIdx:              0,
				isFirst:              true,
				isLast:               true,
				start:                time.Now(),
				wait:                 0,
				childrenCallDuration: 0,
				opStats:              NewOperatorStats("testOp"),
			},
			args: args{
				t: time.Now(),
			},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("operatorAnalyzer.AddWaitLockTime() panic: %v", r)
					} else {
						t.Errorf("should catch operatorAnalyzer.AddWaitLockTime() panic")
					}
				}()
			}

			opAlyzr := &operatorAnalyzer{
				nodeIdx:              tt.fields.nodeIdx,
				isFirst:              tt.fields.isFirst,
				isLast:               tt.fields.isLast,
				start:                tt.fields.start,
				wait:                 tt.fields.wait,
				childrenCallDuration: tt.fields.childrenCallDuration,
				opStats:              tt.fields.opStats,
			}
			opAlyzr.AddWaitLockTime(tt.args.t)
		})
	}
}

func Test_operatorAnalyzer_AddIncrementTime(t *testing.T) {
	type fields struct {
		nodeIdx              int
		isFirst              bool
		isLast               bool
		start                time.Time
		wait                 time.Duration
		childrenCallDuration time.Duration
		opStats              *OperatorStats
	}
	type args struct {
		t time.Time
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantPanic bool
	}{
		{
			name: "test01",
			fields: fields{
				nodeIdx:              0,
				isFirst:              true,
				isLast:               true,
				start:                time.Now(),
				wait:                 0,
				childrenCallDuration: 0,
				opStats:              nil,
			},
			args: args{
				t: time.Now(),
			},
			wantPanic: true,
		},
		{
			name: "test02",
			fields: fields{
				nodeIdx:              0,
				isFirst:              true,
				isLast:               true,
				start:                time.Now(),
				wait:                 0,
				childrenCallDuration: 0,
				opStats:              NewOperatorStats("testOp"),
			},
			args: args{
				t: time.Now(),
			},
			wantPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("operatorAnalyzer.AddIncrementTime() panic: %v", r)
					} else {
						t.Errorf("should catch operatorAnalyzer.AddIncrementTime() panic")
					}
				}()
			}

			opAlyzr := &operatorAnalyzer{
				nodeIdx:              tt.fields.nodeIdx,
				isFirst:              tt.fields.isFirst,
				isLast:               tt.fields.isLast,
				start:                tt.fields.start,
				wait:                 tt.fields.wait,
				childrenCallDuration: tt.fields.childrenCallDuration,
				opStats:              tt.fields.opStats,
			}
			opAlyzr.AddIncrementTime(tt.args.t)
		})
	}
}

func TestOperatorStats_String(t *testing.T) {
	type fields struct {
		OperatorName     string
		CallNum          int
		TimeConsumed     int64
		WaitTimeConsumed int64
		MemorySize       int64
		SpillSize        int64
		InputRows        int64
		InputSize        int64
		OutputRows       int64
		OutputSize       int64
		NetworkIO        int64
		DiskIO           int64
		InputBlocks      int64
		ScanBytes        int64
		ReadSize         int64
		S3ReadSize       int64
		DiskReadSize     int64
		WrittenRows      int64
		DeletedRows      int64
		S3List           int64
		S3Head           int64
		S3Put            int64
		S3Get            int64
		S3Delete         int64
		S3DeleteMul      int64
		CacheRead        int64
		CacheHit         int64
		CacheMemoryRead  int64
		CacheMemoryHit   int64
		CacheDiskRead    int64
		CacheDiskHit     int64
		CacheRemoteRead  int64
		CacheRemoteHit   int64
		OperatorMetrics  map[MetricType]int64
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			// CallNum:154 TimeCost:54449492ns WaitTime:0ns InRows:1248064 OutRows:0 InSize:19969024bytes InBlock:153 OutSize:0bytes SpillSize:131072bytes ScanBytes:19969024bytes NetworkIO:0bytes DiskIO:7888601bytes CacheRead:428 CacheMemoryRead:428
			name: "test01",
			fields: fields{
				OperatorName:     "testOp",
				CallNum:          154,
				TimeConsumed:     54449492,
				WaitTimeConsumed: 0,
				MemorySize:       131072,
				SpillSize:        131072,
				InputRows:        1248064,
				InputSize:        19969024,
				OutputRows:       0,
				OutputSize:       0,
				NetworkIO:        0,
				DiskIO:           7888601,
				InputBlocks:      153,
				ScanBytes:        19969024,
				ReadSize:         16000000,
				S3ReadSize:       15000000,
				DiskReadSize:     1000000,
				WrittenRows:      12,
				DeletedRows:      12,
				S3List:           2,
				S3Head:           2,
				S3Put:            2,
				S3Get:            2,
				S3Delete:         2,
				S3DeleteMul:      2,
				CacheRead:        428,
				CacheHit:         428,
				CacheMemoryRead:  428,
				CacheMemoryHit:   428,
				CacheDiskRead:    428,
				CacheDiskHit:     428,
				CacheRemoteRead:  428,
				CacheRemoteHit:   428,
				OperatorMetrics: map[MetricType]int64{
					OpScanTime: 452,
				},
			},
			want: " CallNum:154 TimeCost:54449492ns WaitTime:0ns InRows:1248064 OutRows:0 InSize:19969024bytes InBlock:153 OutSize:0bytes SpillSize:131072bytes SpillRows:0 ScanBytes:19969024bytes NetworkIO:0bytes DiskIO:7888601bytes WrittenRows:12 DeletedRows:12 S3List:2 S3Head:2 S3Put:2 S3Get:2 S3Delete:2 S3DeleteMul:2 ReadSize:16000000bytes S3ReadSize:15000000bytes DiskReadSize:1000000bytes CacheRead:428 CacheHit:428 CacheMemoryRead:428 CacheMemoryHit:428 CacheDiskRead:428 CacheDiskHit:428 CacheRemoteRead:428 CacheRemoteHit:428 ScanTime:452ns",
		},
		{
			name: "test02 - ReadSize, S3ReadSize, DiskReadSize are zero, should not appear in output",
			fields: fields{
				OperatorName:     "testOp",
				CallNum:          10,
				TimeConsumed:     1000000,
				WaitTimeConsumed: 0,
				MemorySize:       1024,
				SpillSize:        0,
				InputRows:        100,
				InputSize:        1024,
				OutputRows:       50,
				OutputSize:       512,
				NetworkIO:        0,
				DiskIO:           0,
				InputBlocks:      1,
				ScanBytes:        1024,
				ReadSize:         0,
				S3ReadSize:       0,
				DiskReadSize:     0,
				WrittenRows:      0,
				DeletedRows:      0,
				S3List:           0,
				S3Head:           0,
				S3Put:            0,
				S3Get:            0,
				S3Delete:         0,
				S3DeleteMul:      0,
				CacheRead:        0,
				CacheHit:         0,
				CacheMemoryRead:  0,
				CacheMemoryHit:   0,
				CacheDiskRead:    0,
				CacheDiskHit:     0,
				CacheRemoteRead:  0,
				CacheRemoteHit:   0,
				OperatorMetrics:  nil,
			},
			want: " CallNum:10 TimeCost:1000000ns WaitTime:0ns InRows:100 OutRows:50 InSize:1024bytes InBlock:1 OutSize:512bytes SpillSize:0bytes SpillRows:0 ScanBytes:1024bytes NetworkIO:0bytes DiskIO:0bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := &OperatorStats{
				OperatorName:     tt.fields.OperatorName,
				CallNum:          tt.fields.CallNum,
				TimeConsumed:     tt.fields.TimeConsumed,
				WaitTimeConsumed: tt.fields.WaitTimeConsumed,
				MemorySize:       tt.fields.MemorySize,
				SpillSize:        tt.fields.SpillSize,
				InputRows:        tt.fields.InputRows,
				InputSize:        tt.fields.InputSize,
				OutputRows:       tt.fields.OutputRows,
				OutputSize:       tt.fields.OutputSize,
				NetworkIO:        tt.fields.NetworkIO,
				DiskIO:           tt.fields.DiskIO,
				InputBlocks:      tt.fields.InputBlocks,
				ScanBytes:        tt.fields.ScanBytes,
				ReadSize:         tt.fields.ReadSize,
				S3ReadSize:       tt.fields.S3ReadSize,
				DiskReadSize:     tt.fields.DiskReadSize,
				WrittenRows:      tt.fields.WrittenRows,
				DeletedRows:      tt.fields.DeletedRows,
				S3List:           tt.fields.S3List,
				S3Head:           tt.fields.S3Head,
				S3Put:            tt.fields.S3Put,
				S3Get:            tt.fields.S3Get,
				S3Delete:         tt.fields.S3Delete,
				S3DeleteMul:      tt.fields.S3DeleteMul,
				CacheRead:        tt.fields.CacheRead,
				CacheHit:         tt.fields.CacheHit,
				CacheMemoryRead:  tt.fields.CacheMemoryRead,
				CacheMemoryHit:   tt.fields.CacheMemoryHit,
				CacheDiskRead:    tt.fields.CacheDiskRead,
				CacheDiskHit:     tt.fields.CacheDiskHit,
				CacheRemoteRead:  tt.fields.CacheRemoteRead,
				CacheRemoteHit:   tt.fields.CacheRemoteHit,
				OperatorMetrics:  tt.fields.OperatorMetrics,
			}
			assert.Equalf(t, tt.want, ps.String(), "String()")
		})
	}
}

func TestOperatorAnalyzerHarvestsTerminalCounterIntervals(t *testing.T) {
	opAlyzr := NewAnalyzer(0, false, false, "test").(*operatorAnalyzer)
	opAlyzr.Start()

	first := opAlyzr.GetOpCounterSet()
	first.FileService.S3.Get.Add(1)
	first.FileService.ReadSize.Add(1024)
	first.FileService.S3ReadSize.Add(512)

	// Starting the next producer interval harvests the previous one.
	second := opAlyzr.GetOpCounterSet()
	assert.Equal(t, int64(1), opAlyzr.opStats.S3Get)
	assert.Equal(t, int64(1024), opAlyzr.opStats.ReadSize)
	second.FileService.S3.Put.Add(2)
	second.FileService.S3WriteSize.Add(256)

	// Stop is the terminal owner, including error and panic return paths.
	opAlyzr.Stop()
	assert.Equal(t, int64(2), opAlyzr.opStats.S3Put)
	assert.Equal(t, int64(256), opAlyzr.opStats.S3WriteSize)
}

func TestMeasureFilesystemWaitRecordsTerminalPaths(t *testing.T) {
	opAlyzr := NewAnalyzer(0, false, false, "test").(*operatorAnalyzer)

	err := MeasureFilesystemWaitErr(opAlyzr, func() error {
		return assert.AnError
	})
	assert.ErrorIs(t, err, assert.AnError)
	first := opAlyzr.opStats.ResourceWaitNS[resource.WaitFilesystem]
	assert.Positive(t, first)

	assert.Panics(t, func() {
		_ = MeasureFilesystemWaitErr(opAlyzr, func() error {
			panic("storage panic")
		})
	})
	assert.Greater(t, opAlyzr.opStats.ResourceWaitNS[resource.WaitFilesystem], first)
}

func TestMeasureWaitStopsAtBlockingBoundary(t *testing.T) {
	opAlyzr := NewAnalyzer(0, false, false, "test").(*operatorAnalyzer)
	opAlyzr.Start()

	_, err := MeasureWait(opAlyzr, resource.WaitOther, func() (struct{}, error) {
		time.Sleep(time.Millisecond)
		return struct{}{}, nil
	})
	assert.NoError(t, err)
	recordedWait := opAlyzr.opStats.ResourceWaitNS[resource.WaitOther]
	assert.Positive(t, recordedWait)

	// Work after the blocking call must remain active, not extend the wait.
	time.Sleep(5 * time.Millisecond)
	opAlyzr.Stop()
	assert.Equal(t, recordedWait, opAlyzr.opStats.ResourceWaitNS[resource.WaitOther])
	assert.Positive(t, opAlyzr.opStats.TimeConsumed)
	assert.Zero(t, opAlyzr.opStats.ResourceQuality)
}

func Test_operatorAnalyzer_AddParquetProfile(t *testing.T) {
	opAlyzr := NewTempAnalyzer()
	opAlyzr.AddParquetProfile(ParquetProfileStats{
		Files:                             1,
		RowGroups:                         2,
		RowsRead:                          100,
		BytesRead:                         1024,
		PrefetchBytes:                     512,
		ProjectedColumns:                  2,
		TotalColumns:                      4,
		SelectedFiles:                     1,
		SelectedFileBytes:                 1024,
		IcebergMetadataBytes:              10,
		IcebergManifestListBytes:          20,
		IcebergManifestBytes:              30,
		IcebergManifestsSelected:          2,
		IcebergManifestsPruned:            1,
		IcebergDataFilesSelected:          2,
		IcebergDataFilesPruned:            3,
		IcebergDataFileBytesSelected:      100,
		IcebergDataFileBytesPruned:        200,
		IcebergPlanningCacheHits:          4,
		IcebergPlanningCacheMiss:          5,
		IcebergDeleteFilesRead:            1,
		IcebergDeleteRowsFiltered:         6,
		IcebergPositionDeleteRowsFiltered: 2,
		IcebergEqualityDeleteRowsFiltered: 4,
		IcebergDeleteApplyPeakMemoryBytes: 2048,
		OpenTime:                          10,
		ReadPageTime:                      20,
		MapTime:                           30,
		RowModeTime:                       40,
		PeakBatchBytes:                    4096,
	})
	opAlyzr.AddParquetProfile(ParquetProfileStats{
		Files:                             2,
		RowGroups:                         3,
		RowsRead:                          200,
		BytesRead:                         2048,
		PrefetchBytes:                     256,
		ProjectedColumns:                  3,
		TotalColumns:                      4,
		SelectedFiles:                     2,
		SelectedFileBytes:                 2048,
		IcebergMetadataBytes:              11,
		IcebergManifestListBytes:          21,
		IcebergManifestBytes:              31,
		IcebergManifestsSelected:          3,
		IcebergManifestsPruned:            2,
		IcebergDataFilesSelected:          3,
		IcebergDataFilesPruned:            4,
		IcebergDataFileBytesSelected:      300,
		IcebergDataFileBytesPruned:        400,
		IcebergPlanningCacheHits:          6,
		IcebergPlanningCacheMiss:          7,
		IcebergDeleteFilesRead:            2,
		IcebergDeleteRowsFiltered:         8,
		IcebergPositionDeleteRowsFiltered: 3,
		IcebergEqualityDeleteRowsFiltered: 5,
		IcebergDeleteApplyPeakMemoryBytes: 1024,
		OpenTime:                          11,
		ReadPageTime:                      21,
		MapTime:                           31,
		RowModeTime:                       41,
		PeakBatchBytes:                    1024,
	})

	stats := opAlyzr.GetOpStats()
	assert.Equal(t, int64(3), stats.ParquetFiles)
	assert.Equal(t, int64(5), stats.ParquetRowGroups)
	assert.Equal(t, int64(300), stats.ParquetRowsRead)
	assert.Equal(t, int64(3072), stats.ParquetBytesRead)
	assert.Equal(t, int64(768), stats.ParquetPrefetchBytes)
	assert.Equal(t, int64(5), stats.ParquetProjectedColumns)
	assert.Equal(t, int64(8), stats.ParquetTotalColumns)
	assert.Equal(t, int64(3), stats.ParquetSelectedFiles)
	assert.Equal(t, int64(3072), stats.ParquetSelectedFileBytes)
	assert.Equal(t, int64(21), stats.IcebergMetadataBytes)
	assert.Equal(t, int64(41), stats.IcebergManifestListBytes)
	assert.Equal(t, int64(61), stats.IcebergManifestBytes)
	assert.Equal(t, int64(5), stats.IcebergManifestsSelected)
	assert.Equal(t, int64(3), stats.IcebergManifestsPruned)
	assert.Equal(t, int64(5), stats.IcebergDataFilesSelected)
	assert.Equal(t, int64(7), stats.IcebergDataFilesPruned)
	assert.Equal(t, int64(400), stats.IcebergDataFileBytesSelected)
	assert.Equal(t, int64(600), stats.IcebergDataFileBytesPruned)
	assert.Equal(t, int64(10), stats.IcebergPlanningCacheHits)
	assert.Equal(t, int64(12), stats.IcebergPlanningCacheMiss)
	assert.Equal(t, int64(3), stats.IcebergDeleteFilesRead)
	assert.Equal(t, int64(14), stats.IcebergDeleteRowsFiltered)
	assert.Equal(t, int64(5), stats.IcebergPositionDeleteRowsFiltered)
	assert.Equal(t, int64(9), stats.IcebergEqualityDeleteRowsFiltered)
	assert.Equal(t, int64(2048), stats.IcebergDeleteApplyPeakMemoryBytes)
	assert.Equal(t, int64(21), stats.ParquetOpenTime)
	assert.Equal(t, int64(41), stats.ParquetReadPageTime)
	assert.Equal(t, int64(61), stats.ParquetMapTime)
	assert.Equal(t, int64(81), stats.ParquetRowModeTime)
	assert.Equal(t, int64(4096), stats.ParquetPeakBatchBytes)

	got := stats.String()
	assert.Contains(t, got, "ParquetFiles:3 ")
	assert.Contains(t, got, "ParquetRowGroups:5 ")
	assert.Contains(t, got, "ParquetRowsRead:300 ")
	assert.Contains(t, got, "ParquetBytesRead:3072bytes ")
	assert.Contains(t, got, "ParquetPrefetchBytes:768bytes ")
	assert.Contains(t, got, "ParquetProjectedColumns:5 ")
	assert.Contains(t, got, "ParquetTotalColumns:8 ")
	assert.Contains(t, got, "ParquetSelectedFiles:3 ")
	assert.Contains(t, got, "ParquetSelectedFileBytes:3072bytes ")
	assert.Contains(t, got, "IcebergMetadataBytes:21bytes ")
	assert.Contains(t, got, "IcebergManifestListBytes:41bytes ")
	assert.Contains(t, got, "IcebergManifestBytes:61bytes ")
	assert.Contains(t, got, "IcebergManifestsSelected:5 ")
	assert.Contains(t, got, "IcebergManifestsPruned:3 ")
	assert.Contains(t, got, "IcebergDataFilesSelected:5 ")
	assert.Contains(t, got, "IcebergDataFilesPruned:7 ")
	assert.Contains(t, got, "IcebergDataFileBytesSelected:400bytes ")
	assert.Contains(t, got, "IcebergDataFileBytesPruned:600bytes ")
	assert.Contains(t, got, "IcebergPlanningCacheHits:10 ")
	assert.Contains(t, got, "IcebergPlanningCacheMiss:12 ")
	assert.Contains(t, got, "IcebergDeleteFilesRead:3 ")
	assert.Contains(t, got, "IcebergDeleteRowsFiltered:14 ")
	assert.Contains(t, got, "IcebergPositionDeleteRowsFiltered:5 ")
	assert.Contains(t, got, "IcebergEqualityDeleteRowsFiltered:9 ")
	assert.Contains(t, got, "IcebergDeleteApplyPeakMemoryBytes:2048bytes ")
	assert.Contains(t, got, "ParquetOpenTime:21ns ")
	assert.Contains(t, got, "ParquetReadPageTime:41ns ")
	assert.Contains(t, got, "ParquetMapTime:61ns ")
	assert.Contains(t, got, "ParquetRowModeTime:81ns ")
	assert.Contains(t, got, "ParquetPeakBatchBytes:4096bytes")
	assert.NotContains(t, got, "s3://")
	assert.NotContains(t, got, "warehouse")
	assert.NotContains(t, got, "secret")
	assert.NotContains(t, got, "CredentialScope")
}

func TestParquetProfileStatsEmptyAndAdd(t *testing.T) {
	var stats ParquetProfileStats
	require.True(t, stats.Empty())

	stats.Add(ParquetProfileStats{
		Files:                             1,
		RowGroups:                         2,
		RowsRead:                          3,
		BytesRead:                         4,
		PrefetchBytes:                     5,
		ProjectedColumns:                  6,
		TotalColumns:                      7,
		SelectedFiles:                     8,
		SelectedFileBytes:                 9,
		IcebergMetadataBytes:              10,
		IcebergManifestListBytes:          11,
		IcebergManifestBytes:              12,
		IcebergManifestsSelected:          13,
		IcebergManifestsPruned:            14,
		IcebergDataFilesSelected:          15,
		IcebergDataFilesPruned:            16,
		IcebergDataFileBytesSelected:      17,
		IcebergDataFileBytesPruned:        18,
		IcebergPlanningCacheHits:          19,
		IcebergPlanningCacheMiss:          20,
		IcebergDeleteFilesRead:            21,
		IcebergDeleteRowsFiltered:         22,
		IcebergPositionDeleteRowsFiltered: 23,
		IcebergEqualityDeleteRowsFiltered: 24,
		IcebergDeleteApplyPeakMemoryBytes: 25,
		OpenTime:                          26,
		ReadPageTime:                      27,
		MapTime:                           28,
		RowModeTime:                       29,
		PeakBatchBytes:                    30,
	})
	stats.Add(ParquetProfileStats{
		Files:                             2,
		IcebergDeleteApplyPeakMemoryBytes: 24,
		PeakBatchBytes:                    31,
	})

	require.False(t, stats.Empty())
	require.Equal(t, int64(3), stats.Files)
	require.Equal(t, int64(2), stats.RowGroups)
	require.Equal(t, int64(3), stats.RowsRead)
	require.Equal(t, int64(4), stats.BytesRead)
	require.Equal(t, int64(5), stats.PrefetchBytes)
	require.Equal(t, int64(6), stats.ProjectedColumns)
	require.Equal(t, int64(7), stats.TotalColumns)
	require.Equal(t, int64(8), stats.SelectedFiles)
	require.Equal(t, int64(9), stats.SelectedFileBytes)
	require.Equal(t, int64(10), stats.IcebergMetadataBytes)
	require.Equal(t, int64(11), stats.IcebergManifestListBytes)
	require.Equal(t, int64(12), stats.IcebergManifestBytes)
	require.Equal(t, int64(13), stats.IcebergManifestsSelected)
	require.Equal(t, int64(14), stats.IcebergManifestsPruned)
	require.Equal(t, int64(15), stats.IcebergDataFilesSelected)
	require.Equal(t, int64(16), stats.IcebergDataFilesPruned)
	require.Equal(t, int64(17), stats.IcebergDataFileBytesSelected)
	require.Equal(t, int64(18), stats.IcebergDataFileBytesPruned)
	require.Equal(t, int64(19), stats.IcebergPlanningCacheHits)
	require.Equal(t, int64(20), stats.IcebergPlanningCacheMiss)
	require.Equal(t, int64(21), stats.IcebergDeleteFilesRead)
	require.Equal(t, int64(22), stats.IcebergDeleteRowsFiltered)
	require.Equal(t, int64(23), stats.IcebergPositionDeleteRowsFiltered)
	require.Equal(t, int64(24), stats.IcebergEqualityDeleteRowsFiltered)
	require.Equal(t, int64(25), stats.IcebergDeleteApplyPeakMemoryBytes)
	require.Equal(t, int64(26), stats.OpenTime)
	require.Equal(t, int64(27), stats.ReadPageTime)
	require.Equal(t, int64(28), stats.MapTime)
	require.Equal(t, int64(29), stats.RowModeTime)
	require.Equal(t, int64(31), stats.PeakBatchBytes)
}

func TestOperatorAnalyzerAggregatesCountersAndStats(t *testing.T) {
	analyzer := NewAnalyzer(1, true, false, "scan")
	stats := analyzer.GetOpStats()
	require.Equal(t, "scan", stats.OperatorName)

	analyzer.Start()
	waitStart := time.Now().Add(-time.Millisecond)
	analyzer.WaitStop(waitStart)
	childStart := time.Now().Add(-time.Millisecond)
	analyzer.ChildrenCallStop(childStart)
	analyzer.Stop()

	analyzer.Alloc(10)
	analyzer.SetMemUsed(8)
	analyzer.SetMemUsed(16)
	analyzer.Spill(32)
	analyzer.SpillRows(4)
	analyzer.InputBlock()
	analyzer.AddWrittenRows(5)
	analyzer.AddDeletedRows(6)
	analyzer.AddScanTime(time.Now().Add(-time.Millisecond))
	analyzer.AddInsertTime(time.Now().Add(-time.Millisecond))
	analyzer.AddIncrementTime(time.Now().Add(-time.Millisecond))
	analyzer.AddWaitLockTime(time.Now().Add(-time.Millisecond))

	bat := batch.NewWithSize(0)
	bat.SetRowCount(3)
	analyzer.Input(bat)
	analyzer.Output(bat)
	analyzer.ScanBytes(bat)
	analyzer.Network(bat)

	counter := &perfcounter.CounterSet{}
	counter.FileService.S3.List.Add(1)
	counter.FileService.S3.Head.Add(2)
	counter.FileService.S3.Put.Add(3)
	counter.FileService.S3.Get.Add(4)
	counter.FileService.S3.Delete.Add(5)
	counter.FileService.S3.DeleteMulti.Add(6)
	counter.FileService.Cache.Read.Add(7)
	counter.FileService.Cache.Hit.Add(8)
	counter.FileService.Cache.Memory.Read.Add(9)
	counter.FileService.Cache.Memory.Hit.Add(10)
	counter.FileService.Cache.Disk.Read.Add(11)
	counter.FileService.Cache.Disk.Hit.Add(12)
	counter.FileService.Cache.Remote.Read.Add(13)
	counter.FileService.Cache.Remote.Hit.Add(14)
	counter.FileService.ReadSize.Add(15)
	counter.FileService.S3ReadSize.Add(16)
	counter.FileService.DiskReadSize.Add(17)
	analyzer.AddS3RequestCount(counter)
	analyzer.AddFileServiceCacheInfo(counter)
	analyzer.AddDiskIO(counter)
	analyzer.AddReadSizeInfo(counter)

	stats = analyzer.GetOpStats()
	require.Equal(t, 1, stats.CallNum)
	require.GreaterOrEqual(t, stats.WaitTimeConsumed, int64(0))
	require.GreaterOrEqual(t, stats.TimeConsumed, int64(0))
	require.Equal(t, int64(16), stats.MemorySize)
	require.Equal(t, int64(32), stats.SpillSize)
	require.Equal(t, int64(4), stats.SpillRows)
	require.Equal(t, int64(1), stats.InputBlocks)
	require.Equal(t, int64(3), stats.InputRows)
	require.Equal(t, int64(3), stats.OutputRows)
	require.Equal(t, int64(0), stats.ScanBytes)
	require.Equal(t, int64(0), stats.NetworkIO)
	require.Equal(t, int64(5), stats.WrittenRows)
	require.Equal(t, int64(6), stats.DeletedRows)
	require.Greater(t, stats.GetMetricByKey(OpScanTime), int64(0))
	require.Greater(t, stats.GetMetricByKey(OpInsertTime), int64(0))
	require.Greater(t, stats.GetMetricByKey(OpIncrementTime), int64(0))
	require.Greater(t, stats.GetMetricByKey(OpWaitLockTime), int64(0))
	require.Equal(t, int64(1), stats.S3List)
	require.Equal(t, int64(2), stats.S3Head)
	require.Equal(t, int64(3), stats.S3Put)
	require.Equal(t, int64(4), stats.S3Get)
	require.Equal(t, int64(5), stats.S3Delete)
	require.Equal(t, int64(6), stats.S3DeleteMul)
	require.Equal(t, int64(7), stats.CacheRead)
	require.Equal(t, int64(8), stats.CacheHit)
	require.Equal(t, int64(9), stats.CacheMemoryRead)
	require.Equal(t, int64(10), stats.CacheMemoryHit)
	require.Equal(t, int64(11), stats.CacheDiskRead)
	require.Equal(t, int64(12), stats.CacheDiskHit)
	require.Equal(t, int64(13), stats.CacheRemoteRead)
	require.Equal(t, int64(14), stats.CacheRemoteHit)
	require.Equal(t, int64(15), stats.ReadSize)
	require.Equal(t, int64(16), stats.S3ReadSize)
	require.Equal(t, int64(17), stats.DiskReadSize)

	opCounter := analyzer.GetOpCounterSet()
	opCounter.FileService.S3.List.Store(99)
	require.Same(t, opCounter, analyzer.GetOpCounterSet())
	require.Zero(t, opCounter.FileService.S3.List.Load())

	analyzer.Reset()
	stats = analyzer.GetOpStats()
	require.Zero(t, stats.CallNum)
	require.Zero(t, stats.MemorySize)
	require.Zero(t, stats.S3List)
	require.Zero(t, stats.GetMetricByKey(OpScanTime))
}

func TestOperatorStatsMetricHelpers(t *testing.T) {
	stats := NewOperatorStats("metrics")
	require.Equal(t, "metrics", stats.OperatorName)
	require.Zero(t, stats.GetMetricByKey(OpScanTime))

	stats.AddOpMetric(OpScanTime, 10)
	stats.AddOpMetric(OpScanTime, 5)
	stats.AddOpMetric(OpWaitLockTime, 3)
	require.Equal(t, int64(15), stats.GetMetricByKey(OpScanTime))
	require.Equal(t, int64(3), stats.GetMetricByKey(OpWaitLockTime))

	stats.Reset()
	require.Empty(t, stats.OperatorName)
	require.Zero(t, stats.GetMetricByKey(OpScanTime))
}

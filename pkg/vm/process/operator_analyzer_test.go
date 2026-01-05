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

	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
)

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
			name: "test01 - with ReadSize, S3ReadSize, DiskReadSize, no Cache stats",
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
				ReadSize:         16000000, // 15.26 MiB
				S3ReadSize:       15000000, // 14.31 MiB
				DiskReadSize:     1000000,  // 976.56 KiB
				WrittenRows:      12,
				DeletedRows:      12,
				S3List:           2,
				S3Head:           2,
				S3Put:            2,
				S3Get:            2,
				S3Delete:         2,
				S3DeleteMul:      2,
				CacheRead:        428, // Should not appear in output
				CacheHit:         428, // Should not appear in output
				CacheMemoryRead:  428, // Should not appear in output
				CacheMemoryHit:   428, // Should not appear in output
				CacheDiskRead:    428, // Should not appear in output
				CacheDiskHit:     428, // Should not appear in output
				CacheRemoteRead:  428, // Should not appear in output
				CacheRemoteHit:   428, // Should not appear in output
				OperatorMetrics: map[MetricType]int64{
					OpScanTime: 452,
				},
			},
			// Format: ReadSize=total|s3|disk (same as explain analyze)
			// Cache stats should NOT appear
			// OutSize is 0, so it should not appear
			// NetworkIO and DiskIO are removed to avoid duplication with ReadSize
			// Bytes and time are formatted: 0bytes -> 0B, 1386624bytes -> 1.39MB, 21625539ns -> 21.63ms
			// Note: ScanTime 452ns = 0.000452ms, rounded to 0.00ms
			// ReadSize uses FormatBytes (decimal units: MB, KB) instead of ConvertBytesToHumanReadable (binary units: MiB, KiB)
			// 16000000 bytes = 16.00MB (decimal), 15000000 bytes = 15.00MB (decimal), 1000000 bytes = 1.00MB (decimal)
			want: " CallNum:154 TimeCost:54.45ms WaitTime:0ns InRows:1248064 OutRows:0 InSize:19.97MB InBlock:153 MemSize:131.07KB SpillSize:131.07KB ScanBytes:19.97MB WrittenRows:12 DeletedRows:12 S3List:2 S3Head:2 S3Put:2 S3Get:2 S3Delete:2 S3DeleteMul:2 ReadSize=16.00MB|15.00MB|1.00MB ScanTime:0.00ms ",
		},
		{
			name: "test02 - SpillSize and ReadSize are zero, so they should not appear",
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
			// SpillSize is 0, so it should not appear
			// ReadSize all values are 0, so it should not appear
			// NetworkIO and DiskIO are removed to avoid duplication with ReadSize
			// Bytes and time are formatted: 0bytes -> 0B, 1386624bytes -> 1.39MB, 21625539ns -> 21.63ms
			want: " CallNum:10 TimeCost:1.00ms WaitTime:0ns InRows:100 OutRows:50 InSize:1.02KB InBlock:1 OutSize:512B MemSize:1.02KB ScanBytes:1.02KB ",
		},
		{
			name: "test03 - ReadSize with KiB format (real-world example)",
			fields: fields{
				OperatorName:     "TableScan",
				CallNum:          77,
				TimeConsumed:     1353592,
				WaitTimeConsumed: 0,
				MemorySize:       196608,
				SpillSize:        0,
				InputRows:        622592,
				InputSize:        7471104,
				OutputRows:       622592,
				OutputSize:       7471104,
				NetworkIO:        0,
				DiskIO:           0,
				InputBlocks:      76,
				ScanBytes:        7471104,
				ReadSize:         343050, // 335.01 KiB
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
				CacheRead:        228, // Should not appear
				CacheHit:         228, // Should not appear
				CacheMemoryRead:  228, // Should not appear
				CacheMemoryHit:   228, // Should not appear
				CacheDiskRead:    0,
				CacheDiskHit:     0,
				CacheRemoteRead:  0,
				CacheRemoteHit:   0,
				OperatorMetrics:  nil,
			},
			// Real-world example: ReadSize=343.05KB|0B|0B
			// SpillSize is 0, so it should not appear
			// NetworkIO and DiskIO are removed to avoid duplication with ReadSize
			// Bytes and time are formatted: 0bytes -> 0B, 1386624bytes -> 1.39MB, 21625539ns -> 21.63ms
			// ReadSize uses FormatBytes (0B) instead of ConvertBytesToHumanReadable (0 bytes)
			// 343050 bytes = 343.05KB (decimal), not 335.01 KiB (binary)
			want: " CallNum:77 TimeCost:1.35ms WaitTime:0ns InRows:622592 OutRows:622592 InSize:7.47MB InBlock:76 OutSize:7.47MB MemSize:196.61KB ScanBytes:7.47MB ReadSize=343.05KB|0B|0B ",
		},
		{
			name: "test04 - ReadSize with MiB format",
			fields: fields{
				OperatorName:     "TableScan",
				CallNum:          1,
				TimeConsumed:     1000000,
				WaitTimeConsumed: 0,
				MemorySize:       1024 * 1024,
				SpillSize:        0,
				InputRows:        1000000,
				InputSize:        100 * 1024 * 1024,
				OutputRows:       1000000,
				OutputSize:       100 * 1024 * 1024,
				NetworkIO:        0,
				DiskIO:           0,
				InputBlocks:      100,
				ScanBytes:        100 * 1024 * 1024,
				ReadSize:         50 * 1024 * 1024, // 50 MiB
				S3ReadSize:       30 * 1024 * 1024, // 30 MiB
				DiskReadSize:     20 * 1024 * 1024, // 20 MiB
				WrittenRows:      0,
				DeletedRows:      0,
				S3List:           0,
				S3Head:           0,
				S3Put:            0,
				S3Get:            0,
				S3Delete:         0,
				S3DeleteMul:      0,
				CacheRead:        1000, // Should not appear
				CacheHit:         1000, // Should not appear
				CacheMemoryRead:  500,  // Should not appear
				CacheMemoryHit:   500,  // Should not appear
				CacheDiskRead:    300,  // Should not appear
				CacheDiskHit:     300,  // Should not appear
				CacheRemoteRead:  200,  // Should not appear
				CacheRemoteHit:   200,  // Should not appear
				OperatorMetrics:  nil,
			},
			// SpillSize is 0, so it should not appear
			// NetworkIO and DiskIO are removed to avoid duplication with ReadSize
			// Bytes and time are formatted: 0bytes -> 0B, 1386624bytes -> 1.39MB, 21625539ns -> 21.63ms
			// ReadSize uses FormatBytes (decimal units: MB) instead of ConvertBytesToHumanReadable (binary units: MiB)
			// 50*1024*1024 bytes = 52.43MB (decimal), 30*1024*1024 = 31.46MB, 20*1024*1024 = 20.97MB
			want: " CallNum:1 TimeCost:1.00ms WaitTime:0ns InRows:1000000 OutRows:1000000 InSize:104.86MB InBlock:100 OutSize:104.86MB MemSize:1.05MB ScanBytes:104.86MB ReadSize=52.43MB|31.46MB|20.97MB ",
		},
		{
			name: "test05 - ReadSize with GiB format",
			fields: fields{
				OperatorName:     "TableScan",
				CallNum:          1,
				TimeConsumed:     1000000,
				WaitTimeConsumed: 0,
				MemorySize:       1024 * 1024 * 1024,
				SpillSize:        0,
				InputRows:        10000000,
				InputSize:        10 * 1024 * 1024 * 1024,
				OutputRows:       10000000,
				OutputSize:       10 * 1024 * 1024 * 1024,
				NetworkIO:        0,
				DiskIO:           0,
				InputBlocks:      1000,
				ScanBytes:        10 * 1024 * 1024 * 1024,
				ReadSize:         5 * 1024 * 1024 * 1024, // 5 GiB
				S3ReadSize:       3 * 1024 * 1024 * 1024, // 3 GiB
				DiskReadSize:     2 * 1024 * 1024 * 1024, // 2 GiB
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
			// SpillSize is 0, so it should not appear
			// NetworkIO and DiskIO are removed to avoid duplication with ReadSize
			// Bytes and time are formatted: 0bytes -> 0B, 1386624bytes -> 1.39MB, 21625539ns -> 21.63ms
			// ReadSize uses FormatBytes (decimal units: GB) instead of ConvertBytesToHumanReadable (binary units: GiB)
			// 5*1024*1024*1024 bytes = 5.37GB (decimal), 3*1024*1024*1024 = 3.22GB, 2*1024*1024*1024 = 2.15GB
			want: " CallNum:1 TimeCost:1.00ms WaitTime:0ns InRows:10000000 OutRows:10000000 InSize:10.74GB InBlock:1000 OutSize:10.74GB MemSize:1.07GB ScanBytes:10.74GB ReadSize=5.37GB|3.22GB|2.15GB ",
		},
		{
			name: "test06 - ReadSize with mixed formats (bytes, KiB, MiB)",
			fields: fields{
				OperatorName:     "TableScan",
				CallNum:          1,
				TimeConsumed:     1000000,
				WaitTimeConsumed: 0,
				MemorySize:       1024,
				SpillSize:        0,
				InputRows:        1000,
				InputSize:        1024 * 1024,
				OutputRows:       1000,
				OutputSize:       1024 * 1024,
				NetworkIO:        0,
				DiskIO:           0,
				InputBlocks:      1,
				ScanBytes:        1024 * 1024,
				ReadSize:         512,         // 512 bytes
				S3ReadSize:       10 * 1024,   // 10 KiB
				DiskReadSize:     1024 * 1024, // 1 MiB
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
			// SpillSize is 0, so it should not appear
			// NetworkIO and DiskIO are removed to avoid duplication with ReadSize
			// Bytes and time are formatted: 0bytes -> 0B, 1386624bytes -> 1.39MB, 21625539ns -> 21.63ms
			// ReadSize uses FormatBytes (decimal units: B, KB, MB) instead of ConvertBytesToHumanReadable (binary units: bytes, KiB, MiB)
			// 512 bytes = 512B, 10*1024 = 10.24KB, 1024*1024 = 1.05MB
			want: " CallNum:1 TimeCost:1.00ms WaitTime:0ns InRows:1000 OutRows:1000 InSize:1.05MB InBlock:1 OutSize:1.05MB MemSize:1.02KB ScanBytes:1.05MB ReadSize=512B|10.24KB|1.05MB ",
		},
		{
			name: "test07 - Cache stats should NOT appear even with non-zero values",
			fields: fields{
				OperatorName:     "TableScan",
				CallNum:          1,
				TimeConsumed:     1000000,
				WaitTimeConsumed: 0,
				MemorySize:       1024,
				SpillSize:        0,
				InputRows:        100,
				InputSize:        1024,
				OutputRows:       100,
				OutputSize:       1024,
				NetworkIO:        0,
				DiskIO:           0,
				InputBlocks:      1,
				ScanBytes:        1024,
				ReadSize:         1024,
				S3ReadSize:       512,
				DiskReadSize:     256,
				WrittenRows:      0,
				DeletedRows:      0,
				S3List:           0,
				S3Head:           0,
				S3Put:            0,
				S3Get:            0,
				S3Delete:         0,
				S3DeleteMul:      0,
				CacheRead:        1000, // Non-zero, but should NOT appear
				CacheHit:         800,  // Non-zero, but should NOT appear
				CacheMemoryRead:  500,  // Non-zero, but should NOT appear
				CacheMemoryHit:   400,  // Non-zero, but should NOT appear
				CacheDiskRead:    300,  // Non-zero, but should NOT appear
				CacheDiskHit:     200,  // Non-zero, but should NOT appear
				CacheRemoteRead:  100,  // Non-zero, but should NOT appear
				CacheRemoteHit:   50,   // Non-zero, but should NOT appear
				OperatorMetrics:  nil,
			},
			// Verify that no Cache* fields appear in output
			// SpillSize is 0, so it should not appear
			// NetworkIO and DiskIO are removed to avoid duplication with ReadSize
			// Bytes and time are formatted: 0bytes -> 0B, 1386624bytes -> 1.39MB, 21625539ns -> 21.63ms
			// ReadSize uses FormatBytes (decimal units: B, KB) instead of ConvertBytesToHumanReadable (binary units: bytes, KiB)
			// 1024 bytes = 1.02KB (decimal), 512 bytes = 512B, 256 bytes = 256B
			want: " CallNum:1 TimeCost:1.00ms WaitTime:0ns InRows:100 OutRows:100 InSize:1.02KB InBlock:1 OutSize:1.02KB MemSize:1.02KB ScanBytes:1.02KB ReadSize=1.02KB|512B|256B ",
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

func Test_operatorAnalyzer_AddReadSizeInfo(t *testing.T) {
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
		counter *perfcounter.CounterSet
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantPanic bool
		want      struct {
			ReadSize     int64
			S3ReadSize   int64
			DiskReadSize int64
		}
	}{
		{
			name: "panic when opStats is nil",
			fields: fields{
				opStats: nil,
			},
			args: args{
				counter: &perfcounter.CounterSet{},
			},
			wantPanic: true,
		},
		{
			name: "normal case - add read size info from CounterSet",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				counter: func() *perfcounter.CounterSet {
					cs := &perfcounter.CounterSet{}
					cs.FileService.ReadSize.Add(1024 * 1024)    // 1MB
					cs.FileService.S3ReadSize.Add(512 * 1024)   // 0.5MB
					cs.FileService.DiskReadSize.Add(256 * 1024) // 0.25MB
					return cs
				}(),
			},
			wantPanic: false,
			want: struct {
				ReadSize     int64
				S3ReadSize   int64
				DiskReadSize int64
			}{
				ReadSize:     1024 * 1024,
				S3ReadSize:   512 * 1024,
				DiskReadSize: 256 * 1024,
			},
		},
		{
			name: "accumulate read size info from CounterSet",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				counter: func() *perfcounter.CounterSet {
					cs := &perfcounter.CounterSet{}
					cs.FileService.ReadSize.Add(2048 * 1024)    // 2MB
					cs.FileService.S3ReadSize.Add(1536 * 1024)  // 1.5MB
					cs.FileService.DiskReadSize.Add(512 * 1024) // 0.5MB
					return cs
				}(),
			},
			wantPanic: false,
			want: struct {
				ReadSize     int64
				S3ReadSize   int64
				DiskReadSize int64
			}{
				ReadSize:     2048 * 1024,
				S3ReadSize:   1536 * 1024,
				DiskReadSize: 512 * 1024,
			},
		},
		{
			name: "accumulate multiple times",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				counter: func() *perfcounter.CounterSet {
					cs := &perfcounter.CounterSet{}
					cs.FileService.ReadSize.Add(1024 * 1024)    // 1MB
					cs.FileService.S3ReadSize.Add(512 * 1024)   // 0.5MB
					cs.FileService.DiskReadSize.Add(256 * 1024) // 0.25MB
					return cs
				}(),
			},
			wantPanic: false,
			want: struct {
				ReadSize     int64
				S3ReadSize   int64
				DiskReadSize int64
			}{
				ReadSize:     1024 * 1024,
				S3ReadSize:   512 * 1024,
				DiskReadSize: 256 * 1024,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("operatorAnalyzer.AddReadSizeInfo() panic: %v", r)
					} else {
						t.Errorf("should catch operatorAnalyzer.AddReadSizeInfo() panic")
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
			opAlyzr.AddReadSizeInfo(tt.args.counter)
			if !tt.wantPanic && opAlyzr.opStats != nil {
				assert.Equalf(t, tt.want.ReadSize, opAlyzr.opStats.ReadSize, "AddReadSizeInfo() ReadSize")
				assert.Equalf(t, tt.want.S3ReadSize, opAlyzr.opStats.S3ReadSize, "AddReadSizeInfo() S3ReadSize")
				assert.Equalf(t, tt.want.DiskReadSize, opAlyzr.opStats.DiskReadSize, "AddReadSizeInfo() DiskReadSize")
				// Test accumulation
				if tt.name == "normal case - add read size info from CounterSet" {
					cs2 := &perfcounter.CounterSet{}
					cs2.FileService.ReadSize.Add(2048 * 1024)    // 2MB
					cs2.FileService.S3ReadSize.Add(1024 * 1024)  // 1MB
					cs2.FileService.DiskReadSize.Add(512 * 1024) // 0.5MB
					opAlyzr.AddReadSizeInfo(cs2)
					assert.Equalf(t, int64(3072*1024), opAlyzr.opStats.ReadSize, "AddReadSizeInfo() should accumulate ReadSize")
					assert.Equalf(t, int64(1536*1024), opAlyzr.opStats.S3ReadSize, "AddReadSizeInfo() should accumulate S3ReadSize")
					assert.Equalf(t, int64(768*1024), opAlyzr.opStats.DiskReadSize, "AddReadSizeInfo() should accumulate DiskReadSize")
				}
			}
		})
	}
}

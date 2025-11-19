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
			// CallNum:154 TimeCost:54449492ns WaitTime:0ns InRows:1248064 OutRows:0 InSize:19969024bytes InBlock:153 OutSize:0bytes MemSize:131072bytes ScanBytes:19969024bytes NetworkIO:0bytes DiskIO:7888601bytes CacheRead:428 CacheMemoryRead:428
			name: "test01",
			fields: fields{
				OperatorName:     "testOp",
				CallNum:          154,
				TimeConsumed:     54449492,
				WaitTimeConsumed: 0,
				MemorySize:       131072,
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
			want: " CallNum:154 TimeCost:54449492ns WaitTime:0ns InRows:1248064 OutRows:0 InSize:19969024bytes InBlock:153 OutSize:0bytes MemSize:131072bytes ScanBytes:19969024bytes NetworkIO:0bytes DiskIO:7888601bytes WrittenRows:12 DeletedRows:12 S3List:2 S3Head:2 S3Put:2 S3Get:2 S3Delete:2 S3DeleteMul:2 ReadSize:16000000bytes S3ReadSize:15000000bytes DiskReadSize:1000000bytes CacheRead:428 CacheHit:428 CacheMemoryRead:428 CacheMemoryHit:428 CacheDiskRead:428 CacheDiskHit:428 CacheRemoteRead:428 CacheRemoteHit:428 ScanTime:452ns ",
		},
		{
			name: "test02 - ReadSize, S3ReadSize, DiskReadSize are zero, should not appear in output",
			fields: fields{
				OperatorName:     "testOp",
				CallNum:          10,
				TimeConsumed:     1000000,
				WaitTimeConsumed: 0,
				MemorySize:       1024,
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
			want: " CallNum:10 TimeCost:1000000ns WaitTime:0ns InRows:100 OutRows:50 InSize:1024bytes InBlock:1 OutSize:512bytes MemSize:1024bytes ScanBytes:1024bytes NetworkIO:0bytes DiskIO:0bytes ",
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

func Test_operatorAnalyzer_AddReadSize(t *testing.T) {
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
		bytes int64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantPanic bool
		want      int64
	}{
		{
			name: "panic when opStats is nil",
			fields: fields{
				opStats: nil,
			},
			args: args{
				bytes: 100,
			},
			wantPanic: true,
		},
		{
			name: "normal case - add read size",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				bytes: 1024,
			},
			wantPanic: false,
			want:      1024,
		},
		{
			name: "accumulate read size",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				bytes: 2048,
			},
			wantPanic: false,
			want:      2048,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("operatorAnalyzer.AddReadSize() panic: %v", r)
					} else {
						t.Errorf("should catch operatorAnalyzer.AddReadSize() panic")
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
			opAlyzr.AddReadSize(tt.args.bytes)
			if !tt.wantPanic && opAlyzr.opStats != nil {
				assert.Equalf(t, tt.want, opAlyzr.opStats.ReadSize, "AddReadSize() ReadSize")
				// Test accumulation
				if tt.name == "normal case - add read size" {
					opAlyzr.AddReadSize(2048)
					assert.Equalf(t, int64(3072), opAlyzr.opStats.ReadSize, "AddReadSize() should accumulate")
				}
			}
		})
	}
}

func Test_operatorAnalyzer_AddS3ReadSize(t *testing.T) {
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
		bytes int64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantPanic bool
		want      int64
	}{
		{
			name: "panic when opStats is nil",
			fields: fields{
				opStats: nil,
			},
			args: args{
				bytes: 100,
			},
			wantPanic: true,
		},
		{
			name: "normal case - add S3 read size",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				bytes: 1536 * 1024, // 1.5MB
			},
			wantPanic: false,
			want:      1536 * 1024,
		},
		{
			name: "accumulate S3 read size",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				bytes: 2048 * 1024, // 2MB
			},
			wantPanic: false,
			want:      2048 * 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("operatorAnalyzer.AddS3ReadSize() panic: %v", r)
					} else {
						t.Errorf("should catch operatorAnalyzer.AddS3ReadSize() panic")
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
			opAlyzr.AddS3ReadSize(tt.args.bytes)
			if !tt.wantPanic && opAlyzr.opStats != nil {
				assert.Equalf(t, tt.want, opAlyzr.opStats.S3ReadSize, "AddS3ReadSize() S3ReadSize")
				// Test accumulation
				if tt.name == "normal case - add S3 read size" {
					opAlyzr.AddS3ReadSize(512 * 1024)
					assert.Equalf(t, int64(2048*1024), opAlyzr.opStats.S3ReadSize, "AddS3ReadSize() should accumulate")
				}
			}
		})
	}
}

func Test_operatorAnalyzer_AddDiskReadSize(t *testing.T) {
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
		bytes int64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantPanic bool
		want      int64
	}{
		{
			name: "panic when opStats is nil",
			fields: fields{
				opStats: nil,
			},
			args: args{
				bytes: 100,
			},
			wantPanic: true,
		},
		{
			name: "normal case - add disk read size",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				bytes: 512 * 1024, // 0.5MB
			},
			wantPanic: false,
			want:      512 * 1024,
		},
		{
			name: "accumulate disk read size",
			fields: fields{
				opStats: NewOperatorStats("testOp"),
			},
			args: args{
				bytes: 1024 * 1024, // 1MB
			},
			wantPanic: false,
			want:      1024 * 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("operatorAnalyzer.AddDiskReadSize() panic: %v", r)
					} else {
						t.Errorf("should catch operatorAnalyzer.AddDiskReadSize() panic")
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
			opAlyzr.AddDiskReadSize(tt.args.bytes)
			if !tt.wantPanic && opAlyzr.opStats != nil {
				assert.Equalf(t, tt.want, opAlyzr.opStats.DiskReadSize, "AddDiskReadSize() DiskReadSize")
				// Test accumulation
				if tt.name == "normal case - add disk read size" {
					opAlyzr.AddDiskReadSize(256 * 1024)
					assert.Equalf(t, int64(768*1024), opAlyzr.opStats.DiskReadSize, "AddDiskReadSize() should accumulate")
				}
			}
		})
	}
}

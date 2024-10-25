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

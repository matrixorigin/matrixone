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

package errors

import (
	"context"
	"fmt"
	"testing"
)

func TestRecover(t *testing.T) {
}

func TestRecoverRaw(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Logf("Recover() error = %v", err)
		} else {
			t.Logf("Recover() error = %v, wantErr %v", err, true)
		}
	}()
	panic("TestRecoverRaw")
}

func TestRecoverFunc(t *testing.T) {
	defer Recover(context.Background())
	panic("TestRecoverFunc")
}

func TestReportPanic(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		hasContext bool
		wantMsg    string
	}{
		{
			name:       "normal",
			args:       args{ctx: context.Background()},
			wantErr:    true,
			hasContext: true,
			wantMsg:    "TestReportPanic",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if err := recover(); (err != nil) == tt.wantErr {
					t.Logf("recover() error = %v", err)
					err = ReportPanic(tt.args.ctx, err, 1)
					if fmt.Sprintf("%s", err) != fmt.Sprintf("panic: %s", tt.wantMsg) {
						t.Errorf("ReportPanic() error = %v, wantMsg: %s", err, tt.wantMsg)
					}
					if HasContext(err.(error)) != tt.hasContext {
						t.Errorf("ReportPanic() error = %v, wantErr %v", err, tt.hasContext)
					} else {
						t.Logf("ReportPanic() error = %+v", err)
					}
				} else {
					t.Errorf("Recover() error = %v, wantErr %v", err, tt.wantErr)
				}
			}()
			panic(tt.wantMsg)
		})
	}
}

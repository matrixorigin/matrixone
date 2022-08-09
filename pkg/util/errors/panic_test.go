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

	"github.com/matrixorigin/matrixone/pkg/util"

	"github.com/stretchr/testify/require"
)

func InitErrorCollector(ch chan error) {
	SetErrorReporter(func(_ context.Context, err error, i int) {
		ch <- err
	})
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
	resultCh := make(chan error)
	InitErrorCollector(resultCh)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				ReportPanic(context.Background(), err)
				t.Logf("raw Caller: %+v", util.Callers(0))
			}
		}()
		panic("TestRecoverFunc")
	}()
	err := <-resultCh
	require.NotEmpty(t, err, "get error is nil.")
	t.Logf("err %%s : %s", err)
	t.Logf("err %%+v: %+v", err)
}

func TestRecoverFunc_nil(t *testing.T) {
	resultCh := make(chan error)
	InitErrorCollector(resultCh)
	go func() {
		defer func() {
			if err := recover(); err != nil {

				ReportPanic(context.Background(), err)
				t.Logf("raw Caller: %+v", util.Callers(0))
			}
		}()
		panic(fmt.Errorf("TestRecoverFunc"))
	}()
	err := <-resultCh
	require.NotEmpty(t, err, "get error is nil.")
	t.Logf("err %%s : %s", WithStack(fmt.Errorf("example")))
	t.Logf("err %%+v: %+v", err)
}

func TestReportPanic(t *testing.T) {
	SetErrorReporter(noopReportError)
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
					err = ReportPanic(tt.args.ctx, err)
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

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

package trace

import (
	"context"
	"strings"
	"sync"
	"testing"

	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

func Test_showSchema(t *testing.T) {

	t.Logf("%s", sqlCreateStatementInfoTable)
	t.Logf("%s", sqlCreateSpanInfoTable)
	t.Logf("%s", sqlCreateLogInfoTable)
	t.Logf("%s", sqlCreateErrorInfoTable)
}

var _ ie.InternalExecutor = &dummySqlExecutor{}

type dummySqlExecutor struct {
	opts ie.SessionOverrideOptions
	ch   chan<- string
}

func (e *dummySqlExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {}
func (e *dummySqlExecutor) Query(ctx context.Context, s string, options ie.SessionOverrideOptions) ie.InternalExecResult {
	return nil
}
func (e *dummySqlExecutor) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	e.ch <- sql
	return nil
}

// copy from /Users/jacksonxie/go/src/github.com/matrixorigin/matrixone/pkg/util/metric/metric_collector_test.go
func newDummyExecutorFactory(sqlch chan string) func() ie.InternalExecutor {
	return func() ie.InternalExecutor {
		return &dummySqlExecutor{
			opts: ie.NewOptsBuilder().Finish(),
			ch:   sqlch,
		}
	}
}

func TestInitSchemaByInnerExecutor(t *testing.T) {
	type args struct {
		ctx context.Context
		ch  chan string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "dummy",
			args: args{
				ctx: context.Background(),
				ch:  make(chan string, 10),
			},
		},
		{
			name: "dummyS3",
			args: args{
				ctx: context.Background(),
				ch:  make(chan string, 10),
			},
		},
	}

	// (1 + 4) * n + 1
	// 1: create database ...
	// 4: create EXTERNAL table
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			wg.Add(1 + len(initDDLs))
			err := InitSchemaByInnerExecutor(tt.args.ctx, newDummyExecutorFactory(tt.args.ch))
			require.Equal(t, nil, err)
			go func() {
				wg.Wait()
				wg.Add(1)
				close(tt.args.ch)
			}()
		loop:
			for {
				sql, ok := <-tt.args.ch
				wg.Done()
				if ok {
					t.Logf("exec sql: %s", sql)
					if sql == "create database if not exists system" {
						continue
					}
					idx := strings.Index(sql, "CREATE EXTERNAL TABLE")
					require.Equal(t, 0, idx)
				} else {
					t.Log("exec sql Done.")
					wg.Wait()
					break loop
				}
			}
		})
	}
}

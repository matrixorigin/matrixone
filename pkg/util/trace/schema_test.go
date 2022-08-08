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
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"sync"
	"testing"
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
func (e *dummySqlExecutor) Query(s string, options ie.SessionOverrideOptions) ie.InternalExecResult {
	return nil
}
func (e *dummySqlExecutor) Exec(sql string, opts ie.SessionOverrideOptions) error {
	e.ch <- sql
	return nil
}

// copy from /Users/jacksonxie/go/src/github.com/matrixorigin/matrixone/pkg/util/metric/metric_collector_test.go
func newExecutorFactory(sqlch chan string) func() ie.InternalExecutor {
	return func() ie.InternalExecutor {
		return &dummySqlExecutor{
			opts: ie.NewOptsBuilder().Finish(),
			ch:   sqlch,
		}
	}
}

func TestInitSchemaByInnerExecutor(t *testing.T) {
	type args struct {
		ieFactory func() ie.InternalExecutor
	}
	c := make(chan string, 10)
	tests := []struct {
		name string
		args args
	}{
		{
			name: "fake",
			args: args{newExecutorFactory(c)},
		},
	}
	wg := sync.WaitGroup{}
	startedC := make(chan struct{}, 1)
	wg.Add(1)
	go func() {
		startedC <- struct{}{}
	loop:
		for {
			sql, ok := <-c
			if ok {
				t.Logf("exec sql: %s", sql)
			} else {
				t.Log("exec sql Done.")
				break loop
			}
		}
		wg.Done()
	}()
	<-startedC
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitSchemaByInnerExecutor(tt.args.ieFactory)
		})
	}
	close(c)
	wg.Wait()
}

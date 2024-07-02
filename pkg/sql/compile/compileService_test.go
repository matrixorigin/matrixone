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

package compile

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
)

func generateRunningProc(n int) []*process.Process {
	rs := make([]*process.Process, n)
	for i := range rs {
		ctx, cancel := context.WithCancel(context.TODO())

		rs[i] = &process.Process{
			Ctx:    ctx,
			Cancel: cancel,
		}
	}
	return rs
}

func TestCompileService(t *testing.T) {
	service := InitCompileService()

	doneRoutine := atomic.Int32{}
	doneRoutine.Store(0)
	wg := sync.WaitGroup{}

	// 1. service should count running Compile in correct.
	inputs := generateRunningProc(10)
	for _, p := range inputs {
		wg.Add(1)

		c := service.getCompile(p)
		go func(cc *Compile) {
			<-cc.proc.Ctx.Done()

			doneRoutine.Add(1)
			_, _ = service.putCompile(cc)
			wg.Done()
		}(c)
	}
	require.Equal(t, 10, service.aliveCompile())

	// 2. kill all running Compile.
	service.PauseService()
	service.KillAllQueriesWithError(nil)
	service.ResumeService()

	require.Equal(t, int32(10), doneRoutine.Load())

	// after all, alive compile should be 0.
	wg.Wait()
	require.Equal(t, 0, service.aliveCompile())
}

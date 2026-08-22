// Copyright 2026 Matrix Origin
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

package plan

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestMockCompilerContextReusesProcess(t *testing.T) {
	tests := []struct {
		name string
		ctx  *MockCompilerContext
	}{
		{name: "constructor", ctx: NewMockCompilerContext(false)},
		{name: "literal", ctx: &MockCompilerContext{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertMockCompilerContextReusesProcess(t, test.ctx)
		})
	}
}

func assertMockCompilerContextReusesProcess(t *testing.T, ctx *MockCompilerContext) {
	const workers = 16

	results := make(chan *process.Process, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- ctx.GetProcess()
		}()
	}
	wg.Wait()
	close(results)

	first := ctx.GetProcess()
	for result := range results {
		require.Same(t, first, result)
	}
}

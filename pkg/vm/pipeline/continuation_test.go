// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

type escapedContinuation struct {
	*colexec.MockOperator
	calls int
}

func (op *escapedContinuation) Call(*process.Process) (vm.CallResult, error) {
	op.calls++
	return vm.CallResult{Status: vm.ExecYield}, nil
}
func TestPipelineRejectsUnownedContinuation(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := &escapedContinuation{MockOperator: colexec.NewMockOperator()}
	complete, err := NewMerge(op).Run(proc)
	require.ErrorContains(t, err, "continuation demand escaped")
	require.False(t, complete)
	require.Equal(t, 1, op.calls)
}

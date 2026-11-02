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

package icebergwrite

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type yieldingIcebergInput struct {
	*colexec.MockOperator
	results []vm.CallResult
}

func (input *yieldingIcebergInput) Call(*process.Process) (vm.CallResult, error) {
	result := input.results[0]
	input.results = input.results[1:]
	return result, nil
}

func TestIcebergWritePreservesCoordinatorAcrossYield(t *testing.T) {
	for _, mode := range []string{"eof", "cancel", "append_error", "commit_error"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			coord := &testCoordinator{}
			failure := errors.New("injected writer failure")
			if mode == "append_error" {
				coord.appendErr = failure
			}
			if mode == "commit_error" {
				coord.commitErr = failure
			}
			bat := testBatchWithRows(proc, 2)
			defer bat.Clean(proc.Mp())
			op := NewArgument(AppendRequest{Operation: OperationAppend}).WithCoordinator(coord)
			defer op.Free(proc, false, nil)
			op.AppendChild(&yieldingIcebergInput{MockOperator: colexec.NewMockOperator(), results: []vm.CallResult{
				{Status: vm.ExecYield}, {Status: vm.ExecNext, Batch: bat}, {Status: vm.ExecYield}, {Status: vm.ExecStop},
			}})
			require.NoError(t, op.Prepare(proc))
			result, err := op.Call(proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecYield, result.Status)
			require.Nil(t, result.Batch)
			require.Equal(t, 1, coord.beginCalls)
			require.Zero(t, coord.commitCalls, "a demand checkpoint is not EOF")
			require.Zero(t, coord.appendCalls)
			if mode == "cancel" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				cancel()
				proc.Ctx = ctx
				op.Reset(proc, true, context.Canceled)
				require.Equal(t, 1, coord.abortCalls)
				require.NoError(t, coord.abortContextErr, "abort must use recovery context")
			} else {
				result, err = op.Call(proc)
				if mode == "append_error" {
					require.ErrorIs(t, err, failure)
					op.Reset(proc, true, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, vm.ExecNext, result.Status)
					require.Equal(t, []int{2}, coord.appendRows)
					result, err = op.Call(proc)
					require.NoError(t, err)
					require.Equal(t, vm.ExecYield, result.Status)
					require.Zero(t, coord.commitCalls)
					result, err = op.Call(proc)
					if mode == "commit_error" {
						require.ErrorIs(t, err, failure)
					} else {
						require.NoError(t, err)
						require.Equal(t, vm.ExecStop, result.Status)
					}
					require.Equal(t, 1, coord.commitCalls)
					op.Reset(proc, err != nil, err)
				}
			}
			op.Free(proc, false, nil)
			if mode == "eof" {
				require.Zero(t, coord.abortCalls)
			} else {
				require.Equal(t, 1, coord.abortCalls)
			}
		})
	}
}

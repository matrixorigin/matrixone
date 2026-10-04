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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestMockOperatorEndOfDataCallback(t *testing.T) {
	callbackCalls := 0
	op := NewMockOperator().WithEndOfDataCallback(func() {
		callbackCalls++
	})

	result, err := op.Call(nil)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Equal(t, 1, callbackCalls)

	_, err = op.Call(nil)
	require.NoError(t, err)
	require.Equal(t, 1, callbackCalls)

	op.Reset(nil, false, nil)
	_, err = op.Call(nil)
	require.NoError(t, err)
	require.Equal(t, 2, callbackCalls)

	op.Free(nil, false, nil)
	_, err = op.Call(nil)
	require.NoError(t, err)
	require.Equal(t, 2, callbackCalls)
}

func TestMockOperatorBatchCallback(t *testing.T) {
	var batchIndexes []int
	op := NewMockOperator().
		WithBatchs([]*batch.Batch{batch.EmptyBatch, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			batchIndexes = append(batchIndexes, index)
		})

	for range 3 {
		_, err := op.Call(nil)
		require.NoError(t, err)
	}
	require.Equal(t, []int{0, 1}, batchIndexes)

	op.Reset(nil, false, nil)
	_, err := op.Call(nil)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 0}, batchIndexes)

	op.Free(nil, false, nil)
	_, err = op.Call(nil)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 0}, batchIndexes)
}

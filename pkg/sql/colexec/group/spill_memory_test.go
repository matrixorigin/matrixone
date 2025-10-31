// Copyright 2025 Matrix Origin
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

package group

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMemorySpillManager(t *testing.T) {
	proc := testutil.NewProcess(t)
	before := proc.Mp().CurrNB()

	manager := NewMemorySpillManager()
	defer manager.Free()

	// Test spill and retrieve
	data := &SpillableAggState{
		GroupCount: 10,
		MarshaledAggStates: [][]byte{
			[]byte("test_agg_state_1"),
			[]byte("test_agg_state_2"),
		},
	}

	id, err := manager.Spill(data)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	retrieved, err := manager.Retrieve(id, proc.Mp())
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	spillState, ok := retrieved.(*SpillableAggState)
	require.True(t, ok)
	require.Equal(t, int(10), spillState.GroupCount)
	require.Equal(t, 2, len(spillState.MarshaledAggStates))

	// Test delete
	err = manager.Delete(id)
	require.NoError(t, err)

	_, err = manager.Retrieve(id, proc.Mp())
	require.Error(t, err)

	// Test memory accounting
	require.Equal(t, int64(0), manager.TotalMem())

	// Test with larger data
	largeData := &SpillableAggState{
		GroupCount:         1000,
		MarshaledAggStates: make([][]byte, 100),
	}
	for i := range largeData.MarshaledAggStates {
		largeData.MarshaledAggStates[i] = make([]byte, 1024) // 1KB per state
	}

	id, err = manager.Spill(largeData)
	require.NoError(t, err)

	// Verify memory usage increased
	memAfterSpill := manager.TotalMem()
	require.Greater(t, memAfterSpill, int64(0))

	// Clean up
	err = manager.Delete(id)
	require.NoError(t, err)

	require.Equal(t, int64(0), manager.TotalMem())

	after := proc.Mp().CurrNB()
	require.Equal(t, before, after, "Memory leak detected")
}

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

package hashbuild

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newTestAllocationAccount(t testing.TB) *mpool.AllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.Open(1 << 60)
	require.NoError(t, err)
	return account
}

func installTestExecutionResourceBudget(
	t testing.TB,
	op *HashBuild,
	generation *process.ExecutionResourceGeneration,
) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<60, generation)
	require.NoError(t, err)
	replaceTestHashBuildAllocation(t, op, account)
	op.ctr.hashmapBuilder.setBudget(generation)
}

func installTestProcessExecutionResourceBudget(
	t testing.TB,
	op *HashBuild,
	proc *process.Process,
) *process.ExecutionResourceGeneration {
	t.Helper()
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<60, generation)
	require.NoError(t, err)
	replaceTestHashBuildAllocation(t, op, account)
	return generation
}

func newTestHashmapBuilder(t testing.TB) *HashmapBuilder {
	t.Helper()
	builder := &HashmapBuilder{}
	require.NoError(t, builder.SetAllocationAccount(newTestAllocationAccount(t)))
	return builder
}

func installTestHashBuildAllocation(t testing.TB, op *HashBuild) {
	t.Helper()
	require.NoError(t, op.SetAllocationAccount(newTestAllocationAccount(t)))
}

func replaceTestHashBuildAllocation(
	t testing.TB,
	op *HashBuild,
	account *mpool.AllocationAccount,
) {
	t.Helper()
	if current := op.ctr.hashmapBuilder.mapAllocationAccount; current != nil {
		require.NoError(t, op.ClearAllocationAccount(current))
	}
	require.NoError(t, op.SetAllocationAccount(account))
}

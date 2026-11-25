// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIndependentGenerationSharesAdmissionNotLifetime(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1024, 1024)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	first, err := parent.OpenIndependentGeneration()
	require.NoError(t, err)
	second, err := parent.OpenIndependentGeneration()
	require.NoError(t, err)
	require.NoError(t, first.AcquireAllocationCapacity(600))
	require.Error(t, second.AcquireAllocationCapacity(600))
	first.Close()
	require.False(t, parent.Closed())
	require.False(t, second.Closed())
	first.ReleaseAllocationCapacity(600)
	require.NoError(t, second.AcquireAllocationCapacity(600))
	second.ReleaseAllocationCapacity(600)
	require.Zero(t, budget.AggregateUsed())
	second.Close()
	parent.Close()
}

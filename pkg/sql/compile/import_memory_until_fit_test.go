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

package compile

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/stretchr/testify/require"
)

func TestImportMemoryFinalGraphUsesUntilFitRealConsumer(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("dop-%d", parallel), func(t *testing.T) {
			c := memoryCompileFixture(t, parallel)
			m := &compilerMemoryAdmission{
				real: newCompilerMemoryThrottler(t.Name(), .9), realCandidates: true,
			}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
			var specs []rscthrottler.WorkingSetSpec
			if parallel == 1 {
				require.Equal(t, 1, m.finalCalls)
				require.Zero(t, m.candidateCalls)
				specs = m.finalSpecs
			} else {
				require.Zero(t, m.finalCalls)
				require.Equal(t, 1, m.candidateCalls)
				specs = m.candidates
			}
			require.Greater(t, len(specs), 1, "the caller must keep all candidate units, including during final waiting")
			require.LessOrEqual(t, len(specs), 27)
			require.Zero(t, m.tries, "candidate admission enters one real FIFO request")
			require.Equal(t, int64(64<<20), c.proc.Base.ImportMemory.UnitBytes)
			require.Equal(t, int64(64<<20), specs[0].UnitBytes)
			require.Equal(t, c.proc.Base.ImportMemory.UnitBytes, c.proc.Base.ImportMemory.WorkingSet.UnitBytes())
		})
	}
}

func TestImportMemoryUntilFitDoesNotReplaceHigherDOPRetry(t *testing.T) {
	c := memoryCompileFixture(t, 2)
	pressure := &rscthrottler.WorkingSetCapacityError{Minimum: 300, Available: 200}
	m := &compilerMemoryAdmission{
		candidateErr: pressure, realCandidates: true,
	}
	err := c.beginStatementImportMemory(context.Background(), m)
	var retry *importDOPRetryError
	require.ErrorAs(t, err, &retry)
	require.Equal(t, 1, retry.next)
	require.Zero(t, m.finalCalls)
	require.Equal(t, 1, m.candidateCalls)
	require.Nil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryUntilFitCancellationIsNotCapacityOrDOPRetry(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	m := &compilerMemoryAdmission{finalErr: context.Canceled, realCandidates: true}
	err := c.beginStatementImportMemory(context.Background(), m)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, memoryRetryParallel(err))
	require.Equal(t, 1, m.finalCalls)
	require.Zero(t, m.candidateCalls)
	require.Nil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryUntilFitStaticCapacityIsTerminal(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	capacity := &rscthrottler.WorkingSetCapacityError{Minimum: 1001, Available: 1000}
	m := &compilerMemoryAdmission{finalErr: capacity, realCandidates: true}
	err := c.beginStatementImportMemory(context.Background(), m)
	require.ErrorIs(t, err, capacity)
	require.Zero(t, memoryRetryParallel(err), "a statically impossible final graph has no smaller DOP")
	require.Equal(t, 1, m.finalCalls)
	require.Zero(t, m.candidateCalls)
	require.Nil(t, c.proc.Base.ImportMemory)
}

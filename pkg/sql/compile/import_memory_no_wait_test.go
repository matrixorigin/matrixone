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
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

type importMemoryTxnOptions struct {
	client.TxnOperator
	options txn.TxnOptions
}

func (op *importMemoryTxnOptions) TxnOptions() txn.TxnOptions { return op.options }

func TestImportMemoryOriginWaitPolicy(t *testing.T) {
	for _, test := range []struct {
		name           string
		options        txn.TxnOptions
		nilTxn, noWait bool
	}{
		{name: "autocommit", options: txn.TxnOptions{Autocommit: true}},
		{name: "begin", options: txn.TxnOptions{ByBegin: true, Autocommit: true}, noWait: true},
		{name: "autocommit_off", noWait: true},
		{name: "begin_autocommit_off", options: txn.TxnOptions{ByBegin: true}, noWait: true},
		{name: "nil_embedding", nilTxn: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			if !test.nilTxn {
				c.proc.Base.TxnOperator = &importMemoryTxnOptions{options: test.options}
			}
			require.Equal(t, test.noWait, c.importMemoryNoWait())
			local, err := c.localMemoryManifest(64 << 20)
			require.NoError(t, err)
			require.Equal(t, test.noWait, local.NoWait)
			provider := &compilerMemoryAdmission{real: newCompilerMemoryThrottler(t.Name(), .9), available: math.MaxInt64, realCandidates: true}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), provider))
			if test.noWait {
				require.Equal(t, 1, provider.tries)
				require.Zero(t, provider.finalCalls)
			} else {
				require.Zero(t, provider.tries)
				require.Equal(t, 1, provider.finalCalls, "known autocommit and nil embedding retain D32")
			}
			require.Zero(t, provider.candidateCalls)
		})
	}
}

func TestImportMemoryNoWaitPressureKeepsTypedFinalAndDOPRetry(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		t.Run(map[int]string{1: "final", 2: "retry"}[parallel], func(t *testing.T) {
			c := memoryCompileFixture(t, parallel)
			c.proc.Base.TxnOperator = &importMemoryTxnOptions{options: txn.TxnOptions{ByBegin: true, Autocommit: true}}
			manifest, err := c.localMemoryManifest(64 << 20)
			require.NoError(t, err)
			specs, _, err := memoryManifestCandidates(manifest, manifest.TargetUnit)
			require.NoError(t, err)
			m := &compilerMemoryAdmission{realCandidates: true}
			err = c.beginStatementImportMemory(context.Background(), m)
			if parallel > 1 {
				require.Equal(t, 1, memoryRetryParallel(err))
			} else {
				var capacity *rscthrottler.WorkingSetCapacityError
				require.ErrorAs(t, err, &capacity)
				require.Zero(t, capacity.Available)
				require.Positive(t, capacity.Minimum)
			}
			require.Equal(t, len(specs), m.tries, "every legal candidate must be considered without waiting")
			require.Zero(t, m.finalCalls)
			require.Zero(t, m.candidateCalls)
			require.Nil(t, c.proc.Base.ImportMemory)
		})
	}
}

func TestImportMemoryNoWaitTriesSmallerCandidateBeforeError(t *testing.T) {
	for _, tooLarge := range []bool{false, true} {
		t.Run(fmt.Sprint(tooLarge), func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			c.proc.Base.TxnOperator = &importMemoryTxnOptions{}
			manifest, err := c.localMemoryManifest(64 << 20)
			require.NoError(t, err)
			specs, _, err := memoryManifestCandidates(manifest, manifest.TargetUnit)
			require.NoError(t, err)
			require.Greater(t, len(specs), 1)
			budget := (workingLayoutTotal(specs[0]) + workingLayoutTotal(specs[1])) / 2
			m := &importMemoryDiagnosticAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: newCompilerMemoryThrottler(t.Name(), .9), available: budget, realCandidates: true}}
			calls := 0
			m.try = func(ctx context.Context, spec rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, bool, error) {
				calls++
				if tooLarge && calls == 1 {
					return nil, false, &rscthrottler.RequestTooLargeError{Requested: 1001, Maximum: 1000}
				}
				return m.compilerMemoryAdmission.TryReserveWorkingSet(ctx, spec)
			}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
			require.Equal(t, 2, calls)
			require.Equal(t, int64(32<<20), specs[1].UnitBytes)
			require.Equal(t, specs[1].UnitBytes, c.proc.Base.ImportMemory.UnitBytes)
			require.Zero(t, m.finalCalls)
			require.Zero(t, m.candidateCalls)
		})
	}
}

type importMemoryDiagnosticAdmission struct {
	*compilerMemoryAdmission
	sample func() rscthrottler.WorkingSetHeadroomSnapshot
	try    func(context.Context, rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, bool, error)
}

func (m *importMemoryDiagnosticAdmission) WorkingSetHeadroom() rscthrottler.WorkingSetHeadroomSnapshot {
	if m.sample != nil {
		return m.sample()
	}
	return m.compilerMemoryAdmission.WorkingSetHeadroom()
}
func (m *importMemoryDiagnosticAdmission) TryReserveWorkingSet(ctx context.Context, spec rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, bool, error) {
	if m.try != nil {
		return m.try(ctx, spec)
	}
	return m.compilerMemoryAdmission.TryReserveWorkingSet(ctx, spec)
}

func TestImportMemoryNoWaitTooLargeAndCancellation(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		c := memoryCompileFixture(t, parallel)
		c.proc.Base.TxnOperator = &importMemoryTxnOptions{}
		m := &compilerMemoryAdmission{failure: &rscthrottler.RequestTooLargeError{Requested: 1001, Maximum: 1000}, realCandidates: true}
		err := c.beginStatementImportMemory(context.Background(), m)
		if parallel > 1 {
			require.Equal(t, 1, memoryRetryParallel(err))
		} else {
			var capacity *rscthrottler.WorkingSetCapacityError
			require.ErrorAs(t, err, &capacity)
		}
		require.Greater(t, m.tries, 1, "a large first candidate must not prevent trying smaller ones")
	}
	c := memoryCompileFixture(t, 1)
	c.proc.Base.TxnOperator = &importMemoryTxnOptions{}
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	cause := errors.New("cancel during pressure diagnostics")
	m := &importMemoryDiagnosticAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{realCandidates: true}}
	m.sample = func() rscthrottler.WorkingSetHeadroomSnapshot {
		cancel(cause)
		return rscthrottler.WorkingSetHeadroomSnapshot{Available: 1000}
	}
	err := c.beginStatementImportMemory(ctx, m)
	require.ErrorIs(t, err, cause)
	require.Zero(t, memoryRetryParallel(err))
	require.Nil(t, c.proc.Base.ImportMemory)
	require.Zero(t, m.finalCalls)
}

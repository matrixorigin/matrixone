// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/stretchr/testify/require"
)

type compilerUntilFitAdmission struct {
	*compilerCandidatesAdmission
	finalCalls int
	finalSpecs []rscthrottler.WorkingSetSpec
	finalErr   error
}

func (m *compilerUntilFitAdmission) ReserveWorkingSetCandidatesUntilFit(ctx context.Context, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	m.finalCalls++
	m.finalSpecs = specs
	if m.finalErr != nil {
		return nil, m.finalErr
	}
	return m.real.(rscthrottler.WorkingSetCandidatesUntilFitThrottler).ReserveWorkingSetCandidatesUntilFit(ctx, specs)
}

func TestImportMemoryFinalGraphUsesUntilFitRealConsumer(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		t.Run(fmt.Sprintf("dop-%d", parallel), func(t *testing.T) {
			c := memoryCompileFixture(t, parallel)
			m := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{
				real: rscthrottler.NewMemThrottler(t.Name(), .9),
			}}}
			require.NoError(t, c.admitImportMemory(context.Background(), m))
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
			require.Equal(t, int64(64<<20), specs[0].UnitBytes)
			require.Equal(t, c.proc.Base.ImportMemory.UnitBytes, c.proc.Base.ImportMemory.WorkingSet.UnitBytes())
		})
	}
}

func TestImportMemoryUntilFitDoesNotReplaceHigherDOPRetry(t *testing.T) {
	c := memoryCompileFixture(t, 2)
	pressure := &rscthrottler.WorkingSetCapacityError{Minimum: 300, Available: 200}
	m := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{
		compilerMemoryAdmission: &compilerMemoryAdmission{}, candidateErr: pressure,
	}}
	err := c.admitImportMemory(context.Background(), m)
	var retry *importDOPRetryError
	require.ErrorAs(t, err, &retry)
	require.Equal(t, 1, retry.next)
	require.Zero(t, m.finalCalls)
	require.Equal(t, 1, m.candidateCalls)
	require.Nil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryUntilFitCancellationIsNotCapacityOrDOPRetry(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	m := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{}}, finalErr: context.Canceled}
	err := c.admitImportMemory(context.Background(), m)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, MemoryRetryParallel(err))
	require.Equal(t, 1, m.finalCalls)
	require.Zero(t, m.candidateCalls)
	require.Nil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryUntilFitStaticCapacityIsTerminal(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	capacity := &rscthrottler.WorkingSetCapacityError{Minimum: 1001, Available: 1000}
	m := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{}}, finalErr: capacity}
	err := c.admitImportMemory(context.Background(), m)
	require.ErrorIs(t, err, capacity)
	require.Zero(t, MemoryRetryParallel(err), "a statically impossible final graph has no smaller DOP")
	require.Equal(t, 1, m.finalCalls)
	require.Zero(t, m.candidateCalls)
	require.Nil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryUntilFitKeepsOlderProviderContracts(t *testing.T) {
	for _, capable := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy_single_spec", true: "legacy_candidates"}[capable], func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			old := &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: math.MaxInt64}
			var provider rscthrottler.RSCThrottler = old
			candidates := &compilerCandidatesAdmission{compilerMemoryAdmission: old}
			if capable {
				provider = candidates
			}
			_, untilFit := provider.(rscthrottler.WorkingSetCandidatesUntilFitThrottler)
			require.False(t, untilFit)
			require.NoError(t, c.admitImportMemory(context.Background(), provider))
			if capable {
				require.Equal(t, 1, candidates.candidateCalls)
				require.Greater(t, len(candidates.candidates), 1)
				require.Zero(t, old.tries)
			} else {
				require.Equal(t, 1, old.tries)
				require.Zero(t, old.waits)
			}
		})
	}
}

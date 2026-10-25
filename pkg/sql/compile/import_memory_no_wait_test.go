// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
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
			provider := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: math.MaxInt64}}}
			require.NoError(t, c.beginStatementImportMemory(context.Background(), provider))
			if test.noWait {
				require.Equal(t, 1, provider.tries)
				require.Zero(t, provider.finalCalls)
			} else {
				require.Zero(t, provider.tries)
				require.Equal(t, 1, provider.finalCalls, "known autocommit and nil embedding retain D32")
			}
			require.Zero(t, provider.candidateCalls)
			require.Zero(t, provider.waits)
			require.Zero(t, provider.reclaims)
		})
	}
}

func TestImportMemoryNoWaitPressureKeepsTypedFinalAndDOPRetry(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		for _, owners := range []bool{false, true} {
			t.Run(map[bool]string{false: "ownerless", true: "active_owner"}[owners]+map[int]string{1: "_final", 2: "_retry"}[parallel], func(t *testing.T) {
				c := memoryCompileFixture(t, parallel)
				c.proc.Base.TxnOperator = &importMemoryTxnOptions{options: txn.TxnOptions{ByBegin: true, Autocommit: true}}
				manifest, err := c.localMemoryManifest(64 << 20)
				require.NoError(t, err)
				specs, _, err := memoryManifestCandidates(manifest, manifest.TargetUnit)
				require.NoError(t, err)
				m := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{owners: owners}}}
				err = c.beginStatementImportMemory(context.Background(), m)
				if parallel > 1 {
					require.Equal(t, 1, memoryRetryParallel(err))
				} else {
					var capacity *rscthrottler.WorkingSetCapacityError
					require.ErrorAs(t, err, &capacity)
					require.Equal(t, int64(0), capacity.Available)
					require.Positive(t, capacity.Minimum)
				}
				require.Equal(t, len(specs), m.tries, "every legal candidate must be considered without waiting")
				require.Zero(t, m.finalCalls)
				require.Zero(t, m.candidateCalls)
				require.Zero(t, m.waits)
				require.Zero(t, m.reclaims)
				require.Nil(t, c.proc.Base.ImportMemory)
			})
		}
	}
}

func TestImportMemoryNoWaitTriesSmallerCandidateBeforeError(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	c.proc.Base.TxnOperator = &importMemoryTxnOptions{}
	manifest, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	specs, _, err := memoryManifestCandidates(manifest, manifest.TargetUnit)
	require.NoError(t, err)
	require.Greater(t, len(specs), 1)
	budget := (workingLayoutTotal(specs[0]) + workingLayoutTotal(specs[1])) / 2
	m := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: budget}}}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Equal(t, specs[1].UnitBytes, c.proc.Base.ImportMemory.UnitBytes)
	require.Equal(t, 2, m.tries)
	require.Zero(t, m.finalCalls)
	require.Zero(t, m.candidateCalls)
	require.Zero(t, m.waits)
	require.Zero(t, m.reclaims)
}

type importMemoryDiagnosticAdmission struct {
	*compilerUntilFitAdmission
	sample func() rscthrottler.WorkingSetHeadroomSnapshot
	try    func(context.Context, rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, bool, error)
}

func (m *importMemoryDiagnosticAdmission) WorkingSetHeadroom() rscthrottler.WorkingSetHeadroomSnapshot {
	if m.sample != nil {
		return m.sample()
	}
	return m.compilerUntilFitAdmission.WorkingSetHeadroom()
}
func (m *importMemoryDiagnosticAdmission) TryReserveWorkingSet(ctx context.Context, spec rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, bool, error) {
	if m.try != nil {
		return m.try(ctx, spec)
	}
	return m.compilerUntilFitAdmission.TryReserveWorkingSet(ctx, spec)
}

func TestImportMemoryNoWaitTooLargeAndCancellation(t *testing.T) {
	for _, parallel := range []int{1, 2} {
		c := memoryCompileFixture(t, parallel)
		c.proc.Base.TxnOperator = &importMemoryTxnOptions{}
		m := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{failure: &rscthrottler.RequestTooLargeError{Requested: 1001, Maximum: 1000}}}}
		err := c.beginStatementImportMemory(context.Background(), m)
		if parallel > 1 {
			require.Equal(t, 1, memoryRetryParallel(err))
		} else {
			var capacity *rscthrottler.WorkingSetCapacityError
			require.ErrorAs(t, err, &capacity)
		}
		require.Greater(t, m.tries, 1, "a large first candidate must not prevent trying smaller ones")
		require.Zero(t, m.reclaims)
		require.Zero(t, m.waits)
	}
	c := memoryCompileFixture(t, 1)
	c.proc.Base.TxnOperator = &importMemoryTxnOptions{}
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	cause := errors.New("cancel during pressure diagnostics")
	m := &importMemoryDiagnosticAdmission{compilerUntilFitAdmission: &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{}}}}
	m.sample = func() rscthrottler.WorkingSetHeadroomSnapshot {
		cancel(cause)
		return rscthrottler.WorkingSetHeadroomSnapshot{Available: 1000}
	}
	err := c.beginStatementImportMemory(ctx, m)
	require.ErrorIs(t, err, cause)
	require.Zero(t, memoryRetryParallel(err))
	require.Nil(t, c.proc.Base.ImportMemory)
	require.Zero(t, m.finalCalls)
	require.Zero(t, m.reclaims)
}

func TestImportMemoryNoWaitTooLargeCandidateKeepsSmallerFit(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	c.proc.Base.TxnOperator = &importMemoryTxnOptions{}
	m := &importMemoryDiagnosticAdmission{compilerUntilFitAdmission: &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: math.MaxInt64}}}}
	calls := 0
	m.try = func(ctx context.Context, spec rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, bool, error) {
		calls++
		if calls == 1 {
			return nil, false, &rscthrottler.RequestTooLargeError{Requested: 1001, Maximum: 1000}
		}
		return m.compilerUntilFitAdmission.TryReserveWorkingSet(ctx, spec)
	}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Equal(t, 2, calls)
	require.Equal(t, int64(32<<20), c.proc.Base.ImportMemory.UnitBytes)
	require.Zero(t, m.finalCalls)
	require.Zero(t, m.reclaims)
}

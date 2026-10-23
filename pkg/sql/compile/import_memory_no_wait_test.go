// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
			manifests, _, err := c.collectMemoryManifests(64 << 20)
			require.NoError(t, err)
			require.NotEmpty(t, manifests)
			for _, entry := range manifests {
				require.Equal(t, test.noWait, entry.manifest.NoWait)
				wire, err := EncodeMemoryControlRequest(MemoryControlRequest{Action: MemoryControlTryOpen, Manifest: &entry.manifest})
				require.NoError(t, err)
				decoded, matched, err := DecodeMemoryControlRequest(wire)
				require.NoError(t, err)
				require.True(t, matched)
				require.Equal(t, test.noWait, decoded.Manifest.NoWait)
			}
			provider := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: math.MaxInt64}}}
			require.NoError(t, c.admitImportMemory(context.Background(), provider))
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
				err = c.admitImportMemory(context.Background(), m)
				if parallel > 1 {
					require.Equal(t, 1, MemoryRetryParallel(err))
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
				require.Empty(t, c.importMemoryControls)
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
	require.NoError(t, c.admitImportMemory(context.Background(), m))
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
		err := c.admitImportMemory(context.Background(), m)
		if parallel > 1 {
			require.Equal(t, 1, MemoryRetryParallel(err))
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
	err := c.admitImportMemory(ctx, m)
	require.ErrorIs(t, err, cause)
	require.Zero(t, MemoryRetryParallel(err))
	require.Nil(t, c.proc.Base.ImportMemory)
	require.Zero(t, m.finalCalls)
	require.Zero(t, m.reclaims)
}

func TestMemoryTryOpenResponsePreservesCapacityRetryAndCancellation(t *testing.T) {
	capacity := &rscthrottler.WorkingSetCapacityError{Minimum: 300, Available: 200}
	for _, test := range []struct {
		name   string
		retry  int
		cancel bool
	}{
		{name: "capacity"}, {name: "retry_wins", retry: 1}, {name: "cancel_wins", retry: 1, cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { cancel(nil) })
			cause := errors.New("cancel before remote pressure response")
			stream := &memoryControlTestStream{replies: make(chan morpc.Message, 1)}
			t.Cleanup(func() { require.NoError(t, stream.Close(true)) })
			stream.send = func(context.Context, morpc.Message) error {
				data, err := EncodeMemoryControlResponse(MemoryControlResponse{Error: capacity.Error(), Capacity: capacity, RetryParallel: test.retry})
				require.NoError(t, err)
				stream.replies <- &pipeline.Message{Data: data}
				if test.cancel {
					cancel(cause)
				}
				return nil
			}
			h := &statementMemoryControl{stream: stream, receive: stream.replies}
			_, err := h.request(ctx, MemoryControlRequest{Action: MemoryControlTryOpen, Manifest: &MemoryManifest{NoWait: true}})
			if test.cancel {
				require.ErrorIs(t, err, cause)
			} else if test.retry > 0 {
				require.Equal(t, test.retry, MemoryRetryParallel(err))
			} else {
				var got *rscthrottler.WorkingSetCapacityError
				require.ErrorAs(t, err, &got)
				require.Equal(t, capacity, got)
			}
		})
	}
}

func TestMemoryTryOpenRejectsWaitPolicyMismatchBeforeAdmission(t *testing.T) {
	for _, test := range []struct {
		action string
		noWait bool
	}{{MemoryControlOpen, true}, {MemoryControlTryOpen, false}} {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		session := &memoryControlSession{ctx: ctx, replies: make(chan *pipeline.Message, 1)}
		request := MemoryControlRequest{Action: test.action, Manifest: &MemoryManifest{Version: query.MemoryAdmissionProtocolVersion, NoWait: test.noWait}, ExpectedProcessID: runtime.ProcessInstanceID()}
		wire, err := EncodeMemoryControlRequest(request)
		require.NoError(t, err)
		handled, err := handleMemoryControl(ctx, "missing-runtime-must-not-be-used", &pipeline.Message{Id: 19, Cmd: pipeline.Method_StopSending, Data: wire}, session, func() morpc.Message { return &pipeline.Message{} })
		require.NoError(t, err)
		require.True(t, handled)
		response := receiveMemoryControl(t, session)
		require.Contains(t, response.Error, "wait policy mismatch")
		require.Empty(t, response.Token)
	}
}

func TestMemoryTryOpenFailureReverseClosesEarlierCNs(t *testing.T) {
	capacity := &rscthrottler.WorkingSetCapacityError{Minimum: 300, Available: 200}
	var order []string
	controls, _, err := acquireMemoryControls(context.Background(), []*memoryCNManifest{memoryControlCN("C"), memoryControlCN("B"), memoryControlCN("A")}, func(_ context.Context, entry *memoryCNManifest) (*statementMemoryControl, error) {
		order = append(order, "open"+entry.id)
		if entry.id == "C" {
			return nil, capacity
		}
		return &statementMemoryControl{unit: 64, memory: nil, closeLocal: func(error) { order = append(order, "close"+entry.id) }}, nil
	})
	require.ErrorIs(t, err, capacity)
	require.Nil(t, controls)
	require.Equal(t, []string{"openA", "openB", "openC", "closeB", "closeA"}, order)
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
	require.NoError(t, c.admitImportMemory(context.Background(), m))
	require.Equal(t, 2, calls)
	require.Equal(t, int64(32<<20), c.proc.Base.ImportMemory.UnitBytes)
	require.Zero(t, m.finalCalls)
	require.Zero(t, m.reclaims)
}

func TestMemoryTryOpenPressureReturnsTypedFailureWithoutOwner(t *testing.T) {
	service := t.Name()
	rt := runtime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop())
	runtime.SetupServiceBasedRuntime(service, rt)
	m := &compilerUntilFitAdmission{compilerCandidatesAdmission: &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{owners: true}}}
	rt.SetGlobalVariables(runtime.CNMemoryThrottler, m)
	c := memoryCompileFixture(t, 1)
	c.proc.Base.TxnOperator = &importMemoryTxnOptions{options: txn.TxnOptions{ByBegin: true, Autocommit: true}}
	manifest, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	session := &memoryControlSession{ctx: ctx, replies: make(chan *pipeline.Message, 1)}
	require.NoError(t, memoryControlCall(t, ctx, service, session, MemoryControlRequest{Action: MemoryControlTryOpen, Manifest: &manifest}))
	response := receiveMemoryControl(t, session)
	require.NotEmpty(t, response.Error)
	require.NotNil(t, response.Capacity)
	require.Empty(t, response.Token)
	require.Zero(t, response.RetryParallel)
	require.Greater(t, m.tries, 1)
	require.Zero(t, m.finalCalls)
	require.Zero(t, m.candidateCalls)
	require.Zero(t, m.waits)
	require.Zero(t, m.reclaims)
	statementMemoryOwners.Lock()
	defer statementMemoryOwners.Unlock()
	for key := range statementMemoryOwners.owners {
		require.NotEqual(t, service, key.service)
	}
}

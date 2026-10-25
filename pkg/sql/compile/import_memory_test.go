// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package compile

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func memorySourceScope(addr string, parallel int, fields int32) *Scope {
	op := &external.External{Es: &external.ExternalParam{ExParamConst: external.ExParamConst{Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV}}, Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}, ColumnListLen: 1, Attrs: []plan.ExternAttr{{ColFieldIndex: fields}}}}}
	return &Scope{NodeInfo: engine.Node{Addr: addr, Mcpu: parallel}, RootOp: op}
}

func TestImportMemoryLayoutCountsLocalParallelStages(t *testing.T) {
	local := "127.0.0.1:6001"
	c := &Compile{addr: local, scopes: []*Scope{memorySourceScope(local, 3, 27), memorySourceScope("127.0.0.2:6001", 100, 1)}}
	meta, spec, controlled, err := c.importMemoryLayout(64 << 20)
	require.NoError(t, err)
	require.True(t, controlled)
	require.Equal(t, uint32(3), spec.Inputs[0].Count)
	require.Zero(t, spec.Inputs[1].Count)
	require.Zero(t, spec.Inputs[2].Count)
	expected, err := external.SourceEnvelopeForFields([]types.Type{types.T_int64.ToType()}, 64<<20, 28)
	require.NoError(t, err)
	require.Equal(t, expected, meta.SourceBytes, "sparse CSV field indexes must be admitted before parser allocation")
	require.Equal(t, expected, spec.Inputs[0].Bytes)
	c.scopes = c.scopes[1:]
	_, _, controlled, err = c.importMemoryLayout(64 << 20)
	require.NoError(t, err)
	require.False(t, controlled, "a CN must not reserve another CN's input slots")
}

type compilerMemoryAdmission struct {
	real             rscthrottler.RSCThrottler
	available        int64
	owners           bool
	tries            int
	waits            int
	reclaims         int
	failure          error
	lastUnit         int64
	triedUnits       []int64
	reclaimAvailable int64
}

func (m *compilerMemoryAdmission) Refresh()                    {}
func (m *compilerMemoryAdmission) PrintUsage()                 {}
func (m *compilerMemoryAdmission) Acquire(int64) (int64, bool) { return m.available, false }
func (m *compilerMemoryAdmission) Release(int64) int64         { return m.available }
func (m *compilerMemoryAdmission) Available() int64            { return m.available }
func (m *compilerMemoryAdmission) AcquireContext(context.Context, int64) error {
	return errors.New("unexpected ordinary wait")
}
func (m *compilerMemoryAdmission) MaxAcquire() int64 { return math.MaxInt64 }
func (m *compilerMemoryAdmission) BatchBytes() int64 { return 64 << 20 }
func (m *compilerMemoryAdmission) WorkingSetHeadroom() rscthrottler.WorkingSetHeadroomSnapshot {
	return rscthrottler.WorkingSetHeadroomSnapshot{Available: m.available, HasOwners: m.owners}
}
func (m *compilerMemoryAdmission) TryReserveWorkingSet(ctx context.Context, spec rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, bool, error) {
	m.tries++
	m.lastUnit = spec.UnitBytes
	m.triedUnits = append(m.triedUnits, spec.UnitBytes)
	if m.failure != nil {
		return nil, false, m.failure
	}
	if err := context.Cause(ctx); err != nil {
		return nil, false, err
	}
	if workingLayoutTotal(spec) > m.available {
		return nil, false, nil
	}
	w, err := rscthrottler.ReserveWorkingSet(ctx, m.real, spec)
	return w, err == nil, err
}
func (m *compilerMemoryAdmission) ReserveWorkingSet(context.Context, rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	m.waits++
	return nil, errors.New("unexpected working-set wait")
}
func (m *compilerMemoryAdmission) ReclaimForWorkingSet(context.Context) error {
	m.reclaims++
	if m.reclaimAvailable > 0 {
		m.available = m.reclaimAvailable
	}
	return nil
}

func TestImportMemoryReclaimReconsidersLargestFittingUnit(t *testing.T) {
	for _, test := range []struct {
		name  string
		typ   types.Type
		fixed bool
	}{
		{name: "smaller-candidates-cannot-fit-before-reclaim", typ: types.T_int64.ToType()},
		{name: "single-row-floor-makes-unit-shrink-ineffective", typ: types.T_json.ToType(), fixed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest := MemoryManifest{TargetUnit: 64 << 20, Parallel: 1, Stages: []MemoryStageDescriptor{{Role: MemoryStageCSV, Count: 1, Schema: []types.Type{test.typ}, RetainedFields: 3}}}
			_, target, controlled, err := memoryManifestLayout(manifest, 64<<20)
			require.NoError(t, err)
			require.True(t, controlled)
			_, minimum, _, err := memoryManifestLayout(manifest, 1)
			require.NoError(t, err)
			if test.fixed {
				_, half, _, err := memoryManifestLayout(manifest, 32<<20)
				require.NoError(t, err)
				require.Equal(t, workingLayoutTotal(target), workingLayoutTotal(half))
				require.Equal(t, workingLayoutTotal(target), workingLayoutTotal(minimum), "same promise at 64MiB, 32MiB and one byte")
			}
			m := &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler("compiler-memory-reclaim-unit", .9), available: workingLayoutTotal(minimum) - 1, reclaimAvailable: workingLayoutTotal(target)}
			memory, err := admitMemoryManifest(context.Background(), m, manifest)
			require.NoError(t, err)
			require.NotNil(t, memory)
			defer memory.WorkingSet.Close()
			t.Logf("attempted units=%v; reclaimed=%d; admitted unit=%d; target bytes=%d; minimum bytes=%d", m.triedUnits, m.reclaims, memory.UnitBytes, workingLayoutTotal(target), workingLayoutTotal(minimum))
			require.Equal(t, 1, m.reclaims)
			require.Equal(t, int64(64<<20), memory.UnitBytes, "reclaim made the original target affordable; obsolete pressure must not permanently lower execution batch size")
			if test.fixed {
				for _, attempted := range m.triedUnits {
					require.Equal(t, int64(64<<20), attempted, "shrinking an unchanged promise has no admission benefit")
				}
			}
		})
	}
}

type compilerCandidatesAdmission struct {
	*compilerMemoryAdmission
	candidateCalls int
	candidates     []rscthrottler.WorkingSetSpec
	candidateErr   error
}

func (m *compilerCandidatesAdmission) ReserveWorkingSetCandidates(ctx context.Context, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	m.candidateCalls++
	m.candidates = specs
	if m.candidateErr != nil {
		return nil, m.candidateErr
	}
	return rscthrottler.ReserveWorkingSetCandidates(ctx, m.real, specs)
}

func TestImportMemoryCandidateCapabilityUsesRealConsumer(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	m := &compilerCandidatesAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler("compiler-candidates-real", .9)}}
	require.NoError(t, c.admitImportMemory(context.Background(), m))
	require.Equal(t, 1, m.candidateCalls)
	require.Zero(t, m.tries, "the real candidate capability enters one FIFO node instead of probing through the older single-spec mock API")
	require.Equal(t, int64(64<<20), c.proc.Base.ImportMemory.UnitBytes)
	require.Equal(t, c.proc.Base.ImportMemory.UnitBytes, c.proc.Base.ImportMemory.WorkingSet.UnitBytes())
	require.LessOrEqual(t, len(m.candidates), 27)
	c.endImportMemory()
	m.candidateErr = &rscthrottler.WorkingSetCapacityError{Minimum: 10, Available: 9}
	c.ncpu = 2
	c.scopes[0].NodeInfo.Mcpu = 2
	var retry *importDOPRetryError
	require.ErrorAs(t, c.admitImportMemory(context.Background(), m), &retry)
	require.Equal(t, 1, retry.next, "typed ownerless failure requests a real graph recompile rather than waiting forever")
}

func TestImportMemoryCandidateEquivalencePreservesFullLayout(t *testing.T) {
	left := rscthrottler.WorkingSetSpec{UnitBytes: 64, Inputs: []rscthrottler.InputSlotSpec{{Name: "source", Count: 1, Bytes: 100}}, IOBytes: 200}
	right := rscthrottler.WorkingSetSpec{UnitBytes: 32, Inputs: []rscthrottler.InputSlotSpec{{Name: "source", Count: 1, Bytes: 200}}, IOBytes: 100}
	require.Equal(t, workingLayoutTotal(left), workingLayoutTotal(right))
	require.False(t, sameMemoryWorkingSetEnvelope(left, right), "equal totals have different deltas when W is already shared")
	right = left
	right.UnitBytes = 32
	require.True(t, sameMemoryWorkingSetEnvelope(left, right))
	right.Inputs = []rscthrottler.InputSlotSpec{{Name: "output", Count: 1, Bytes: 100}}
	require.False(t, sameMemoryWorkingSetEnvelope(left, right), "slot ownership is part of envelope equivalence")
	manifest := MemoryManifest{TargetUnit: 64 << 20, Stages: []MemoryStageDescriptor{{Role: MemoryStageCSV, Count: 1, Schema: []types.Type{types.T_json.ToType()}, RetainedFields: 3}}}
	specs, _, err := memoryManifestCandidates(manifest, 64<<20)
	require.NoError(t, err)
	require.Len(t, specs, 1, "the legal singleton dominates all target units")
	require.Equal(t, int64(64<<20), specs[0].UnitBytes)
}

func memoryCompileFixture(t *testing.T, parallel int) *Compile {
	t.Helper()
	proc := process.NewTopProcess(context.Background(), mpool.MustNewZero(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	t.Cleanup(proc.Free)
	c := &Compile{addr: "127.0.0.1:6001", proc: proc, ncpu: parallel, scopes: []*Scope{memorySourceScope("127.0.0.1:6001", parallel, 1)}}
	t.Cleanup(c.endImportMemory)
	return c
}

func TestImportMemoryAdmissionShrinksBeforeWaiting(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	_, large, _, err := c.importMemoryLayout(64 << 20)
	require.NoError(t, err)
	_, small, _, err := c.importMemoryLayout(32 << 20)
	require.NoError(t, err)
	budget := (workingLayoutTotal(large) + workingLayoutTotal(small)) / 2
	m := &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler("compiler-memory-test", .9), available: budget}
	require.NoError(t, c.admitImportMemory(context.Background(), m))
	require.Equal(t, int64(32<<20), c.proc.Base.ImportMemory.UnitBytes)
	require.GreaterOrEqual(t, m.tries, 2)
	require.Zero(t, m.waits, "without another owner, waiting cannot reduce this query's layout")
	c.endImportMemory()
	require.Nil(t, c.proc.Base.ImportMemory)
	// Prepared/retry reuse creates a fresh root instead of retaining old credits.
	require.NoError(t, c.admitImportMemory(context.Background(), m))
	require.NotNil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryAdmissionRequiresDOPRecompile(t *testing.T) {
	c := memoryCompileFixture(t, 16)
	_, minimum, _, err := c.importMemoryLayout(1)
	require.NoError(t, err)
	// The row/parser floor cannot shrink with unit. One local worker fits,
	// while the existing physical graph with 16 workers does not.
	one := memoryCompileFixture(t, 1)
	_, single, _, err := one.importMemoryLayout(1)
	require.NoError(t, err)
	budget := (workingLayoutTotal(minimum) + workingLayoutTotal(single)) / 2
	m := &compilerMemoryAdmission{available: budget}
	err = c.admitImportMemory(context.Background(), m)
	var next *importDOPRetryError
	require.ErrorAs(t, err, &next)
	require.Equal(t, 8, next.next)
	require.Equal(t, 16, c.scopes[0].NodeInfo.Mcpu, "admission cannot mutate the already wired graph")
	require.Equal(t, 1, m.reclaims)
	require.Zero(t, m.waits)
	require.Nil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryNestedBorrowDoesNotCloseParent(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	parent := &process.ImportMemory{UnitBytes: 123}
	c.proc.Base.ImportMemory = parent
	require.NoError(t, c.beginImportMemory())
	c.endImportMemory()
	require.Same(t, parent, c.proc.Base.ImportMemory)
	c.proc.Base.ImportMemory = nil
}

func TestImportMemoryRecompileRebuildsParallelGraph(t *testing.T) {
	client, err := cnclient.NewPipelineClient("", "test", &cnclient.PipelineConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	fixture := newTestCase("select * from R join S on R.uid = S.uid", t)
	t.Cleanup(func() { fixture.proc.Free(); fixture.proc.GetSessionInfo().Buf.Free() })
	ctrl := gomock.NewController(t)
	txnClient, txnOp := newTestTxnClientAndOp(ctrl)
	fixture.proc.Base.TxnClient = txnClient
	fixture.proc.Base.TxnOperator = txnOp
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	fixture.proc.Ctx = ctx
	fixture.proc.ReplaceTopCtx(ctx)
	for _, node := range fixture.pn.GetQuery().Nodes {
		if node.Stats == nil {
			node.Stats = &plan.Stats{}
		}
		node.Stats.BlockNum = 128
		node.Stats.Cost = 1e8
		node.Stats.Outcnt = 1e8
		node.Stats.Rowsize = 64
	}
	original := NewCompile("test", "test", fixture.sql, "", "", fixture.e, fixture.proc, fixture.stmt, false, nil, time.Now())
	t.Cleanup(original.Release)
	original.ncpu = 16
	require.NoError(t, original.Compile(ctx, fixture.pn, testPrint))
	maxParallel := func(c *Compile) int {
		maximum := 0
		require.NoError(t, c.walkLocalImportOperators(func(_ vm.Operator, n int) error { maximum = max(maximum, n); return nil }))
		return maximum
	}
	require.Greater(t, maxParallel(original), 1)
	rebuilt, err := original.recompileImportMemory(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(rebuilt.Release)
	require.Equal(t, 1, maxParallel(rebuilt), "the planner must rebuild receiver/operator arities at the reduced DOP")
	require.Greater(t, maxParallel(original), 1, "the previous graph is not mutated in place")
}

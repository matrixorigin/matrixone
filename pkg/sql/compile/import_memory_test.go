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
	"maps"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func memorySourceScope(addr string, parallel int, fields int32) *Scope {
	op := &external.External{Es: &external.ExternalParam{ExParamConst: external.ExParamConst{Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV}}, Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}, ColumnListLen: 1, Attrs: []plan.ExternAttr{{ColFieldIndex: fields}}}}}
	return &Scope{NodeInfo: engine.Node{Addr: addr, Mcpu: parallel}, RootOp: op}
}

func TestImportRecordGrowthReservesCompleteLayout(t *testing.T) {
	schema := []types.Type{types.T_json.ToType()}
	manifest := MemoryManifest{TargetUnit: 1024, Parallel: 1, NoWait: true,
		Producers: []MemoryProducerDescriptor{{Kind: "csv", Schema: schema}},
		Stages: []MemoryStageDescriptor{
			{Role: MemoryStageCSV, Count: 1, Schema: schema, RetainedFields: 3},
			{Role: MemoryStageOutput, Count: 1, Schema: schema, TableCount: 1, InsertOnly: true},
		},
	}
	m := newCompilerMemoryThrottler(t.Name(), 1, rscthrottler.WithConstLimit(4<<30), rscthrottler.WithMemorySampleForTesting(4<<30, 0))
	meta, err := admitMemoryManifest(context.Background(), m, manifest)
	require.NoError(t, err)
	t.Cleanup(meta.WorkingSet.Close)
	input, err := meta.AcquireInput(context.Background())
	require.NoError(t, err)
	t.Cleanup(input.Release)
	source, workspace := input.Capacity(), meta.WorkingSet.IOBytes()
	budget, err := meta.GrowRecord(context.Background(), csvparser.ReadBlockSize+1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, budget, csvparser.ReadBlockSize+1)
	require.Greater(t, input.Capacity(), source)
	require.Greater(t, meta.WorkingSet.IOBytes(), workspace, "the IO consumer must share the enlarged row proof")
	io, err := meta.AcquireIO(context.Background())
	require.NoError(t, err)
	t.Cleanup(io.Release)
	require.Equal(t, meta.WorkingSet.IOBytes(), io.Capacity())
	source, workspace = input.Capacity(), meta.WorkingSet.IOBytes()
	_, err = meta.GrowRecord(context.Background(), int64(config.LargestEntryLimit))
	require.ErrorContains(t, err, "cannot reserve", "real oversized records must not bypass the physical bound")
	require.Equal(t, source, input.Capacity())
	require.Equal(t, workspace, meta.WorkingSet.IOBytes())
	require.Zero(t, manifest.Producers[0].RecordBytes, "building growth choices must not mutate the compiler graph")
}

func (c *Compile) importMemoryLayout(unit int64) (*process.ImportMemory, rscthrottler.WorkingSetSpec, bool, error) {
	manifest, err := c.localMemoryManifest(unit)
	if err != nil {
		return nil, rscthrottler.WorkingSetSpec{}, false, err
	}
	return memoryManifestLayout(memoryRecordManifest(manifest, min(max(unit, int64(csvparser.ReadBlockSize)), int64(config.LargestEntryLimit))), unit)
}

func TestImportMemoryLayoutCountsLocalParallelStages(t *testing.T) {
	c := memoryCompileFixture(t, 3)
	c.scopes = []*Scope{memorySourceScope(c.addr, 3, 27)}
	c.scopes[0].Proc = c.proc
	meta, spec, controlled, err := c.importMemoryLayout(64 << 20)
	require.NoError(t, err)
	require.True(t, controlled)
	require.Equal(t, uint32(3), spec.Inputs[0].Count)
	require.Zero(t, spec.Inputs[1].Count)
	require.Zero(t, spec.Inputs[2].Count)
	expected, err := external.SourceEnvelopeForFields([]types.Type{types.T_int64.ToType()}, 64<<20, int64(config.LargestEntryLimit), 28)
	require.NoError(t, err)
	require.Equal(t, expected, meta.SourceBytes, "sparse CSV field indexes must be admitted before parser allocation")
	require.Equal(t, expected, spec.Inputs[0].Bytes)
	remote := memorySourceScope("127.0.0.2:6001", 100, 1)
	remote.Magic, remote.Proc = Remote, c.proc
	c.scopes = append(c.scopes, remote)
	_, _, controlled, err = c.importMemoryLayout(64 << 20)
	require.NoError(t, err)
	require.False(t, controlled, "a remote scope keeps the complete graph on its legacy path")
}

func TestImportMemoryLayoutCountsAllLocalSources(t *testing.T) {
	c := memoryCompileFixture(t, 8)
	c.scopes = []*Scope{memorySourceScope(c.addr, 3, 1), memorySourceScope(c.addr, 5, 4)}
	for _, scope := range c.scopes {
		scope.Proc = c.proc
	}
	_, spec, controlled, err := c.importMemoryLayout(32 << 20)
	require.NoError(t, err)
	require.True(t, controlled)
	require.Equal(t, uint32(8), spec.Inputs[0].Count, "reserve every physical source copy before execution")
}

func memoryRetryParallel(err error) int {
	var retry *importDOPRetryError
	if errors.As(err, &retry) {
		return retry.next
	}
	return 0
}

type compilerMemoryAdmission struct {
	realCandidates             bool
	candidateCalls, finalCalls int
	candidates, finalSpecs     []rscthrottler.WorkingSetSpec
	candidateErr, finalErr     error
	real                       rscthrottler.WorkingSetThrottler
	available                  int64
	tries                      int
	failure                    error
	lastUnit                   int64
	triedUnits                 []int64
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
	return rscthrottler.WorkingSetHeadroomSnapshot{Available: m.available}
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
	return m.real.TryReserveWorkingSet(ctx, spec)
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
	require.Greater(t, len(specs), 1, "smaller candidates must reduce both batch and record reservations")
	require.Less(t, workingLayoutTotal(specs[len(specs)-1]), workingLayoutTotal(specs[0]))
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
	m := &compilerMemoryAdmission{real: newCompilerMemoryThrottler("compiler-memory-test", .9), available: budget}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Equal(t, int64(32<<20), c.proc.Base.ImportMemory.UnitBytes)
	require.GreaterOrEqual(t, m.tries, 2)

	c.endImportMemory()
	require.Nil(t, c.proc.Base.ImportMemory)
	// Prepared/retry reuse creates a fresh root instead of retaining old credits.
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
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
	err = c.beginStatementImportMemory(context.Background(), m)
	var next *importDOPRetryError
	require.ErrorAs(t, err, &next)
	require.Equal(t, 8, next.next)
	require.Equal(t, 16, c.scopes[0].NodeInfo.Mcpu, "admission cannot mutate the already wired graph")

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
	original, _ := memoryRecompileFixture(t, 16, nil)
	maxParallel := func(c *Compile) int {
		maximum := 0
		var walk func(*Scope)
		walk = func(scope *Scope) {
			if scope == nil {
				return
			}
			if scope.RootOp != nil {
				maximum = max(maximum, max(1, scope.NodeInfo.Mcpu))
			}
			for _, child := range scope.PreScopes {
				walk(child)
			}
		}
		for _, scope := range c.scopes {
			walk(scope)
		}
		return maximum
	}
	require.Greater(t, maxParallel(original), 1)
	rebuilt, err := original.recompileImportMemory(original.proc.Ctx, 1)
	require.NoError(t, err)
	t.Cleanup(rebuilt.Release)
	require.Equal(t, 1, maxParallel(rebuilt), "rebuild receiver/operator arities at the smaller DOP")
	require.Greater(t, maxParallel(original), 1, "the original graph is immutable")
}

func (c *Compile) supportsBoundedImportKeys() bool {
	_, _, supported, err := c.memoryProducers()
	return supported && err == nil
}

// This fixture selects from the compiler's complete candidate list. Recovery,
// fairness and physical-pressure waiting are tested on the real coordinator.
func (m *compilerMemoryAdmission) selectCandidate(ctx context.Context, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	for _, spec := range specs {
		working, granted, err := m.TryReserveWorkingSet(ctx, spec)
		if err != nil {
			return nil, err
		}
		if granted {
			return working, nil
		}
	}
	return nil, &rscthrottler.WorkingSetCapacityError{Minimum: workingLayoutTotal(specs[len(specs)-1]), Available: m.available}
}
func (m *compilerMemoryAdmission) ReserveWorkingSetCandidates(ctx context.Context, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	m.candidateCalls++
	m.candidates = specs
	if m.candidateErr != nil {
		return nil, m.candidateErr
	}
	if m.realCandidates {
		return m.real.ReserveWorkingSetCandidates(ctx, specs)
	}
	return m.selectCandidate(ctx, specs)
}
func (m *compilerMemoryAdmission) ReserveWorkingSetCandidatesUntilFit(ctx context.Context, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	m.finalCalls++
	m.finalSpecs = specs
	if m.finalErr != nil {
		return nil, m.finalErr
	}
	if m.realCandidates {
		return m.real.ReserveWorkingSetCandidatesUntilFit(ctx, specs)
	}
	return m.selectCandidate(ctx, specs)
}

// The existing public constructor keeps its RSCThrottler return signature.
// Tests that exercise import admission explicitly require its full contract.
func newCompilerMemoryThrottler(name string, rate float64, options ...rscthrottler.MemThrottlerOption) rscthrottler.WorkingSetThrottler {
	options = append([]rscthrottler.MemThrottlerOption{rscthrottler.WithMemorySampleForTesting(math.MaxInt64, 0)}, options...)
	return rscthrottler.NewMemThrottler(name, rate, options...).(rscthrottler.WorkingSetThrottler)
}

type memoryLabelRecordingEngine struct {
	engine.Engine
	seen []map[string]string
}

func (e *memoryLabelRecordingEngine) Nodes(internal bool, tenant, user string, labels map[string]string) (engine.Nodes, error) {
	e.seen = append(e.seen, maps.Clone(labels))
	return e.Engine.Nodes(internal, tenant, user, labels)
}

func memoryRecompileFixture(t *testing.T, parallel int, labels map[string]string) (*Compile, *memoryLabelRecordingEngine) {
	t.Helper()
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
	recording := &memoryLabelRecordingEngine{Engine: fixture.e}
	original := NewCompile("test", "test", fixture.sql, "", "", recording, fixture.proc, fixture.stmt, false, labels, time.Now())
	t.Cleanup(func() { original.SetIsPrepare(false); original.Release() })
	original.ncpu = parallel
	require.NoError(t, original.Compile(ctx, fixture.pn, testPrint))
	return original, recording
}

// Retiring the first retry is the real compile2.go path before a second DOP
// recompile. Every attempt must preserve the caller's and original's labels.
func TestImportMemoryDOPRetriesPreserveCNLabelRouting(t *testing.T) {
	labels := map[string]string{"workload": "isolated-import"}
	original, recording := memoryRecompileFixture(t, 4, labels)
	original.SetIsPrepare(true)
	for _, parallel := range []int{2, 1} {
		retry, err := original.recompileImportMemory(original.proc.Ctx, parallel)
		require.NoError(t, err)
		func() {
			defer retry.Release()
			require.True(t, retry.importMemoryPrepared)
			require.False(t, retry.isPrepare, "a retry keeps ordinary pool ownership")
			_, err = retry.getCNList()
			require.NoError(t, err)
			require.Equal(t, labels, recording.seen[len(recording.seen)-1])
		}()
		require.Equal(t, map[string]string{"workload": "isolated-import"}, labels)
		require.Equal(t, labels, original.cnLabel, "retiring either attempt preserves the origin routing labels")
	}
}

// The actual admission transition can require 4 -> 2 -> 1: this is not a
// synthetic extra retry beyond the new compiler's supported public behavior.
func TestImportMemoryAdmissionCanRequireTwoDOPRecompiles(t *testing.T) {
	one := memoryCompileFixture(t, 1)
	two := memoryCompileFixture(t, 2)
	_, oneMin, _, err := one.importMemoryLayout(1)
	require.NoError(t, err)
	_, twoMin, _, err := two.importMemoryLayout(1)
	require.NoError(t, err)
	require.Greater(t, workingLayoutTotal(twoMin), workingLayoutTotal(oneMin))
	budget := (workingLayoutTotal(oneMin) + workingLayoutTotal(twoMin)) / 2
	for _, parallel := range []int{4, 2} {
		c := memoryCompileFixture(t, parallel)
		m := &compilerMemoryAdmission{available: budget}
		err := c.beginStatementImportMemory(context.Background(), m)
		var retry *importDOPRetryError
		require.ErrorAs(t, err, &retry)
		require.Equal(t, parallel/2, retry.next)
		require.Nil(t, c.importMemory)
		t.Logf("DOP %d minimum cannot fit budget%d; next=%d", parallel, budget, retry.next)
	}
	// The single-worker minimum does fit that same budget by construction.
	require.LessOrEqual(t, workingLayoutTotal(oneMin), budget)
}

func TestCompileOwnsRoutingLabelsAcrossReleaseAndReuse(t *testing.T) {
	fixture := memoryCompileFixture(t, 1)
	labels := map[string]string{"workload": "original"}
	first := NewCompile("", "", "", "", "", nil, fixture.proc, nil, false, labels, time.Now())
	labels["workload"] = "connection-change"
	require.Equal(t, "original", first.cnLabel["workload"], "caller mutation must not change an active attempt")
	first.cnLabel["only-attempt"] = "private"
	require.NotContains(t, labels, "only-attempt")
	first.Release()
	require.Equal(t, map[string]string{"workload": "connection-change"}, labels)
	second := NewCompile("", "", "", "", "", nil, fixture.proc, nil, false, labels, time.Now())
	require.Equal(t, labels, second.cnLabel)
	require.NotContains(t, second.cnLabel, "only-attempt", "pool reuse must not inherit retired routing metadata")
	second.Release()
	require.Equal(t, "connection-change", labels["workload"])
	withoutLabels := NewCompile("", "", "", "", "", nil, fixture.proc, nil, false, nil, time.Now())
	require.Empty(t, withoutLabels.cnLabel)
	withoutLabels.Release()
}

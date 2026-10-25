// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// This provider applies the injected real byte budget to the whole candidate
// list, then uses an actual ticket. A direct old-policy call demonstrates that
// a smaller unit really could have succeeded before the D33 preference.
type staticTargetAdmission struct {
	*compilerMemoryAdmission
	limit        int64
	reserveCalls int
	selected     []rscthrottler.WorkingSetSpec
	onHeadroom   func()
}

func (m *staticTargetAdmission) WorkingSetHeadroom() rscthrottler.WorkingSetHeadroomSnapshot {
	if m.onHeadroom != nil {
		m.onHeadroom()
	}
	return rscthrottler.WorkingSetHeadroomSnapshot{Limit: m.limit, Available: m.available, HasOwners: m.owners}
}

func (m *staticTargetAdmission) ReserveWorkingSetCandidates(ctx context.Context, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	m.reserveCalls++
	m.selected = specs
	for _, spec := range specs {
		if workingLayoutTotal(spec) <= m.available {
			return rscthrottler.ReserveWorkingSet(ctx, m.real, spec)
		}
	}
	return nil, &rscthrottler.WorkingSetCapacityError{Minimum: workingLayoutTotal(specs[len(specs)-1]), Available: m.available}
}

func TestImportMemoryTargetImpossiblePrefersDOPBeforeFittingSmallerUnit(t *testing.T) {
	c := memoryCompileFixture(t, 2)
	manifest, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	specs, _, err := memoryManifestCandidates(manifest, 64<<20)
	require.NoError(t, err)
	require.Greater(t, len(specs), 1)
	target, minimum := workingLayoutTotal(specs[0]), workingLayoutTotal(specs[len(specs)-1])
	require.Greater(t, target, minimum)
	budget := minimum + (target-minimum)/2
	m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: budget}, limit: budget}
	old, err := m.ReserveWorkingSetCandidates(context.Background(), specs)
	require.NoError(t, err)
	t.Cleanup(old.Close)
	require.Less(t, old.UnitBytes(), specs[0].UnitBytes, "this is not the already-covered all-minima-impossible case")
	old.Close()
	m.reserveCalls = 0
	err = c.beginStatementImportMemory(context.Background(), m)
	var retry *importDOPRetryError
	require.ErrorAs(t, err, &retry)
	require.Equal(t, 1, retry.next)
	require.Zero(t, m.reserveCalls, "the static target proof must request recompile before any ticket")
	require.Nil(t, c.proc.Base.ImportMemory)
	require.Equal(t, 2, c.ncpu)
	require.Equal(t, 2, c.scopes[0].NodeInfo.Mcpu, "the old graph stays wired until actual recompile")
}

func TestImportMemoryTargetLimitBoundaryAndUnknownProvider(t *testing.T) {
	for _, test := range []struct {
		name  string
		delta int64
		known bool
		retry bool
	}{
		{name: "target_exceeds_limit_by_one", delta: -1, known: true, retry: true},
		{name: "target_equals_limit", known: true},
		{name: "limit_exceeds_target_by_one", delta: 1, known: true},
		{name: "unknown_zero_limit"},
		{name: "unknown_negative_limit", delta: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := memoryCompileFixture(t, 2)
			_, target, _, err := c.importMemoryLayout(64 << 20)
			require.NoError(t, err)
			total := workingLayoutTotal(target)
			limit, available := test.delta, total
			if test.known {
				limit = total + test.delta
				available = min(total, limit)
			}
			m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: available}, limit: limit}
			err = c.beginStatementImportMemory(context.Background(), m)
			if test.retry {
				require.Equal(t, 1, memoryRetryParallel(err))
				require.Zero(t, m.reserveCalls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, m.reserveCalls)
				require.Equal(t, int64(64<<20), c.proc.Base.ImportMemory.UnitBytes)
			}
		})
	}
}

func TestImportMemoryTargetStaticFitKeepsActiveOwnerSmallerCandidate(t *testing.T) {
	c := memoryCompileFixture(t, 2)
	manifest, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	specs, _, err := memoryManifestCandidates(manifest, 64<<20)
	require.NoError(t, err)
	target, minimum := workingLayoutTotal(specs[0]), workingLayoutTotal(specs[len(specs)-1])
	available := minimum + (target-minimum)/2
	m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{
		real: rscthrottler.NewMemThrottler(t.Name(), .9), available: available, owners: true,
	}, limit: target}
	require.NoError(t, c.beginStatementImportMemory(context.Background(), m))
	require.Equal(t, 1, m.reserveCalls)
	require.Equal(t, len(specs), len(m.selected), "transient owner pressure must keep all candidate units")
	require.Less(t, c.proc.Base.ImportMemory.UnitBytes, int64(64<<20))
	require.Equal(t, 2, c.ncpu)
}

func TestImportMemoryTargetRefreshCancellationWinsOverRetry(t *testing.T) {
	c := memoryCompileFixture(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{}, limit: 1, onHeadroom: cancel}
	err := c.beginStatementImportMemory(ctx, m)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, memoryRetryParallel(err))
	require.Zero(t, m.reserveCalls)
	require.Nil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryTargetTotalSaturatesInsteadOfWrapping(t *testing.T) {
	for _, spec := range []rscthrottler.WorkingSetSpec{
		{Inputs: []rscthrottler.InputSlotSpec{{Count: 2, Bytes: math.MaxInt64}}},
		{Inputs: []rscthrottler.InputSlotSpec{{Count: 1, Bytes: math.MaxInt64}}, IOBytes: 1},
	} {
		require.Equal(t, int64(math.MaxInt64), workingLayoutTotal(spec), "overflow cannot make an impossible target look small")
	}
}

func TestImportMemoryPublicNarrowTargetFitPreservesPlanAndDOP(t *testing.T) {
	setupMemoryLoadRuntime(t)
	file := filepath.Join(t.TempDir(), "narrow.csv")
	require.NoError(t, os.WriteFile(file, []byte("1|2|3|4\n"), 0600))
	cc, ctrl := newMemoryLoadCatalog(t, true)
	// This public control represents an ordinary autocommit statement.
	cc.proc.Base.TxnOperator = &importMemoryTxnOptions{TxnOperator: cc.proc.Base.TxnOperator, options: txn.TxnOptions{Autocommit: true}}
	// LOCAL/FALSE uses its real public planner fallback; no stats, write flags,
	// source schema or threshold are injected to obtain the physical graph.
	sql := fmt.Sprintf("load data local infile '%s' into table memory_narrow_load fields terminated by '|' parallel 'false'", file)
	c := compileMemoryLoad(t, cc, ctrl, "create table memory_narrow_load (id bigint primary key, a bigint, b bigint, c bigint)", sql, 4)
	ctx := c.proc.Ctx
	require.True(t, c.supportsBoundedImportKeys())
	before, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	// Parallel LOAD writes through independent sibling scopes, each with
	// Mcpu=1. Count the actual writers and their feeding dispatch instead of
	// confusing one scope's copy count with the width of the whole pipeline.
	writers, fanout := 0, 0
	seen := make(map[*Scope]bool)
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if seen[scope] {
			return
		}
		seen[scope] = true
		for _, child := range scope.PreScopes {
			walk(child)
		}
		require.NoError(t, vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			switch op := op.(type) {
			case *multi_update.MultiUpdate:
				if op.Action == multi_update.UpdateWriteS3 {
					writers++
				}
			case *insert.Insert:
				if op.ToWriteS3 {
					writers++
				}
			case *dispatch.Dispatch:
				fanout = max(fanout, len(op.LocalRegs))
			}
			return nil
		}))
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
	if writers != 4 || fanout != 4 {
		for i, node := range c.pn.GetQuery().Nodes {
			t.Logf("public node%d type=%v stats=%+v", i, node.NodeType, node.Stats)
		}
	}
	require.Equal(t, 4, writers, "the public narrow control must actually retain four physical S3 writers")
	require.Equal(t, 4, fanout, "the source dispatch must feed those four writer scopes")
	specs, _, err := memoryManifestCandidates(before, 64<<20)
	require.NoError(t, err)
	require.NotEmpty(t, specs)
	require.LessOrEqual(t, workingLayoutTotal(specs[0]), int64(22<<30))
	m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{
		real: rscthrottler.NewMemThrottler(t.Name(), .9), available: 22 << 30,
	}, limit: 22 << 30}
	require.NoError(t, c.beginStatementImportMemory(ctx, m))
	require.NotNil(t, c.importMemory)
	require.Equal(t, 1, m.reserveCalls)
	require.Equal(t, int64(64<<20), c.importMemory.UnitBytes)
	require.Equal(t, 4, c.ncpu)
	after, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	require.Equal(t, before, after, "a statically fitting public narrow LOAD keeps its full physical DAG")
}

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type memoryLabelRecordingEngine struct {
	engine.Engine
	seen []map[string]string
}

func (e *memoryLabelRecordingEngine) Nodes(internal bool, tenant, user string, labels map[string]string) (engine.Nodes, error) {
	e.seen = append(e.seen, maps.Clone(labels))
	return e.Engine.Nodes(internal, tenant, user, labels)
}

// Retiring the first retry is the real compile2.go path before a second DOP
// recompile. Every attempt must preserve the caller's and original's labels.
func TestImportMemoryDOPRetriesPreserveCNLabelRouting(t *testing.T) {
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
	labels := map[string]string{"workload": "isolated-import"}
	want := maps.Clone(labels)
	recording := &memoryLabelRecordingEngine{Engine: fixture.e}
	original := NewCompile("test", "test", fixture.sql, "", "", recording, fixture.proc, fixture.stmt, false, labels, time.Now())
	t.Cleanup(func() { original.SetIsPrepare(false); original.Release() })
	original.ncpu = 4
	require.NoError(t, original.Compile(ctx, fixture.pn, testPrint))
	original.SetIsPrepare(true)
	first, err := original.recompileImportMemory(ctx, 2)
	require.NoError(t, err)
	require.True(t, first.importMemoryPrepared)
	require.False(t, first.isPrepare, "retry must retain ordinary pool ownership")
	first.Release()
	require.Equal(t, want, labels, "releasing an attempt must preserve connection-owned labels")
	afterRelease := maps.Clone(original.cnLabel)
	second, err := original.recompileImportMemory(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(second.Release)
	require.True(t, second.importMemoryPrepared, "a second retry must keep prepared-origin legacy eligibility")
	// Force observation at the real engine boundary even if this tiny fixture's
	// generated plan happened not to request more than the origin CN.
	_, err = second.getCNList()
	require.NoError(t, err)
	t.Logf("labels after first retry release: %v; second retry engine.Nodes labels: %v", afterRelease, recording.seen[len(recording.seen)-1])
	require.Equal(t, want, afterRelease, "retiring one attempt must preserve the original statement's routing labels")
	require.Equal(t, want, recording.seen[len(recording.seen)-1], "second DOP retry must not widen the eligible CN set")
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
		err := c.admitImportMemory(context.Background(), m)
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

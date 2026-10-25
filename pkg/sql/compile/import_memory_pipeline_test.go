// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestImportPipelineMetadataCapacityBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 2, 17} {
		for _, parallel := range []int{1, 4, 16} {
			manifest := MemoryManifest{Parallel: parallel, Producers: make([]MemoryProducerDescriptor, n)}
			capacity, ok := memoryPipelineChannelCapacity(manifest)
			require.True(t, ok)
			require.Equal(t, uint64(max(shuffleChannelBufferSize, n*parallel)), capacity)
		}
	}
	_, ok := memoryPipelineChannelCapacity(MemoryManifest{Parallel: math.MaxInt, Producers: make([]MemoryProducerDescriptor, 2)})
	require.False(t, ok)
}

func TestImportPipelineMetadataFitsFinalParallelGraph(t *testing.T) {
	c := memoryCompileFixture(t, 4)
	reg := process.NewPipelineEdge(shuffleChannelBufferSize, 1)
	child := memorySourceScope(c.addr, 4, 1)
	child.Proc = c.proc.NewContextChildProc(0)
	child.RootOp.(*external.External).Es.Filter = &external.FilterParam{}
	child.RootOp.(*external.External).Es.Fileparam = &external.ExFileparam{}
	root := connector.NewArgument().WithReg(reg)
	defer root.Release()
	root.AppendChild(child.RootOp)
	child.RootOp = root
	parent := &Scope{NodeInfo: engine.Node{Addr: c.addr, Mcpu: 1}, Proc: c.proc.NewNoContextChildProc(0), PreScopes: []*Scope{child}}
	parent.Proc.Reg.MergeReceivers = []*process.WaitRegister{reg}
	parent.RootOp = merge.NewArgument()
	defer parent.RootOp.Release()
	c.scopes = []*Scope{parent}
	manifest, err := c.localMemoryManifest(64)
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Producers)
	receivers := 0
	for _, desc := range manifest.Producers {
		if desc.Kind == "pass" {
			receivers += int(desc.Receivers)
		}
	}
	require.Equal(t, 1, receivers, "receiver metadata is represented independently of the sending CN/spool stage")
	capacity, ok := memoryPipelineChannelCapacity(manifest)
	require.True(t, ok)
	fake, actuals := newParallelScope(child)
	t.Cleanup(func() { fake.release() })
	require.Len(t, actuals, 4)
	for _, actual := range actuals {
		edge := actual.RootOp.(*connector.Connector).Reg
		require.LessOrEqual(t, uint64(cap(edge.Ch2)), capacity)
	}
	// The complete derived promise includes edge+receiver+registry metadata in
	// addition to spool slots. This remains true after a fresh DOP recompilation.
	for _, parallel := range []int{4, 1} {
		manifest.Parallel = parallel
		bound, valid := process.PipelineEdgeMetadataBound(capacity)
		require.True(t, valid)
		_, spec, controlled, err := memoryManifestLayout(manifest, 64)
		require.NoError(t, err)
		require.True(t, controlled)
		var charged int64
		for _, slot := range spec.Inputs {
			if slot.Name != "csv" && slot.Name != "output" && slot.Name != "join-input" {
				charged += int64(slot.Count) * slot.Bytes
			}
		}
		require.GreaterOrEqual(t, charged, bound)
	}
	// Oversized custom embedding channels cannot enter partial admission.
	child.PreScopes = nil // Return to the pre-parallel planner graph for this guard.
	root.Reg = process.NewPipelineEdge(int(capacity)+1, 1)
	manifest, err = c.localMemoryManifest(64)
	require.NoError(t, err)
	require.Empty(t, manifest.Producers)

}

func TestImportPipelineMetadataCoversDispatchReceiverFanout(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	source := memorySourceScope(c.addr, 1, 1)
	op := dispatch.NewArgument()
	defer op.Release()
	op.FuncId = dispatch.SendToAllLocalFunc
	for range 9 {
		op.LocalRegs = append(op.LocalRegs, process.NewPipelineEdge(33, 1))
	}
	op.AppendChild(source.RootOp)
	source.RootOp = op
	c.scopes = []*Scope{source}
	// #producers*parallel is smaller than a custom channel of 33; the before-
	// Open eligibility guard selects whole-query legacy rather than undercharge.
	manifest, err := c.localMemoryManifest(64)
	require.NoError(t, err)
	require.Empty(t, manifest.Producers)
	for i := range op.LocalRegs {
		op.LocalRegs[i] = process.NewPipelineEdge(32, 1)
	}
	manifest, err = c.localMemoryManifest(64)
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Producers)
	_, _, controlled, err := memoryManifestLayout(manifest, 64)
	require.NoError(t, err)
	require.True(t, controlled)
}

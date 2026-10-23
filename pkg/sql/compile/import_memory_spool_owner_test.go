// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestImportMemoryRetiresAbandonedSpoolsBeforeDetaching(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	source := batch.NewWithSize(1)
	t.Cleanup(func() { source.Clean(c.proc.Mp()) })
	source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(19), false, c.proc.Mp()))
	source.SetRowCount(1)
	baseline := c.proc.Mp().AllocatedBytes()
	c.attachImportMemory(&process.ImportMemory{UnitBytes: 64})
	sp := pSpool.InitMyPipelineSpool(c.proc.Mp(), 1)
	t.Cleanup(sp.CloseAfterReadersStopped)
	c.proc.RegisterPipelineSpool(sp)
	_, err := sp.SendBatch(context.Background(), 0, source, nil)
	require.NoError(t, err)
	require.Greater(t, c.proc.Mp().AllocatedBytes(), baseline)
	detached := false
	c.importMemoryDetach = func() {
		detached = true
		require.Equal(t, baseline, c.proc.Mp().AllocatedBytes(), "no abandoned native clone may outlive the complete producer promise")
	}
	// The attempt has joined all senders/readers without receiving End.
	c.endImportMemory()
	require.True(t, detached)
	require.Nil(t, c.proc.Base.ImportMemory)
	c.endImportMemory()
	require.Equal(t, baseline, c.proc.Mp().AllocatedBytes())
}

func TestJoinedLegacySpoolCleanupPreservesUnownedControlledBase(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	source := batch.NewWithSize(1)
	t.Cleanup(func() { source.Clean(c.proc.Mp()) })
	source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(31), false, c.proc.Mp()))
	source.SetRowCount(1)
	baseline := c.proc.Mp().AllocatedBytes()
	local := pSpool.InitMyPipelineSpool(c.proc.Mp(), 1)
	t.Cleanup(local.CloseAfterReadersStopped)
	c.proc.RegisterPipelineSpool(local)
	_, err := local.SendBatch(context.Background(), 0, source, nil)
	require.NoError(t, err)
	localBytes := c.proc.Mp().AllocatedBytes() - baseline
	other := &process.Process{Base: &process.BaseProcess{ImportMemory: &process.ImportMemory{UnitBytes: 64}}}
	borrowed := pSpool.InitMyPipelineSpool(c.proc.Mp(), 1)
	t.Cleanup(borrowed.CloseAfterReadersStopped)
	other.RegisterPipelineSpool(borrowed)
	c.scopes = append(c.scopes, &Scope{Proc: other})
	_, err = borrowed.SendBatch(context.Background(), 0, source, nil)
	require.NoError(t, err)
	before := c.proc.Mp().AllocatedBytes()
	c.endImportMemory()
	require.Equal(t, before, c.proc.Mp().AllocatedBytes(), "an inactive Compile cleanup is not the joined legacy attempt owner")
	c.closePipelineSpoolsAfterReadersStopped()
	require.Equal(t, before-localBytes, c.proc.Mp().AllocatedBytes(), "check the ticket guard on every Base, including a scope's distinct Base")
	other.Base.ClosePipelineSpoolsAfterReadersStopped()
	require.Equal(t, baseline, c.proc.Mp().AllocatedBytes())
}

// A receive-only CN can hold a direct remote frame without a local sending
// spool. Its edge must be drained before the attached ticket detaches.
func TestImportMemoryRetiresDirectRemoteEdgeBeforeDetaching(t *testing.T) {
	c := memoryCompileFixture(t, 1)
	source := batch.NewWithSize(1)
	t.Cleanup(func() { source.Clean(c.proc.Mp()) })
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(41), false, c.proc.Mp()))
	source.SetRowCount(1)
	baseline := c.proc.Mp().AllocatedBytes()
	c.attachImportMemory(&process.ImportMemory{UnitBytes: 64})
	edge := process.NewPipelineEdge(1, 1)
	c.proc.Reg.MergeReceivers = []*process.WaitRegister{edge}
	direct, err := source.Dup(c.proc.Mp())
	require.NoError(t, err)
	require.True(t, edge.SendDataDirect(context.Background(), direct, c.proc.Mp()))
	cause := errors.New("remote failed after accepted data")
	_, ok := edge.PublishTerminal(process.NewErrorSignal(cause))
	require.True(t, ok)
	require.Greater(t, c.proc.Mp().AllocatedBytes(), baseline)
	c.importMemoryDetach = func() { require.Equal(t, baseline, c.proc.Mp().AllocatedBytes()); require.Empty(t, edge.Ch2) }
	c.endImportMemory()
	require.ErrorIs(t, edge.Err(), cause, "drain must retain the settled terminal cause")
	c.endImportMemory()
	require.Equal(t, baseline, c.proc.Mp().AllocatedBytes())
}

func TestImportMemorySavedCurrentSurvivesCleanupPanicUntilJoinedRetirement(t *testing.T) {
	for _, spooled := range []bool{false, true} {
		t.Run(fmt.Sprintf("spool_%v", spooled), func(t *testing.T) {
			c := memoryCompileFixture(t, 1)
			c.proc.BuildPipelineContext(context.Background())
			source := batch.NewWithSize(1)
			t.Cleanup(func() { source.Clean(c.proc.Mp()) })
			source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(43), false, c.proc.Mp()))
			source.SetRowCount(1)
			baseline := c.proc.Mp().AllocatedBytes()
			edge := process.NewPipelineEdge(1, 1)
			c.proc.Reg.MergeReceivers = []*process.WaitRegister{edge}
			receiver := merge.NewArgument()
			defer receiver.Release()
			panicOp := &cancelJoinOperator{MockOperator: colexec.NewMockOperator(), cleanupPanic: true}
			root := colexec.NewMockOperator()
			root.AppendChild(panicOp)
			root.AppendChild(receiver)
			c.scopes = []*Scope{{Proc: c.proc, RootOp: root}}
			c.attachImportMemory(&process.ImportMemory{UnitBytes: 64})
			var sp *pSpool.PipelineSpool
			send := func() {
				if spooled {
					if sp == nil {
						sp = pSpool.InitMyPipelineSpool(c.proc.Mp(), 1)
						c.proc.RegisterPipelineSpool(sp, edge)
					}
					_, err := sp.SendBatch(context.Background(), 0, source, nil)
					require.NoError(t, err)
					require.True(t, edge.SendData(context.Background(), sp, 0))
				} else {
					direct, err := source.Dup(c.proc.Mp())
					require.NoError(t, err)
					require.True(t, edge.SendDataDirect(context.Background(), direct, c.proc.Mp()))
				}
			}
			require.NoError(t, receiver.Prepare(c.proc))
			send()
			held, err := receiver.Call(c.proc)
			require.NoError(t, err)
			require.Equal(t, int64(43), vector.MustFixedColNoTypeCheck[int64](held.Batch.Vecs[0])[0])
			send() // A pending frame coexists with the saved current frame.
			primary := errors.New("execution failure before cleanup")
			require.ErrorIs(t, cleanupScopePipeline(pipeline.NewMerge(root), c.proc, false, primary), primary)
			require.Equal(t, int64(43), vector.MustFixedColNoTypeCheck[int64](held.Batch.Vecs[0])[0], "the aborted cleanup never reached the receiver")
			require.Greater(t, c.proc.Mp().AllocatedBytes(), baseline)
			c.importMemoryDetach = func() { require.Equal(t, baseline, c.proc.Mp().AllocatedBytes()) }
			c.endImportMemory()
			require.Empty(t, edge.Ch2)
			require.Equal(t, baseline, c.proc.Mp().AllocatedBytes())
			// A fresh prepared generation registers its own current owner. Old cleanup
			// has neither a stale current batch nor a callback into this generation.
			c.proc.BuildPipelineContext(context.Background())
			edge.ResetTerminalStateForReuse()
			c.attachImportMemory(&process.ImportMemory{UnitBytes: 64})
			require.NoError(t, receiver.Prepare(c.proc))
			spooled = false
			send()
			held, err = receiver.Call(c.proc)
			require.NoError(t, err)
			require.Equal(t, int64(43), vector.MustFixedColNoTypeCheck[int64](held.Batch.Vecs[0])[0])
			c.endImportMemory()
			c.endImportMemory()
			require.Equal(t, baseline, c.proc.Mp().AllocatedBytes())
		})
	}
}

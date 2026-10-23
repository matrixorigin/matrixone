// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
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

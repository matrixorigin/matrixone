// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package process

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPipelineSpoolAttemptRegistryOwnsOnlyItsGeneration(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	source := batch.NewWithSize(1)
	t.Cleanup(func() { source.Clean(sourceMP) })
	source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(7), false, sourceMP))
	source.SetRowCount(1)
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	base := &BaseProcess{ImportMemory: &ImportMemory{UnitBytes: 64}}
	t.Cleanup(base.ClosePipelineSpoolsAfterReadersStopped)
	proc := &Process{Base: base}
	for range 2 {
		var wg sync.WaitGroup
		for range 2 {
			sp := pSpool.InitMyPipelineSpool(mp, 1)
			_, err := sp.SendBatch(context.Background(), 0, source, nil)
			require.NoError(t, err)
			wg.Add(1)
			go func() {
				defer wg.Done()
				proc.RegisterPipelineSpool(sp)
			}()
		}
		wg.Wait()
		require.Positive(t, mp.AllocatedBytes())
		base.ClosePipelineSpoolsAfterReadersStopped()
		require.Empty(t, base.pipelineSpools.spools)
		require.Zero(t, mp.AllocatedBytes())
		base.ClosePipelineSpoolsAfterReadersStopped()
	}
	// The final attempt owner also reclaims legacy spools. Their cache policy
	// remains independent of the presence of a working-set ticket.
	base.ImportMemory = nil
	legacy := pSpool.InitMyPipelineSpool(mp, 1)
	t.Cleanup(legacy.CloseAfterReadersStopped)
	proc.RegisterPipelineSpool(legacy)
	_, err := legacy.SendBatch(context.Background(), 0, source, nil)
	require.NoError(t, err)
	base.ClosePipelineSpoolsAfterReadersStopped()
	require.Zero(t, mp.AllocatedBytes())
	legacy.CloseAfterReadersStopped()
	require.Zero(t, mp.AllocatedBytes())
}

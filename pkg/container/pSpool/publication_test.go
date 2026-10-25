// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package pSpool

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPublicationFailureBelongsToSpoolGenerationAndPreservesBorrowers(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	source := spoolOwnerInput(t, sourceMP, 1)
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	sp := InitMyPipelineSpool(mp, 2)
	t.Cleanup(sp.CloseAfterReadersStopped)
	require.False(t, sp.PublicationFailed())
	done, err := sp.SendBatch(context.Background(), SendToAllLocal, source, nil)
	require.NoError(t, err)
	require.False(t, done)
	borrowed, err := sp.ReceiveBatch(0)
	require.NoError(t, err)
	sp.MarkPublicationFailed()
	sp.MarkPublicationFailed()
	require.True(t, sp.PublicationFailed())
	sp.ForceCleanup()
	require.Positive(t, mp.AllocatedBytes())
	require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](borrowed.Vecs[0]))
	sp.ReleaseCurrent(0)
	require.Positive(t, mp.AllocatedBytes(), "unpublished second recipient still owns the shared frame")
	// Both readers have now stopped. The query owner may retire the missing
	// recipient's reference without synthesizing an internal spool End.
	sp.CloseAfterReadersStopped()
	require.Zero(t, mp.AllocatedBytes())
	sp.CloseAfterReadersStopped()
	require.Zero(t, mp.AllocatedBytes())

	// Reusing the same memory pool does not transfer broken-publication mode
	// into the next generation, which keeps its ordinary End/ack protocol.
	next := InitMyPipelineSpool(mp, 1)
	t.Cleanup(next.CloseAfterReadersStopped)
	require.False(t, next.PublicationFailed())
	done, err = next.SendBatch(context.Background(), 0, source, nil)
	require.NoError(t, err)
	require.False(t, done)
	got, err := next.ReceiveBatch(0)
	require.NoError(t, err)
	require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](got.Vecs[0]))
	next.ReleaseCurrent(0)
	done, err = next.SendBatch(context.Background(), 0, nil, nil)
	require.NoError(t, err)
	require.False(t, done)
	got, err = next.ReceiveBatch(0)
	require.NoError(t, err)
	require.Nil(t, got)
	next.Close()
	require.Equal(t, 1, next.drainedReceivers)
	require.Zero(t, mp.AllocatedBytes())
	next.CloseAfterReadersStopped()
	require.Zero(t, mp.AllocatedBytes())
}

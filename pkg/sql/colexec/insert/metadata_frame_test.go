// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package insert

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestControlledInsertMetadataUsesDedicatedFrameCap(t *testing.T) {
	_, op, _ := controlledInsertFixture(t, &controlledTestThrottler{t: t})
	s := op.ctr.controlled
	limit, err := colexec.InsertMetadataFrameLimit(s.unit, 1)
	require.NoError(t, err)
	runtimeLimit, err := s.metadataFrameLimit()
	require.NoError(t, err)
	require.Equal(t, limit, runtimeLimit)
	require.Less(t, limit+1, s.outputSlot.Capacity()/4, "the shared raw output slot must not redefine the INSERT protocol")
	// A plausible file extent inside the old global cap must be rejected at
	// the dedicated protocol boundary, before decoding any vector backing.
	s.spool, err = os.CreateTemp(t.TempDir(), "oversized-insert-frame-*")
	require.NoError(t, err)
	require.NoError(t, os.Remove(s.spool.Name()))
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(limit+1))
	_, err = s.spool.Write(header[:])
	require.NoError(t, err)
	s.written = limit + 1 + 8
	require.NoError(t, s.spool.Truncate(s.written))
	_, err = s.next()
	require.ErrorContains(t, err, "invalid insert metadata frame size")
	require.Zero(t, s.outputMP.AllocatedBytes())
	require.NoError(t, s.close())
}

func TestControlledInsertFixedMetadataPreservesCompleteObject(t *testing.T) {
	_, op, _ := controlledInsertFixture(t, &controlledTestThrottler{t: t})
	s := op.ctr.controlled
	var stats objectio.ObjectStats
	require.NoError(t, objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectName(objectio.NewSegmentid(), 0)))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats, 2))
	require.NoError(t, objectio.SetObjectStatsRowCnt(&stats, objectio.BlockMaxRows+1))
	require.NoError(t, s.spoolStats([]objectio.ObjectStats{stats}))
	out, err := s.next()
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Equal(t, 2, out.Vecs[0].Length())
	require.Equal(t, 1, out.Vecs[1].Length(), "all block infos keep their one complete object stats record")
	decoded := objectio.ObjectStats(out.Vecs[1].GetBytesAt(0))
	require.Equal(t, stats.BlkCnt(), decoded.BlkCnt())
	require.Equal(t, stats.Rows(), decoded.Rows())
	limit, err := s.metadataFrameLimit()
	require.NoError(t, err)
	require.LessOrEqual(t, out.MarshalBinarySize(), limit)
	out, err = s.next()
	require.NoError(t, err)
	require.Nil(t, out)
}

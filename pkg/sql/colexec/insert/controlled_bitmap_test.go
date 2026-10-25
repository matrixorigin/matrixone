// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package insert

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestControlledInsertRetainsBitmapGrantUntilWriterCloses(t *testing.T) {
	credits := &controlledTestThrottler{t: t, grants: 4}
	proc, op, _ := controlledInsertFixture(t, credits)
	s := op.ctr.controlled
	b := batch.NewWithSize(2)
	t.Cleanup(func() { b.Clean(proc.Mp()) })
	b.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp())
	b.Vecs[1] = testutil.MakeVarcharVector([]string{"a", "b"}, []uint64{1}, proc.Mp())
	b.Vecs[1].GetGrouping().Add(0)
	b.SetRowCount(2)
	analyzer := process.NewAnalyzer(0, false, false, "controlled-bitmap")
	require.NoError(t, s.append(proc, analyzer, b))
	bitmap := s.writer.RetainedBitmapBytes()
	require.Positive(t, bitmap)
	require.Equal(t, s.dataMP.AllocatedBytes()+bitmap, credits.balance, "Go bitmap backing remains owned after data allocation reconciliation")
	s.sourceReleased()
	require.Equal(t, s.dataMP.AllocatedBytes()+bitmap, credits.balance)
	require.NoError(t, s.flush(proc, analyzer))
	require.Zero(t, credits.balance)
	require.Zero(t, s.dataMP.AllocatedBytes())
	require.NoError(t, s.append(proc, analyzer, b))
	require.Positive(t, credits.balance)
	require.NoError(t, s.close())
	require.Zero(t, credits.balance)
}

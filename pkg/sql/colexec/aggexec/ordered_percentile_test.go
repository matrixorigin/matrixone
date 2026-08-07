// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestOrderedPercentileExecNumericAndDirection(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	valueVec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5})
	defer valueVec.Free(mp)

	cont, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
		types.T_int64.ToType(), orderedPercentileContinuous)
	require.NoError(t, err)
	require.NoError(t, cont.GroupGrow(1))
	require.NoError(t, cont.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.25"), false), 0))
	require.NoError(t, cont.BulkFill(0, []*vector.Vector{valueVec}))
	result, err := cont.Flush()
	require.NoError(t, err)
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	result[0].Free(mp)
	cont.Free()

	disc, err := makeOrderedPercentileExec(mp, AggIdOfPercentileDisc, false,
		types.T_int64.ToType(), orderedPercentileDiscrete)
	require.NoError(t, err)
	require.NoError(t, disc.GroupGrow(1))
	require.NoError(t, disc.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.5"), true), 0))
	require.NoError(t, disc.BulkFill(0, []*vector.Vector{valueVec}))
	result, err = disc.Flush()
	require.NoError(t, err)
	// DESC makes p=0.5 select the third value from the high end.
	require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	disc.Free()
}

func TestOrderedPercentileExecGroupsNullsAndMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	left, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
		types.T_int64.ToType(), orderedPercentileContinuous)
	require.NoError(t, err)
	right, err := makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false,
		types.T_int64.ToType(), orderedPercentileContinuous)
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))
	config := EncodeOrderedPercentileConfig([]byte("0.5"), false)
	require.NoError(t, left.SetExtraInformation(config, 0))
	require.NoError(t, right.SetExtraInformation(config, 0))

	leftVec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 0, 10, 0})
	leftVec.SetNull(1)
	leftVec.SetNull(3)
	rightVec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{3, 5, 30})
	defer leftVec.Free(mp)
	defer rightVec.Free(mp)
	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 2, GroupNotMatched}, []*vector.Vector{leftVec}))
	require.NoError(t, right.BatchFill(0, []uint64{1, 2, 2}, []*vector.Vector{rightVec}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))

	result, err := left.Flush()
	require.NoError(t, err)
	// Group 1 has [1,3], group 2 has [5,10,30] after NULLs are ignored.
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	require.Equal(t, 10.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 1))
	result[0].Free(mp)
	left.Free()
	right.Free()
}

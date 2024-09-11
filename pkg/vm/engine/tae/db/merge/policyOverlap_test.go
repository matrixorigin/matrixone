// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

const overlapSizeThreshold = common.DefaultMinOsizeQualifiedMB * common.Const1MBytes

func newSortedTestObjectEntry(t *testing.T, v1, v2 int32, size uint32) *catalog.ObjectEntry {
	zm := index.NewZM(types.T_int32, 0)
	index.UpdateZM(zm, types.EncodeInt32(&v1))
	index.UpdateZM(zm, types.EncodeInt32(&v2))
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 2))
	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{
			ObjectStats: *stats,
		},
	}
}

func TestObjOverlap(t *testing.T) {

	// empty policy
	policy := newObjOverlapPolicy()
	objs := policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	policy.resetForTable(nil)

	// no overlap
	entry1 := newSortedTestObjectEntry(t, 1, 2, overlapSizeThreshold)
	entry2 := newSortedTestObjectEntry(t, 3, 4, overlapSizeThreshold)
	require.True(t, policy.onObject(entry1, defaultBasicConfig))
	require.True(t, policy.onObject(entry2, defaultBasicConfig))
	objs = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	policy.resetForTable(nil)

	// overlap
	entry3 := newSortedTestObjectEntry(t, 1, 4, overlapSizeThreshold)
	entry4 := newSortedTestObjectEntry(t, 2, 3, overlapSizeThreshold)
	require.True(t, policy.onObject(entry3, defaultBasicConfig))
	require.True(t, policy.onObject(entry4, defaultBasicConfig))
	objs = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 1, len(objs))
	require.Equal(t, 2, len(objs[0].objs))
	require.Equal(t, TaskHostCN, objs[0].kind)

	policy.resetForTable(nil)

	// entry is not sorted
	entry5 := newTestObjectEntry(t, overlapSizeThreshold, false)
	entry6 := newTestObjectEntry(t, overlapSizeThreshold, false)
	require.False(t, policy.onObject(entry5, defaultBasicConfig))
	require.False(t, policy.onObject(entry6, defaultBasicConfig))
	require.Equal(t, 0, len(policy.objects))
	objs = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	policy.resetForTable(nil)

	// two overlap set:
	// {entry7, entry8}
	// {entry9, entry10, entry11}
	entry7 := newSortedTestObjectEntry(t, 1, 4, overlapSizeThreshold)
	entry8 := newSortedTestObjectEntry(t, 2, 3, overlapSizeThreshold)

	entry9 := newSortedTestObjectEntry(t, 11, 14, overlapSizeThreshold)
	entry10 := newSortedTestObjectEntry(t, 12, 13, overlapSizeThreshold)
	entry11 := newSortedTestObjectEntry(t, 13, 15, overlapSizeThreshold)

	require.True(t, policy.onObject(entry7, defaultBasicConfig))
	require.True(t, policy.onObject(entry8, defaultBasicConfig))
	require.True(t, policy.onObject(entry9, defaultBasicConfig))
	require.True(t, policy.onObject(entry10, defaultBasicConfig))
	require.True(t, policy.onObject(entry11, defaultBasicConfig))

	objs = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 1, len(objs))
	require.Equal(t, 3, len(objs[0].objs))
	require.Equal(t, TaskHostCN, objs[0].kind)

	policy.resetForTable(nil)

	// no enough memory
	entry12 := newSortedTestObjectEntry(t, 1, 4, overlapSizeThreshold)
	entry13 := newSortedTestObjectEntry(t, 2, 3, overlapSizeThreshold)

	require.True(t, policy.onObject(entry12, defaultBasicConfig))
	require.True(t, policy.onObject(entry13, defaultBasicConfig))

	objs = policy.revise(0, 0, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	policy.resetForTable(nil)
}

func TestMinValue(t *testing.T) {
	require.Equal(t, minValue(types.T_bit), 0)
	require.True(t, types.CompareDecimal64(minValue(types.T_decimal64).(types.Decimal64), 0) < 0)
	require.True(t, types.CompareDecimal128(minValue(types.T_decimal128).(types.Decimal128), types.Decimal128{}) < 0)
}

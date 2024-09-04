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
	"github.com/stretchr/testify/require"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

const overlapSizeThreshold = common.DefaultMinOsizeQualifiedMB * common.Const1MBytes

func newTestObjectEntry(size uint32) *catalog.ObjectEntry {
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsOriginSize(stats, size)
	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{ObjectStats: *stats},
	}
}

func newSortedTestObjectEntry(v1, v2 int32, size uint32) *catalog.ObjectEntry {
	zm := index.NewZM(types.T_int32, 0)
	index.UpdateZM(zm, types.EncodeInt32(&v1))
	index.UpdateZM(zm, types.EncodeInt32(&v2))
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsSortKeyZoneMap(stats, zm)
	objectio.SetObjectStatsOriginSize(stats, size)
	entry := &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{
			ObjectStats: *stats,
		},
	}
	return entry
}

func TestObjOverlap(t *testing.T) {

	// empty policy
	policy := newObjOverlapPolicy()
	objs, kind := policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))
	require.Equal(t, TaskHostDN, kind)

	policy.resetForTable(nil)

	// no overlap
	entry1 := newSortedTestObjectEntry(1, 2, overlapSizeThreshold)
	entry2 := newSortedTestObjectEntry(3, 4, overlapSizeThreshold)
	policy.onObject(entry1)
	policy.onObject(entry2)
	objs, kind = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))
	require.Equal(t, TaskHostDN, kind)

	policy.resetForTable(nil)

	// overlap
	entry3 := newSortedTestObjectEntry(1, 4, overlapSizeThreshold)
	entry4 := newSortedTestObjectEntry(2, 3, overlapSizeThreshold)
	policy.onObject(entry3)
	policy.onObject(entry4)
	objs, kind = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 2, len(objs))
	require.Equal(t, TaskHostCN, kind)

	policy.resetForTable(nil)

	// entry is not sorted
	entry5 := newTestObjectEntry(overlapSizeThreshold)
	entry6 := newTestObjectEntry(overlapSizeThreshold)
	policy.onObject(entry5)
	policy.onObject(entry6)
	require.Equal(t, 0, len(policy.objects))
	objs, kind = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))
	require.Equal(t, TaskHostDN, kind)

	policy.resetForTable(nil)

	// two overlap set:
	// {entry7, entry8}
	// {entry9, entry10, entry11}
	entry7 := newSortedTestObjectEntry(1, 4, overlapSizeThreshold)
	entry8 := newSortedTestObjectEntry(2, 3, overlapSizeThreshold)

	entry9 := newSortedTestObjectEntry(11, 14, overlapSizeThreshold)
	entry10 := newSortedTestObjectEntry(12, 13, overlapSizeThreshold)
	entry11 := newSortedTestObjectEntry(13, 15, overlapSizeThreshold)

	policy.onObject(entry7)
	policy.onObject(entry8)
	policy.onObject(entry9)
	policy.onObject(entry10)
	policy.onObject(entry11)

	objs, kind = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 3, len(objs))
	require.Equal(t, TaskHostCN, kind)

	policy.resetForTable(nil)

	// no enough memory
	entry12 := newSortedTestObjectEntry(1, 4, overlapSizeThreshold)
	entry13 := newSortedTestObjectEntry(2, 3, overlapSizeThreshold)

	policy.onObject(entry12)
	policy.onObject(entry13)

	objs, kind = policy.revise(0, 0, defaultBasicConfig)
	require.Equal(t, 0, len(objs))
	require.Equal(t, TaskHostDN, kind)

	policy.resetForTable(nil)
}

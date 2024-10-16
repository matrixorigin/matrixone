// Copyright 2023 Matrix Origin
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

package objectio

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ObjectStats(t *testing.T) {
	// test nil object stats and input data
	require.NotNil(t, SetObjectStatsObjectName(nil, []byte("nil object stats")))
	require.NotNil(t, SetObjectStatsObjectName(NewObjectStats(), nil))

	// test setter and getter methods
	stats := NewObjectStats()
	require.True(t, stats.IsZero())

	objName := BuildObjectName(&types.Uuid{0x1f, 0x2f}, 0)
	require.Nil(t, SetObjectStatsObjectName(stats, objName))
	require.True(t, bytes.Equal(stats.ObjectName(), objName))

	extent := NewExtent(0x1f, 0x2f, 0x3f, 0x4f)
	require.Nil(t, SetObjectStatsExtent(stats, extent))
	require.True(t, bytes.Equal(stats.Extent(), extent))

	blkCnt := uint32(99)
	rowCnt := uint32(98)

	require.Nil(t, SetObjectStatsBlkCnt(stats, blkCnt))
	require.Equal(t, stats.BlkCnt(), blkCnt)

	require.Nil(t, SetObjectStatsRowCnt(stats, rowCnt))
	require.Equal(t, stats.Rows(), rowCnt)

	sortKeyZoneMap := index.BuildZM(types.T_uint8, []byte{0xa, 0xb, 0xc, 0xd})

	require.Nil(t, SetObjectStatsSortKeyZoneMap(stats, sortKeyZoneMap))
	require.True(t, bytes.Equal(stats.SortKeyZoneMap(), sortKeyZoneMap))

	require.True(t, bytes.Equal(stats.ObjectLocation(), BuildLocation(objName, extent, 0, 0)))

	// test set location
	loc := BuildLocation([]byte{0x3f}, []byte{0x7f}, 0, 0)
	require.Nil(t, SetObjectStatsLocation(stats, loc))
	require.True(t, bytes.Equal(stats.ObjectLocation(), loc))

	x := BuildObjectBlockid(objName, 0)
	s := ShortName(x)
	SetObjectStatsShortName(stats, s)
	require.True(t, bytes.Equal(stats.ObjectShortName()[:], s[:]))
}

func TestObjectStats_Clone(t *testing.T) {
	stats := NewObjectStats()
	require.Nil(t, SetObjectStatsRowCnt(stats, 99))

	copied := stats.Clone()
	require.True(t, bytes.Equal(stats.Marshal(), copied.Marshal()))

	require.Nil(t, SetObjectStatsRowCnt(copied, 199))
	require.False(t, bytes.Equal(stats.Marshal(), copied.Marshal()))

	fmt.Println(stats.String())
	fmt.Println(copied.String())
}

func TestObjectStats_Marshal_UnMarshal(t *testing.T) {
	rawBytes := make([]byte, ObjectStatsLen)
	for idx := 0; idx < ObjectStatsLen; idx++ {
		rr := rand.Uint32()
		rawBytes[idx] = types.EncodeUint32(&rr)[0]
	}

	stats := NewObjectStats()
	stats.UnMarshal(rawBytes)

	require.True(t, bytes.Equal(stats.Marshal(), rawBytes))
	fmt.Println(stats.String())
}

func TestObjectStatsOptions(t *testing.T) {
	stats := NewObjectStats()
	require.True(t, stats.IsZero())
	require.False(t, stats.GetAppendable())
	require.False(t, stats.GetCNCreated())
	require.False(t, stats.GetSorted())

	WithCNCreated()(stats)
	require.True(t, stats.GetCNCreated())

	WithSorted()(stats)
	require.True(t, stats.GetSorted())

	WithAppendable()(stats)
	require.True(t, stats.GetAppendable())
}

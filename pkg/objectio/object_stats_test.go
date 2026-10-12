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
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"

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
	require.False(t, stats.GetCNDeleted())
	require.False(t, stats.GetSorted())

	WithCNCreated()(stats)
	require.True(t, stats.GetCNCreated())

	SetObjectStatsCNDeleted(stats, true)
	require.True(t, stats.GetCNDeleted())
	SetObjectStatsCNDeleted(stats, false)
	require.False(t, stats.GetCNDeleted())

	WithSorted()(stats)
	require.True(t, stats.GetSorted())

	WithAppendable()(stats)
	require.True(t, stats.GetAppendable())
}

func TestObjectStats_SetLevel(t *testing.T) {
	tests := []struct {
		name     string
		level    int8
		expected int8
	}{
		{
			name:     "Set level 0",
			level:    0,
			expected: 0,
		},
		{
			name:     "Set level 7",
			level:    7,
			expected: 7,
		},
		{
			name:     "Set negative level (should clamp to 0)",
			level:    -1,
			expected: 0,
		},
		{
			name:     "Set level > 7 (should clamp to 7)",
			level:    8,
			expected: 7,
		},
		{
			name:     "Set level 3",
			level:    3,
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := NewObjectStats()

			// Set some flags to ensure they're preserved
			stats[reservedOffset] = ObjectFlag_Appendable | ObjectFlag_Sorted | ObjectFlag_CNDeleted

			stats.SetLevel(tt.level)

			// Verify the level was set correctly
			got := stats.GetLevel()
			if got != tt.expected {
				t.Errorf("SetLevel() = %v, want %v", got, tt.expected)
			}

			// Verify other flags were preserved
			if !stats.GetAppendable() {
				t.Error("Appendable flag was not preserved")
			}
			if !stats.GetSorted() {
				t.Error("Sorted flag was not preserved")
			}
			if !stats.GetCNDeleted() {
				t.Error("CNDeleted flag was not preserved")
			}
		})
	}
}

// Bounds persisted by the old BOOL vector min/max implementation may describe
// a mixed false/true block as false/false. Every metadata entry point must expose
// a conservative view without changing the shared serialized bytes.
func TestPersistedBoolZoneMapBounds(t *testing.T) {
	malformed := index.BuildZM(types.T_bool, []byte{0})
	malformed[61] = 0 // A truncated max must not add a decoder panic at metadata access.

	for _, tc := range []struct {
		name  string
		zm    ZoneMap
		widen bool
	}{
		{"ambiguous false", index.BuildZM(types.T_bool, []byte{0}), true},
		{"true", index.BuildZM(types.T_bool, []byte{1}), false},
		{"truncated maximum", malformed, false},
		{"uninitialized", index.NewZM(types.T_bool, 0), false},
		{"integer", index.BuildZM(types.T_int8, []byte{0}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			col := ColumnMeta(make([]byte, colMetaLen))
			col.SetZoneMap(tc.zm)
			stats := NewObjectStats()
			require.NoError(t, SetObjectStatsSortKeyZoneMap(stats, tc.zm))
			blockIndex := BuildBlockIndex(1)
			blockIndex.SetBlockCount(1)
			blockIndex.SetBlockMetaPos(0, uint32(blockIndex.Length()), ZoneMapSize)
			area := ZoneMapArea(append(blockIndex, tc.zm...))
			for _, entry := range []struct {
				name string
				read func() ZoneMap
				raw  []byte
			}{
				{"column", col.ZoneMap, col},
				{"object sort key", stats.SortKeyZoneMap, stats[:]},
				{"zone map area", func() ZoneMap { return area.GetZoneMap(0, 0) }, area},
			} {
				t.Run(entry.name, func(t *testing.T) {
					before := bytes.Clone(entry.raw)
					view := entry.read()
					if tc.widen {
						require.False(t, types.DecodeBool(view.GetMinBuf()))
						require.True(t, types.DecodeBool(view.GetMaxBuf()))
						view[0] = 1 // No mutation of shared cached metadata through the copy.
					} else {
						require.Equal(t, tc.zm, view)
						require.Equal(t, float64(0), testing.AllocsPerRun(10, func() { _ = entry.read() }))
					}
					require.Equal(t, before, entry.raw)
				})
			}
		})
	}
}

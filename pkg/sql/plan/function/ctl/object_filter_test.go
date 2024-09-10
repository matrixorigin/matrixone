// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ctl

import (
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestOverlap(t *testing.T) {
	{
		bs := make([]byte, 4)
		zm := index.NewZM(types.T_int32, 0)
		binary.LittleEndian.PutUint32(bs, 0)
		index.UpdateZM(zm, bs)
		binary.LittleEndian.PutUint32(bs, 10)
		index.UpdateZM(zm, bs)

		zm2 := index.NewZM(types.T_int32, 0)
		binary.LittleEndian.PutUint32(bs, 5)
		index.UpdateZM(zm2, bs)
		binary.LittleEndian.PutUint32(bs, 15)
		index.UpdateZM(zm2, bs)

		stats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats1, zm)
		stats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats2, zm2)

		result, _ := NewOverlap(10).Filter([]objectio.ObjectStats{*stats1, *stats2})
		require.Equal(t, 2, len(result))
	}

	{
		bs := make([]byte, 4)
		zm := index.NewZM(types.T_uint32, 0)
		binary.LittleEndian.PutUint32(bs, 0)
		index.UpdateZM(zm, bs)
		binary.LittleEndian.PutUint32(bs, 10)
		index.UpdateZM(zm, bs)

		zm2 := index.NewZM(types.T_uint32, 0)
		binary.LittleEndian.PutUint32(bs, 5)
		index.UpdateZM(zm2, bs)
		binary.LittleEndian.PutUint32(bs, 15)
		index.UpdateZM(zm2, bs)

		stats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats1, zm)
		stats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats2, zm2)

		result, _ := NewOverlap(10).Filter([]objectio.ObjectStats{*stats1, *stats2})
		require.Equal(t, 2, len(result))
	}

	{
		bs := make([]byte, 8)
		zm := index.NewZM(types.T_float64, 0)
		binary.LittleEndian.PutUint32(bs, 0)
		index.UpdateZM(zm, bs)
		binary.LittleEndian.PutUint32(bs, 10)
		index.UpdateZM(zm, bs)

		zm2 := index.NewZM(types.T_float64, 0)
		binary.LittleEndian.PutUint32(bs, 5)
		index.UpdateZM(zm2, bs)
		binary.LittleEndian.PutUint32(bs, 15)
		index.UpdateZM(zm2, bs)

		stats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats1, zm)
		stats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats2, zm2)

		result, _ := NewOverlap(10).Filter([]objectio.ObjectStats{*stats1, *stats2})
		require.Equal(t, 2, len(result))
	}

	{
		bs := make([]byte, 8)
		zm := index.NewZM(types.T_float64, 0)
		binary.LittleEndian.PutUint32(bs, 0)
		index.UpdateZM(zm, bs)
		binary.LittleEndian.PutUint32(bs, 10)
		index.UpdateZM(zm, bs)

		zm2 := index.NewZM(types.T_float64, 0)
		binary.LittleEndian.PutUint32(bs, 2)
		index.UpdateZM(zm2, bs)
		binary.LittleEndian.PutUint32(bs, 5)
		index.UpdateZM(zm2, bs)

		zm3 := index.NewZM(types.T_float64, 0)
		binary.LittleEndian.PutUint32(bs, 9)
		index.UpdateZM(zm3, bs)
		binary.LittleEndian.PutUint32(bs, 15)
		index.UpdateZM(zm3, bs)

		stats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats1, zm)
		stats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats2, zm2)
		stats3 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats3, zm3)

		result, _ := NewOverlap(10).Filter([]objectio.ObjectStats{*stats1, *stats2, *stats3})
		require.Equal(t, 3, len(result))
	}

	{
		bs := make([]byte, 8)
		zm := index.NewZM(types.T_float64, 0)
		binary.LittleEndian.PutUint32(bs, 0)
		index.UpdateZM(zm, bs)
		binary.LittleEndian.PutUint32(bs, 10)
		index.UpdateZM(zm, bs)

		zm2 := index.NewZM(types.T_float64, 0)
		binary.LittleEndian.PutUint32(bs, 2)
		index.UpdateZM(zm2, bs)
		binary.LittleEndian.PutUint32(bs, 5)
		index.UpdateZM(zm2, bs)

		zm3 := index.NewZM(types.T_float64, 0)
		binary.LittleEndian.PutUint32(bs, 11)
		index.UpdateZM(zm3, bs)
		binary.LittleEndian.PutUint32(bs, 15)
		index.UpdateZM(zm3, bs)

		stats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats1, zm)
		stats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats2, zm2)
		stats3 := objectio.NewObjectStats()
		objectio.SetObjectStatsSortKeyZoneMap(stats3, zm3)

		result, _ := NewOverlap(10).Filter([]objectio.ObjectStats{*stats1, *stats2, *stats3})
		require.Equal(t, 2, len(result))
	}
}

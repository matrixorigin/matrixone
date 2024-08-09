// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func getLocation(name ObjectName) Location {
	extent := NewExtent(1, 1, 1, 1)
	return BuildLocation(name, extent, 1, 1)
}

func BenchmarkDecode(b *testing.B) {
	var location Location
	uuid, _ := types.BuildUuid()
	name := BuildObjectName(&uuid, 1)
	b.Run("build", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			location = getLocation(name)
		}
	})
	b.Run("GetName", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			location.Name().SegmentId()
			location.Name().Num()
			location.ID()
		}
	})
	b.Log(location.Name().String())
}

func BenchmarkCheckSame(b *testing.B) {
	uid, _ := types.BuildUuid()
	fname := BuildObjectName(&uid, 0)
	blkID := NewBlockid(&uid, 0, 0)
	b.Run("is-same-obj", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			IsBlockInObject(blkID, &fname)
		}
	})
	var segid Segmentid
	b.Run("is-same-seg", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			IsEmptySegid(&segid)
		}
	})
	var blkid types.Blockid
	b.Run("is-same-blk", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			IsEmptyBlkid(&blkid)
		}
	})
}

func TestLocationSlice_Append(t *testing.T) {
	var s LocationSlice
	location1 := MockLocation(MockObjectName())
	s.AppendLocation(location1)
	require.Equal(t, 1, s.Len())
	require.Equal(t, LocationLen, s.Size())
	require.Equal(t, &location1, s.Get(0))

	location2 := MockLocation(MockObjectName())
	var s2 LocationSlice
	s2.AppendLocation(location1)
	s2.AppendLocation(location2)
	require.Equal(t, 2, s2.Len())
	require.Equal(t, LocationLen*2, s2.Size())
	require.Equal(t, &location1, s2.Get(0))
	require.Equal(t, &location2, s2.Get(1))

	var s3 LocationSlice
	s3.Append(EncodeLocation(location1))
	s3.Append(EncodeLocation(location2))
	require.Equal(t, 2, s3.Len())
	require.Equal(t, LocationLen*2, s3.Size())
	require.Equal(t, &location1, s3.Get(0))
	require.Equal(t, &location2, s3.Get(1))
}

func TestLocationSliceTraverse(t *testing.T) {
	var s LocationSlice
	objectName := MockObjectName()
	for i := uint32(0); i < 1000; i++ {
		location := BuildLocation(objectName, NewExtent(1, i, i, i), i, uint16(i))
		s.AppendLocation(location)
	}

	require.Equal(t, 1000, s.Len())

	for i := 0; i < s.Len(); i++ {
		location := s.Get(i)
		require.Equal(t, uint16(i), location.ID())
		require.Equal(t, uint32(i), location.Rows())
	}
}

func TestBytesToLocationSlice(t *testing.T) {
	bs := make([]byte, 0)
	objectName := MockObjectName()
	for i := uint32(0); i < 1000; i++ {
		location := BuildLocation(objectName, NewExtent(1, i, i, i), i, uint16(i))
		bs = append(bs, EncodeLocation(location)...)
	}

	s := LocationSlice(bs)
	require.Equal(t, 1000, s.Len())

	for i := 0; i < s.Len(); i++ {
		location := s.Get(i)
		require.Equal(t, uint16(i), location.ID())
		require.Equal(t, uint32(i), location.Rows())
	}

}

func TestLocationSlice_Remove(t *testing.T) {
	var s LocationSlice
	objectName := MockObjectName()
	for i := uint32(0); i < 1000; i++ {
		location := BuildLocation(objectName, NewExtent(1, i, i, i), i, uint16(i))
		s.AppendLocation(location)
	}

	curr := 0
	for i := 0; i < 10; i++ {
		location := s.Get(i)
		if location.ID() == uint16(0) {
			// remove the first element
			continue
		}
		s.Set(curr, location)
		curr++
	}

	s = s.Slice(0, curr)
	require.Equal(t, 9, s.Len())
	for i := 0; i < 9; i++ {
		require.Equal(t, uint16(i+1), s.Get(i).ID())
	}
}

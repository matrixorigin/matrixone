// Copyright 2024 - 2022 Matrix Origin
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

package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPacker(t *testing.T) {
	packer := NewPacker()
	defer packer.Close()
	for i := 0; i < 65536; i++ {
		packer.EncodeInt64(int64(i))
	}
	bs := packer.Bytes()
	if len(bs) != 261887 {
		t.Fatalf("got %v", len(bs))
	}
}

func TestClosedPackerIsOK(t *testing.T) {
	packer := NewPacker()
	packer.Close()
	for i := 0; i < 65536; i++ {
		packer.EncodeInt64(int64(i))
	}
	bs := packer.Bytes()
	if len(bs) != 261887 {
		t.Fatalf("got %v", len(bs))
	}
	packer.Close()
}

func TestPackerCapacityUpperBoundCoversGrowthAndReuse(t *testing.T) {
	const (
		columns = 4
		width   = 65535
	)
	component := uint64(2*width + 3)
	maxLength := uint64(columns) * component
	bound, ok := PackerCapacityUpperBound(maxLength, component)
	require.True(t, ok)
	require.Equal(t, uint64(1<<20), bound)

	packer := NewPacker()
	require.Equal(t, DefaultPackerCapacity(), packer.Allocated())
	value := make([]byte, width)
	for range 2 {
		packer.Reset()
		for range columns {
			packer.EncodeStringType(value)
		}
		require.Equal(t, int(maxLength), len(packer.GetBuf()))
		require.LessOrEqual(t, packer.Allocated(), bound)
	}
	packer.Close()
	require.Zero(t, packer.Allocated())

	initial := DefaultPackerCapacity()
	got, ok := PackerCapacityUpperBound(initial, initial)
	require.True(t, ok)
	require.Equal(t, initial, got)
	got, ok = PackerCapacityUpperBound(initial+1, 1)
	require.True(t, ok)
	require.Equal(t, 2*initial, got)
	_, ok = PackerCapacityUpperBound(math.MaxUint64, 2)
	require.False(t, ok)
}

func BenchmarkPacker(b *testing.B) {
	for i := 0; i < b.N; i++ {
		packer := NewPacker()
		packer.EncodeInt64(42)
		packer.Close()
	}
}

func BenchmarkPackerEncode(b *testing.B) {
	packer := NewPacker()
	defer packer.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packer.EncodeInt64(42)
		packer.Reset()
	}
}

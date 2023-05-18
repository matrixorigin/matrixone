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

package types

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

var (
	globalSegmentidPointer *Segmentid
	globalSegmentid        Segmentid

	globalBlockidPointer *Blockid
	globalBlockid        Blockid
)

// BenchmarkSegmentID checks heap allocation.
// go test -run None -bench BenchmarkSegmentID -benchmem
func BenchmarkSegmentID(b *testing.B) {
	rowID := RandomRowid()

	b.Run("BorrowSegmentID", func(b *testing.B) {
		var ret *Segmentid
		for i := 0; i < b.N; i++ {
			ret = rowID.BorrowSegmentID()
		}
		globalSegmentidPointer = ret
	})

	b.Run("CloneSegmentID", func(b *testing.B) {
		var ret Segmentid
		for i := 0; i < b.N; i++ {
			ret = rowID.CloneSegmentID()
		}
		globalSegmentid = ret
	})
}

func BenchmarkBlockID(b *testing.B) {
	rowID := RandomRowid()

	b.Run("BorrowBlockID", func(b *testing.B) {
		var ret *Blockid
		for i := 0; i < b.N; i++ {
			ret = rowID.BorrowBlockID()
		}
		globalBlockidPointer = ret
	})

	b.Run("CloneBlockID", func(b *testing.B) {
		var ret Blockid
		for i := 0; i < b.N; i++ {
			ret = rowID.CloneBlockID()
		}
		globalBlockid = ret
	})
}

func TestSegmentID(t *testing.T) {
	base := RandomRowid()

	borrowed := base.BorrowSegmentID()
	require.Equal(t, unsafe.Pointer(&base[0]), unsafe.Pointer(&borrowed[0]))

	cloned := base.CloneSegmentID()
	require.NotEqual(t, unsafe.Pointer(&base[0]), unsafe.Pointer(&cloned[0]))
}

func TestBlockID(t *testing.T) {
	base := RandomRowid()

	borrowed := base.BorrowBlockID()
	require.Equal(t, unsafe.Pointer(&base[0]), unsafe.Pointer(&borrowed[0]))

	cloned := base.CloneBlockID()
	require.NotEqual(t, unsafe.Pointer(&base[0]), unsafe.Pointer(&cloned[0]))
}

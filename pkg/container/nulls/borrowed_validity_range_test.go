// Copyright 2026 Matrix Origin
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

package nulls

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/stretchr/testify/require"
)

func TestBorrowedValidityCountRangeBoundaries(t *testing.T) {
	for _, offset := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 63, 64, 65} {
		for _, length := range []int{1, 7, 8, 9, 63, 64, 65, 127, 128, 129} {
			for _, pattern := range []byte{0, 0xff, 0x55, 0xa3} {
				checkBorrowedValidityRanges(t, offset, length, pattern)
			}
		}
	}
	var absent *Nulls
	require.Zero(t, absent.CountRange(0, ^uint64(0)))
}

func checkBorrowedValidityRanges(t *testing.T, offset, length int, pattern byte) {
	t.Helper()
	validity := bytes.Repeat([]byte{pattern}, (offset+length+7)/8)
	// Ensure the view is admissible, including an otherwise all-valid parent.
	validity[(offset+length-1)/8] &^= 1 << ((offset + length - 1) & 7)
	prefix := make([]int, length+1)
	for row := 0; row < length; row++ {
		prefix[row+1] = prefix[row]
		if validity[(offset+row)/8]&(1<<((offset+row)&7)) == 0 {
			prefix[row+1]++
		}
	}
	before := bytes.Clone(validity)
	releases := 0
	lease, err := bufferlease.NewRefCounted(validity, int64(len(validity)), func() { releases++ })
	require.NoError(t, err)
	defer lease.Release()
	var nsp Nulls
	defer nsp.Reset()
	require.NoError(t, nsp.InstallBorrowedValidity(validity, offset, length, prefix[length], lease))
	for start := 0; start <= length; start++ {
		for end := start; end <= length; end++ {
			want := prefix[end] - prefix[start]
			if got := nsp.CountRange(uint64(start), uint64(end)); got != want {
				t.Fatalf("offset=%d length=%d pattern=%x range=[%d,%d): got %d, want %d", offset, length, pattern, start, end, got, want)
			}
		}
		require.Equal(t, prefix[length]-prefix[start], nsp.CountRange(uint64(start), ^uint64(0)))
	}
	require.Zero(t, nsp.CountRange(uint64(length+1), ^uint64(0)))
	require.Zero(t, nsp.CountRange(2, 1))
	require.True(t, nsp.HasBorrowedValidity())
	require.Equal(t, before, validity)
	require.Zero(t, releases)
}

func TestBorrowedValidityCountRangeUsesBits(t *testing.T) {
	// Install admits the caller's count without checking it. CountRange must
	// preserve its exact bitmap result even when that cached summary differs.
	validity := make([]byte, 9)
	lease, err := bufferlease.NewRefCounted(validity, 9, nil)
	require.NoError(t, err)
	defer lease.Release()
	var nsp Nulls
	defer nsp.Reset()
	require.NoError(t, nsp.InstallBorrowedValidity(validity, 1, 65, 1, lease))
	require.Equal(t, 1, nsp.Count())
	require.Equal(t, 65, nsp.CountRange(0, 65))
}

func TestBorrowedValidityWordWindowRetainsAndRoundTrips(t *testing.T) {
	validity := bytes.Repeat([]byte{0x55}, 17)
	before := bytes.Clone(validity)
	releases := 0
	lease, err := bufferlease.NewRefCounted(validity, 17, func() { releases++ })
	require.NoError(t, err)
	incoming := true
	defer func() {
		if incoming {
			lease.Release()
		}
	}()
	var source, window, decoded Nulls
	defer source.Reset()
	defer window.Reset()
	defer decoded.Reset()
	require.NoError(t, source.InstallBorrowedValidity(validity, 3, 129, 65, lease))
	lease.Release()
	incoming = false
	window.Build(2, 0)
	borrowed, err := source.InitBorrowedWindow(&window, 7, 72)
	require.NoError(t, err)
	require.True(t, borrowed)
	source.Reset()
	require.Zero(t, releases)
	var expected []uint64
	for row := 0; row < 65; row++ {
		isNull := validity[(3+7+row)/8]&(1<<((3+7+row)&7)) == 0
		require.Equal(t, isNull, window.Contains(uint64(row)))
		if isNull {
			expected = append(expected, uint64(row))
		}
	}
	require.Equal(t, len(expected), window.Count())
	var wire bytes.Buffer
	require.NoError(t, window.MarshalTo(&wire))
	require.NoError(t, decoded.Read(wire.Bytes()))
	require.Equal(t, expected, decoded.ToArray())
	require.True(t, window.HasBorrowedValidity())
	require.Equal(t, before, validity)
	window.Reset()
	require.Equal(t, 1, releases)
}

var borrowedValidityRangeBenchSink int

func BenchmarkBorrowedValidityRange(b *testing.B) {
	for _, rows := range []int{8, 64, 2048, 8192} {
		for _, mode := range []string{"mixed", "all_valid_window"} {
			validity := bytes.Repeat([]byte{0xff}, (3+rows+8+7)/8)
			if mode == "mixed" {
				for row := 0; row < rows+8; row += 4 {
					validity[(3+row)/8] &^= 1 << ((3 + row) & 7)
				}
			} else {
				validity[(3+rows+7)/8] &^= 1 << ((3 + rows + 7) & 7)
			}
			count := 0
			for row := 0; row < rows+8; row++ {
				if validity[(3+row)/8]&(1<<((3+row)&7)) == 0 {
					count++
				}
			}
			for _, operation := range []string{"count", "window"} {
				b.Run(fmt.Sprintf("%s_%s_%d", operation, mode, rows), func(b *testing.B) {
					lease, err := bufferlease.NewRefCounted(validity, int64(len(validity)), nil)
					require.NoError(b, err)
					defer lease.Release()
					var source, window Nulls
					defer source.Reset()
					defer window.Reset()
					require.NoError(b, source.InstallBorrowedValidity(validity, 3, rows+8, count, lease))
					want := 0
					for row := 1; row < rows+1; row++ {
						if validity[(3+row)/8]&(1<<((3+row)&7)) == 0 {
							want++
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if operation == "count" {
							borrowedValidityRangeBenchSink = source.CountRange(1, uint64(rows+1))
						} else {
							if _, err := source.InitBorrowedWindow(&window, 1, rows+1); err != nil {
								b.Fatal(err)
							}
							borrowedValidityRangeBenchSink = window.Count()
						}
					}
					b.StopTimer()
					require.Equal(b, want, borrowedValidityRangeBenchSink)
					require.True(b, source.HasBorrowedValidity())
				})
			}
		}
	}
}

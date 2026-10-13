// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A length-encoded length above MaxInt64 becomes a negative int; it must be
// rejected instead of panicking on the slice (issue #29859).
func TestReadLenEncRejectsOverflowingLength(t *testing.T) {
	mp := &MysqlProtocolImpl{}
	data := []byte{0xfe, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0xb3}
	require.NotPanics(t, func() {
		_, _, ok := mp.readStringLenEnc(data, 0)
		require.False(t, ok)
	})
	// a length that fits int but exceeds the packet
	require.NotPanics(t, func() {
		_, _, ok := mp.readStringLenEnc([]byte{0xfc, 0xff, 0x00, 'a'}, 0)
		require.False(t, ok)
	})
	s, pos, ok := mp.readStringLenEnc([]byte{0x02, 'h', 'i', 'x'}, 0)
	require.True(t, ok)
	require.Equal(t, "hi", s)
	require.Equal(t, 3, pos)
	s, pos, ok = mp.readStringLenEnc([]byte{0x00}, 0)
	require.True(t, ok)
	require.Equal(t, "", s)
	require.Equal(t, 1, pos)

	require.NotPanics(t, func() {
		_, _, ok := mp.readCountOfBytes([]byte("abc"), 1, -5)
		require.False(t, ok)
	})
	b, pos, ok := mp.readCountOfBytes([]byte("abc"), 1, 2)
	require.True(t, ok)
	require.Equal(t, []byte("bc"), b)
	require.Equal(t, 3, pos)
	_, _, ok = mp.readCountOfBytes([]byte("abc"), 1, 3)
	require.False(t, ok)
}

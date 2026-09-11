// Copyright 2021 - 2024 Matrix Origin
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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionInfoDigestAutoIncrementWireCompatibility(t *testing.T) {
	// Fixed wire bytes guard field numbers independently of generated code.
	for _, tc := range []struct {
		name string
		wire []byte
		want SessionInfo
	}{
		{"legacy", nil, SessionInfo{}},
		{"v56 auto increment", []byte{0x78, 7, 0x80, 1, 4}, SessionInfo{AutoIncrementIncrement: 7, AutoIncrementOffset: 4}},
		{"explicit zero digest", []byte{0x90, 1, 1}, SessionInfo{MaxDigestLengthSet: true}},
		{"both", []byte{0x78, 7, 0x80, 1, 4, 0x88, 1, 16, 0x90, 1, 1}, SessionInfo{AutoIncrementIncrement: 7, AutoIncrementOffset: 4, MaxDigestLength: 16, MaxDigestLengthSet: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got SessionInfo
			require.NoError(t, got.Unmarshal(tc.wire))
			require.Equal(t, tc.want, got)
			wire, err := got.Marshal()
			require.NoError(t, err)
			require.Equal(t, tc.wire, append([]byte(nil), wire...))
		})
	}
	for _, wire := range [][]byte{{0x88, 1}, {0x90, 1}, {0x8a, 1, 0}} {
		var got SessionInfo
		require.Error(t, got.Unmarshal(wire), "truncated or wrong-wire-type digest field: %x", wire)
	}
}

// TestProcessInfoAffectedRowsRoundTrip ensures ProcessInfo.AffectedRows survives
// Marshal/Unmarshal, so ROW_COUNT() is preserved when a proc is shipped to a
// remote CN. The -1 sentinel (failed/result-set statement) must round-trip too.
func TestProcessInfoAffectedRowsRoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, 42, 100, -1, 1 << 40} {
		in := &ProcessInfo{AffectedRows: v}
		data, err := in.Marshal()
		require.NoError(t, err)

		out := &ProcessInfo{}
		require.NoError(t, out.Unmarshal(data))
		require.Equal(t, v, out.AffectedRows, "AffectedRows=%d", v)
		require.Equal(t, v, out.GetAffectedRows(), "GetAffectedRows=%d", v)
	}
}

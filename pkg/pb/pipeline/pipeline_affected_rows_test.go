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

func TestProcessInfoSelectDMLAndODKUWireFieldsRemainIndependent(t *testing.T) {
	// Upstream owns SELECT17 and DML18. ODKU ordinal19 must not be decoded as
	// either boolean. Process decoding separately rejects SELECT+DML together.
	legacy := new(ProcessInfo)
	require.NoError(t, legacy.Unmarshal([]byte{0x88, 0x01, 0x01}))
	require.True(t, legacy.StatementRuntimeSelect)
	require.False(t, legacy.StatementRuntimeDml)
	require.Zero(t, legacy.OdkuOrdinalBase)
	mainDML := new(ProcessInfo)
	require.NoError(t, mainDML.Unmarshal([]byte{0x90, 0x01, 0x01}))
	require.False(t, mainDML.StatementRuntimeSelect)
	require.True(t, mainDML.StatementRuntimeDml)
	require.Zero(t, mainDML.OdkuOrdinalBase)

	for _, selectStatement := range []bool{false, true} {
		for _, dml := range []bool{false, true} {
			info := &ProcessInfo{StatementRuntimeSelect: selectStatement, StatementRuntimeDml: dml, OdkuOrdinalBase: 700}
			data, err := info.Marshal()
			require.NoError(t, err)
			var want []byte
			if selectStatement {
				want = append(want, 0x88, 0x01, 0x01)
			}
			if dml {
				want = append(want, 0x90, 0x01, 0x01)
			}
			want = append(want, 0x98, 0x01, 0xbc, 0x05)
			// Non-nullable nested messages precede these fields even at zero.
			require.GreaterOrEqual(t, len(data), len(want))
			require.Equal(t, want, data[len(data)-len(want):])
			decoded := new(ProcessInfo)
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, selectStatement, decoded.StatementRuntimeSelect)
			require.Equal(t, dml, decoded.StatementRuntimeDml)
			require.Equal(t, uint64(700), decoded.OdkuOrdinalBase)
		}
	}
}

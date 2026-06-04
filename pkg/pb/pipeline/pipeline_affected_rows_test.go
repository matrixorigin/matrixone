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

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

package metadata

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestIndexJobMetadata(t *testing.T) {
	require.Equal(t, "index_x", IndexJobName("x"))
	for _, tc := range []struct {
		state int8
		code  int64
		want  string
	}{{StateError, 7, "stopped"}, {StateCanceled, 7, "canceled"}, {StateRunning, 7, "retrying"}, {StateRunning, 0, "running"}} {
		require.Equal(t, tc.want, StateName(tc.state, tc.code))
	}
	for _, bad := range []string{"", "0-0", "1", "x-0", "1-x", "1-4294967296", types.BuildTS(math.MaxInt64, math.MaxUint32).ToString()} {
		_, ok := ParseWatermark(bad)
		require.False(t, ok, bad)
	}
	got, ok := ParseWatermark("123-4")
	require.True(t, ok)
	require.Equal(t, types.BuildTS(123, 4), got)
}

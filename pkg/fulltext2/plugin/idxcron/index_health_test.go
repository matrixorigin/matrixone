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

package idxcron

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceHealth(t *testing.T) {
	for _, tc := range []struct {
		chunks, live, base int64
		mode               string
	}{
		{TailChunkThreshold - 1, 69, 100, ""}, {TailChunkThreshold, 70, 100, "MERGE"}, {TailChunkThreshold + 1, 69, 100, "REBUILD"}, {TailChunkThreshold, 0, 0, "MERGE"},
	} {
		m, a := maintenanceHealth(fulltext2.TailUsage{Chunks: tc.chunks, Frames: 2, Bytes: 99}, tc.live, tc.base)
		require.Equal(t, int64(2), m["tail_frame_count"])
		require.Equal(t, int64(99), m["tail_size_bytes"])
		if tc.base == 0 {
			require.NotContains(t, m, "base_dead_ratio")
		}
		if tc.mode == "" {
			require.Empty(t, a)
		} else {
			require.Len(t, a, 1)
			require.Equal(t, tc.mode, a[0].Params["mode"])
		}
	}
}

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

package iscp

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// GetToTS is the upper bound of the change range this iteration carries -- the same status.To
// that UpdateWatermark persists, and the value an index consumer records as the rebuilt
// generation's build_ts.
//
// It must be To and not the consumer's own transaction SnapshotTS: the latter is >= To, so it
// would claim coverage of changes committed after the range was collected but never applied.
func TestDataRetrieverGetToTSIsTheIterationBound(t *testing.T) {
	to := types.BuildTS(9876, 3)
	r := &DataRetrieverImpl{status: &JobStatus{From: types.BuildTS(1000, 0), To: to}}
	require.Equal(t, to, r.GetToTS())
	require.Equal(t, int64(9876), r.GetToTS().Physical(),
		"the physical component is what lands in metadata.build_ts")
}

// A retriever with no status yields the zero TS, which the consumer records as unknown rather
// than as a generation covering the epoch.
func TestDataRetrieverGetToTSUnknownWithoutStatus(t *testing.T) {
	empty := (&DataRetrieverImpl{}).GetToTS()
	require.True(t, empty.IsEmpty())
	require.EqualValues(t, 0, empty.Physical())
}

// The value handed out is the same one UpdateWatermark persists -- if these ever diverge, a
// generation could claim coverage the watermark does not confirm.
func TestDataRetrieverGetToTSMatchesPersistedWatermark(t *testing.T) {
	st := &JobStatus{To: types.BuildTS(555, 1)}
	r := &DataRetrieverImpl{status: st}
	require.Equal(t, st.To, r.GetToTS())
}

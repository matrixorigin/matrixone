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

package colexec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildSummaryBatch(t *testing.T) {
	for _, tc := range []struct {
		nonEmpty bool
		hasNull  bool
	}{{}, {nonEmpty: true}, {nonEmpty: true, hasNull: true}} {
		bat := NewBuildSummaryBatch(tc.nonEmpty, tc.hasNull)
		require.True(t, IsBuildSummaryBatch(bat))
		require.Empty(t, bat.Vecs)
		require.Len(t, bat.ExtraBuf, 1)
		nonEmpty, hasNull, err := DecodeBuildSummaryBatch(bat)
		require.NoError(t, err)
		require.Equal(t, tc.nonEmpty, nonEmpty)
		require.Equal(t, tc.hasNull, hasNull)
	}
}

func TestBuildSummaryBatchRejectsMalformedPayload(t *testing.T) {
	bat := NewBuildSummaryBatch(true, false)
	bat.ExtraBuf = nil
	_, _, err := DecodeBuildSummaryBatch(bat)
	require.Error(t, err)
}

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

package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDMLOverwritePartitionPlanExtraOptionsRoundTrip(t *testing.T) {
	encoded, err := EncodeDMLOverwritePartitionPlanExtraOptions(map[string]any{
		"region": "ksa",
		"day":    int64(20260624),
	})
	require.NoError(t, err)
	require.Contains(t, encoded, DMLPlanExtraOptionsEnvelopePrefix)

	decoded, err := DecodeDMLPlanExtraOptions(encoded)
	require.NoError(t, err)
	require.Equal(t, DMLOverwritePlanExtraOptions, decoded.Kind)
	require.Equal(t, "partition", decoded.OverwriteScope)
	require.Equal(t, "ksa", decoded.OverwritePartition["region"])
	require.Equal(t, int64(20260624), decoded.OverwritePartition["day"])
}

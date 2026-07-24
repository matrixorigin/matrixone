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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIndexParamsToStringListFulltext2Options pins that the shared SHOW CREATE /
// checkpoint-restore option renderer emits the fulltext2 build options —
// position_free plus the capacities — so a rebuilt index keeps them instead of
// silently reverting to a positional / default-capacity one.
func TestIndexParamsToStringListFulltext2Options(t *testing.T) {
	params, err := IndexParamsMapToJsonString(map[string]string{
		"parser":                          "ngram",
		IndexAlgoParamPositionFree:        "true",
		IndexAlgoParamMaxIndexCapacity:    "1000",
		IndexAlgoParamMaxPostingsCapacity: "8000",
	})
	require.NoError(t, err)

	s, err := IndexParamsToStringList(params)
	require.NoError(t, err)
	require.Contains(t, s, IndexAlgoParamPositionFree+" = true")
	require.Contains(t, s, IndexAlgoParamMaxIndexCapacity+" = 1000")
	require.Contains(t, s, IndexAlgoParamMaxPostingsCapacity+" = 8000")

	// position_free=false ⇒ positional ⇒ NOT rendered (only recorded/rendered when true).
	params2, err := IndexParamsMapToJsonString(map[string]string{IndexAlgoParamPositionFree: "false"})
	require.NoError(t, err)
	s2, err := IndexParamsToStringList(params2)
	require.NoError(t, err)
	require.NotContains(t, s2, IndexAlgoParamPositionFree)
}

// TestIndexParamsToStringListAsyncCadence pins that the AUTO_UPDATE compaction cadence —
// DAY / HOUR / SECOND — round-trips through the shared SHOW CREATE / checkpoint-restore
// renderer. SECOND in particular was dropped, so an async index (fulltext2 / ivfflat /
// cagra / ivfpq — all persist it) created with SECOND=5 silently lost its cadence on
// SHOW CREATE / checkpoint restore and reverted to the idxcron default interval.
func TestIndexParamsToStringListAsyncCadence(t *testing.T) {
	params, err := IndexParamsMapToJsonString(map[string]string{
		AutoUpdate: "true",
		Day:        "1",
		Hour:       "2",
		Second:     "5",
	})
	require.NoError(t, err)

	s, err := IndexParamsToStringList(params)
	require.NoError(t, err)
	require.Contains(t, s, Day+" = 1")
	require.Contains(t, s, Hour+" = 2")
	require.Contains(t, s, Second+" = 5")
}

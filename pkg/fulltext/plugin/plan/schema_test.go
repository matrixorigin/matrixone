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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// buildFullTextParams must capture the idxcron cadence knobs (AUTO_UPDATE/DAY/HOUR)
// into algo_params, like the vector plugins' ParamsFromTree — otherwise a retrieval
// index's CREATE options parse but never reach the idxcron gate.
func TestBuildFullTextParams_CadenceKnobs(t *testing.T) {
	idx := &tree.FullTextIndex{
		IndexOption: &tree.IndexOption{
			ParserName: "retrieval",
			Async:      true,
			AutoUpdate: true,
			Day:        2,
			Hour:       5,
		},
	}
	js, err := buildFullTextParams(idx)
	require.NoError(t, err)
	require.Contains(t, js, `"parser":"retrieval"`)
	require.Contains(t, js, `"async":"true"`)
	require.Contains(t, js, `"auto_update":"true"`)
	require.Contains(t, js, `"day":"2"`)
	require.Contains(t, js, `"hour":"5"`)
}

// day=0 / hour=0 are treated as unset (mirror vector's `> 0` guard), so they are
// omitted — hour=0 is midnight in the gate, indistinguishable from "not provided".
func TestBuildFullTextParams_ZeroCadenceOmitted(t *testing.T) {
	idx := &tree.FullTextIndex{
		IndexOption: &tree.IndexOption{ParserName: "retrieval", Day: 0, Hour: 0},
	}
	js, err := buildFullTextParams(idx)
	require.NoError(t, err)
	require.NotContains(t, js, "day")
	require.NotContains(t, js, "hour")
	require.NotContains(t, js, "auto_update")
}

func TestBuildFullTextParams_InvalidParser(t *testing.T) {
	idx := &tree.FullTextIndex{IndexOption: &tree.IndexOption{ParserName: "bogus"}}
	_, err := buildFullTextParams(idx)
	require.Error(t, err)
}

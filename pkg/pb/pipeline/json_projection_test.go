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

package pipeline

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONSubcolumnProjectionWireContract(t *testing.T) {
	for _, projection := range []string{"", `json-paths-v1:{"0":["$.cost"]}`} {
		input := &TableScan{JsonProjection: projection}
		encoded, err := input.Marshal()
		require.NoError(t, err)
		var output TableScan
		require.NoError(t, output.Unmarshal(encoded))
		require.Equal(t, projection, output.JsonProjection)
	}
	// Regeneration must retain upstream fields while appending the scan hint.
	expected := map[string]map[string]int32{
		"TableScan":   {"json_projection": 3},
		"Aggregate":   {"result_type": 6},
		"ProcessInfo": {"statement_runtime_select": 17},
	}
	for _, message := range decodePipelineFileDescriptor(t).GetMessageType() {
		fields := expected[message.GetName()]
		for _, field := range message.GetField() {
			if number, exists := fields[field.GetName()]; exists {
				require.Equal(t, number, field.GetNumber())
				delete(fields, field.GetName())
			}
		}
	}
	for name, fields := range expected {
		require.Empty(t, fields, "missing retained fields in %s", name)
	}
}

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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestMergedFulltextPartitionPipelineFields(t *testing.T) {
	want := map[string]map[string]int32{
		"PreInsert": {"preserve_input": 22},
		"DedupJoin": {"auxiliary_row_id_col_idx": 36},
		"HashBuild": {"auxiliary_row_id_col_idx": 21},
	}
	for _, message := range decodePipelineFileDescriptor(t).GetMessageType() {
		fields, ok := want[message.GetName()]
		if !ok {
			continue
		}
		for _, field := range message.GetField() {
			if number, exists := fields[field.GetName()]; exists {
				require.Equal(t, number, field.GetNumber(), "%s.%s", message.GetName(), field.GetName())
				delete(fields, field.GetName())
			}
		}
		require.Empty(t, fields, "missing fields in %s", message.GetName())
		delete(want, message.GetName())
	}
	require.Empty(t, want, "missing merged pipeline descriptors")

	tests := []struct {
		name string
		in   proto.Message
		out  proto.Message
	}{
		{name: "preinsert", in: &PreInsert{PreserveInput: true}, out: &PreInsert{}},
		{name: "dedup_join", in: &DedupJoin{AuxiliaryRowIdColIdx: 9}, out: &DedupJoin{}},
		{name: "hash_build", in: &HashBuild{AuxiliaryRowIdColIdx: 11}, out: &HashBuild{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := proto.Marshal(tt.in)
			require.NoError(t, err)
			require.NoError(t, proto.Unmarshal(data, tt.out))
			require.Equal(t, tt.in, tt.out)
		})
	}
}

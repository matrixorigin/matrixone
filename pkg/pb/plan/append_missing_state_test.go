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

package plan_test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func birthState() *plan.AppendMissingState {
	return &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"),
		BaseWidth: 3, PkSeqnum: 2, Columns: []*plan.ColumnBirthFill{{Seqnum: 3,
			BirthAppendOrdinal: 1, Type: plan.Type{Id: int32(types.T_bool)}, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}
}

func TestAppendMissingWireAndClone(t *testing.T) {
	require.Equal(t, int32(types.T_bool), plan.AppendMissingBoolOID)
	original := birthState()
	require.NoError(t, plan.ValidateAppendMissingState(original))
	data, err := original.Marshal()
	require.NoError(t, err)
	var decoded plan.AppendMissingState
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, original, &decoded)
	cloned := plan.CloneAppendMissingState(original)
	cloned.LayoutId[0] = 'x'
	cloned.Columns[0].Kind = plan.MissingFillKind_MISSING_FILL_NULL_V1
	cloned.Columns[0].Type.Width = 1
	require.Equal(t, []byte("0123456789abcdef"), original.LayoutId)
	require.Equal(t, plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1, original.Columns[0].Kind)
	require.Zero(t, original.Columns[0].Type.Width)
	require.Nil(t, plan.CloneAppendMissingState(nil))
	require.NoError(t, plan.ValidateAppendMissingState(nil))
}

func TestAppendMissingRejectsUnknownOrIncompleteState(t *testing.T) {
	cases := map[string]func(*plan.AppendMissingState){
		"format":         func(s *plan.AppendMissingState) { s.FormatVersion = 2 },
		"empty layout":   func(s *plan.AppendMissingState) { s.LayoutId = nil },
		"zero layout":    func(s *plan.AppendMissingState) { s.LayoutId = make([]byte, 16) },
		"base":           func(s *plan.AppendMissingState) { s.BaseWidth = 0 },
		"anchor":         func(s *plan.AppendMissingState) { s.PkSeqnum = 1 },
		"empty columns":  func(s *plan.AppendMissingState) { s.Columns = nil },
		"nil column":     func(s *plan.AppendMissingState) { s.Columns[0] = nil },
		"hole":           func(s *plan.AppendMissingState) { s.Columns[0].Seqnum++ },
		"birth":          func(s *plan.AppendMissingState) { s.Columns[0].BirthAppendOrdinal++ },
		"kind":           func(s *plan.AppendMissingState) { s.Columns[0].Kind = 99 },
		"type":           func(s *plan.AppendMissingState) { s.Columns[0].Type.Id = int32(types.T_int32) },
		"not nullable":   func(s *plan.AppendMissingState) { s.Columns[0].Type.NotNullable = true },
		"auto increment": func(s *plan.AppendMissingState) { s.Columns[0].Type.AutoIncr = true },
		"duplicate":      func(s *plan.AppendMissingState) { s.Columns = append(s.Columns, s.Columns[0]) },
		"reserved":       func(s *plan.AppendMissingState) { s.BaseWidth = 65530; s.PkSeqnum = 65529 },
		"unknown field":  func(s *plan.AppendMissingState) { s.XXX_unrecognized = []byte{48, 1} },
		"unknown type":   func(s *plan.AppendMissingState) { s.Columns[0].Type.XXX_unrecognized = []byte{112, 1} },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			state := birthState()
			change(state)
			require.Error(t, plan.ValidateAppendMissingState(state))
		})
	}
}

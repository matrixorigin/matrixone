// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAppendMissingExtraOwnerBoundary(t *testing.T) {
	source := &SchemaExtra{NextColSeqnum: 4, AppendMissingState: &plan.AppendMissingState{
		FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: 3, PkSeqnum: 2,
		Columns: []*plan.ColumnBirthFill{{Seqnum: 3, BirthAppendOrdinal: 1,
			Type: plan.Type{Id: plan.AppendMissingBoolOID}, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}},
	}}
	restored := MustUnmarshalTblExtra(MustMarshalTblExtra(source))
	require.Equal(t, source, restored)
	cloned := CloneExtra(source)
	cloned.AppendMissingState.Columns[0].Kind = plan.MissingFillKind_MISSING_FILL_NULL_V1
	require.Equal(t, plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1, source.AppendMissingState.Columns[0].Kind)
	for _, mutate := range []func(*SchemaExtra){
		func(s *SchemaExtra) { s.NextColSeqnum++ },
		func(s *SchemaExtra) { s.AppendMissingState.FormatVersion++ },
		func(s *SchemaExtra) { s.AppendMissingState.Columns[0].Kind = 99 },
	} {
		bad := CloneExtra(source)
		mutate(bad)
		require.Error(t, bad.ValidateAppendMissing())
		wire, err := bad.Marshal() // Simulate bytes produced outside our validated writer.
		require.NoError(t, err)
		require.Panics(t, func() { MustUnmarshalTblExtra(wire) })
		require.Panics(t, func() { MustMarshalTblExtra(bad) })
	}
	require.Equal(t, &SchemaExtra{}, MustUnmarshalTblExtra(nil))
}

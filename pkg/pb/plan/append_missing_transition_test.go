// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan_test

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func falseAdded(seq uint32) *plan.ColDef {
	typ := plan.Type{Id: plan.AppendMissingBoolOID}
	return &plan.ColDef{Name: "flag", ColId: uint64(seq) + 1, Seqnum: seq, Typ: typ, Default: &plan.Default{NullAbility: true, OriginString: "false",
		Expr: &plan.Expr{Typ: typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}}}
}

func TestAppendMissingExtensionIsImmutable(t *testing.T) {
	initial := birthState()
	types := make([]plan.Type, 3)
	added := falseAdded(3)
	require.NoError(t, plan.ValidateAppendMissingExtension(nil, initial, 3, 0, types, added))
	next := plan.CloneAppendMissingState(initial)
	next.Columns = append(next.Columns, &plan.ColumnBirthFill{Seqnum: 4, BirthAppendOrdinal: 2, Type: added.Typ, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1})
	types = append(types, added.Typ)
	require.NoError(t, plan.ValidateAppendMissingExtension(initial, next, 3, 1, types, falseAdded(4)))
	for _, change := range []func(*plan.AppendMissingState){
		func(s *plan.AppendMissingState) { s.LayoutId[0] = 'x' },
		func(s *plan.AppendMissingState) { s.Columns[0].Kind = plan.MissingFillKind_MISSING_FILL_NULL_V1 },
		func(s *plan.AppendMissingState) { s.Columns[1].Kind = plan.MissingFillKind_MISSING_FILL_NULL_V1 },
	} {
		bad := plan.CloneAppendMissingState(next)
		change(bad)
		require.Error(t, plan.ValidateAppendMissingExtension(initial, bad, 3, 1, types, falseAdded(4)))
	}
	legacy := plan.CloneAppendMissingState(next)
	legacy.Columns[0].Kind = plan.MissingFillKind_MISSING_FILL_NULL_V1
	require.NoError(t, plan.ValidateAppendMissingExtension(nil, legacy, 3, 1, types, falseAdded(4)))
	require.Error(t, plan.ValidateAppendMissingExtension(nil, next, 3, 1, types, falseAdded(4)), "pre-descriptor history cannot become false")
	wrong := falseAdded(4)
	wrong.Default.OriginString = "1 = 0"
	require.Error(t, plan.ValidateAppendMissingExtension(initial, next, 3, 1, types, wrong))
}

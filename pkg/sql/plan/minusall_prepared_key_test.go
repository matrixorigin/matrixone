// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPreparedMinusAllRefreshesPhysicalKeysAcrossDomains(t *testing.T) {
	for _, tc := range []struct {
		name, leftType, rightType string
		wantKeys                  bool
	}{
		{"pad_space", "varchar(8)", "char(8)", true},
		{"binary_control", "varbinary(8)", "varbinary(8)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := func(argument string) string {
				return fmt.Sprintf(
					"select cast('a ' as %s) as s, bit_and(%s) as b "+
						"except all select cast('a' as %s) as s, bit_and(%s) as b",
					tc.leftType, argument, tc.rightType, argument,
				)
			}
			findMinusAll := func(p *planpb.Plan) *planpb.Node {
				t.Helper()
				var found *planpb.Node
				for _, node := range p.GetQuery().Nodes {
					if node.NodeType == planpb.Node_MINUS_ALL {
						require.Nil(t, found, "fixture must have exactly one MINUS_ALL")
						found = node
					}
				}
				require.NotNil(t, found)
				require.Len(t, found.ProjectList, 2)
				return found
			}

			prepare := buildPreparedAggregatePlan(t, strings.ReplaceAll(query("?"), "'", "''"))
			cached := findMinusAll(prepare.Plan)
			require.Equal(t, int32(types.T_uint64), cached.ProjectList[1].Typ.Id)
			if tc.wantKeys {
				require.Len(t, cached.PhysicalEqualityKeyList, 2)
				require.NotNil(t, cached.PhysicalEqualityKeyList[1].GetCol())
				require.Equal(t, int32(types.T_uint64), cached.PhysicalEqualityKeyList[1].Typ.Id)
			} else {
				require.Empty(t, cached.PhysicalEqualityKeyList)
			}
			original, err := prepare.Plan.Marshal()
			require.NoError(t, err)

			staticPlan, err := runOneStmt(NewMockOptimizer(false), t,
				query("cast(X'02' as varbinary(1))"))
			require.NoError(t, err)
			staticSet := findMinusAll(staticPlan)
			require.NotEqual(t, cached.ProjectList[1].Typ.Id, staticSet.ProjectList[1].Typ.Id,
				"the binary binding must change the set output's scalar domain")

			// Repeat against the same untouched PREPARE template. Neither the
			// template nor its keys are cleared or rewritten to induce refresh.
			for attempt := 0; attempt < 2; attempt++ {
				param := ParamValue{
					Value: []byte{0x02}, SourceType: types.New(types.T_varbinary, 1, 0),
					HasSourceType: true, IsBin: true,
				}
				filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
					context.Background(), prepare.Plan, []any{param, param})
				require.NoError(t, err)
				require.True(t, specialized)
				actual := findMinusAll(filled)
				require.Equal(t, staticSet.ProjectList[1].Typ, actual.ProjectList[1].Typ)
				if tc.wantKeys {
					require.Len(t, actual.PhysicalEqualityKeyList, 2)
					canonical := actual.PhysicalEqualityKeyList[0].GetF()
					require.NotNil(t, canonical)
					require.Equal(t, "cast", canonical.Func.GetObjName())
					_, overload := planfunction.DecodeOverloadID(canonical.Func.GetObj())
					require.EqualValues(t, 3, overload)
					key := actual.PhysicalEqualityKeyList[1]
					require.NotNil(t, key.GetCol())
					require.Equal(t, actual.ProjectList[1].GetCol().RelPos, key.GetCol().RelPos)
					require.Equal(t, actual.ProjectList[1].GetCol().ColPos, key.GetCol().ColPos)
					// The PAD SPACE column requires a complete physical key tuple.
					// Its second ColRef has no parameter to rebind: MINUS_ALL key
					// refresh must replace its cached UINT64 type after BIT_AND
					// specializes to a binary result, including the new width.
					require.Equal(t, actual.ProjectList[1].Typ, key.Typ,
						"physical key must follow the specialized output, not cached UINT64")
				} else {
					require.Empty(t, actual.PhysicalEqualityKeyList,
						"binary equality must not acquire a PAD SPACE normalization key")
				}
				unchanged, err := prepare.Plan.Marshal()
				require.NoError(t, err)
				require.Equal(t, original, unchanged, "specialization must not mutate the cached plan")
			}
		})
	}
}

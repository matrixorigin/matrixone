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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestEnumSetResultPresentation(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want []types.T
	}{
		{"direct", "select e, s, v from enum_order_t", []types.T{types.T_enum, types.T_uint64, types.T_any}},
		{"reordered aliases", "select s as x, v, e as y, s from enum_order_t", []types.T{types.T_uint64, types.T_any, types.T_enum, types.T_uint64}},
		{"empty", "select e, s from enum_order_t where false", []types.T{types.T_enum, types.T_uint64}},
		{"distinct", "select distinct e, s from enum_order_t", []types.T{types.T_enum, types.T_uint64}},
		{"group key", "select e, s from enum_order_t group by e, s", []types.T{types.T_enum, types.T_uint64}},
		{"derived limit", "select s, e from (select e, s from enum_order_t limit 2) d", []types.T{types.T_uint64, types.T_enum}},
		{"cte", "with c as (select e, s from enum_order_t) select s, e from c", []types.T{types.T_uint64, types.T_enum}},
		{"outer join", "select b.s, a.e from enum_order_t a left join enum_order_t b on a.id=b.id", []types.T{types.T_uint64, types.T_enum}},
		{"string expressions", "select concat(e), cast(s as char), coalesce(e, v) from enum_order_t", []types.T{types.T_any, types.T_any, types.T_any}},
		{"union", "select e, s from enum_order_t union all select e, s from enum_order_t", []types.T{types.T_any, types.T_any}},
		{"derived union", "select e, s from (select e, s from enum_order_t union all select e, s from enum_order_t) d", []types.T{types.T_any, types.T_any}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, tc.sql)
			require.NoError(t, err)
			snapshot := proto.Clone(p)
			before, err := p.Marshal()
			require.NoError(t, err)
			cols := GetResultColumnsFromPlan(p)
			require.Len(t, cols, len(tc.want))
			for i, want := range tc.want {
				if want == types.T_any {
					require.Nil(t, cols[i].MysqlSpecialResultType)
					continue
				}
				require.NotNil(t, cols[i].MysqlSpecialResultType)
				require.Equal(t, int32(want), cols[i].MysqlSpecialResultType.Id)
				require.NotEmpty(t, cols[i].MysqlSpecialResultType.Enumvalues)
				require.Equal(t, int32(types.T_varchar), cols[i].Typ.Id, "execution and result storage still hold labels")
				require.Equal(t, cols[i].MysqlSpecialResultType, DeepCopyColDef(cols[i]).MysqlSpecialResultType)
				require.NotSame(t, cols[i].MysqlSpecialResultType, DeepCopyColDef(cols[i]).MysqlSpecialResultType)
			}
			if tc.want[0] != types.T_any {
				require.NotEmpty(t, p.GetQuery().MysqlSpecialResultTypes)
			} else {
				require.Empty(t, p.GetQuery().MysqlSpecialResultTypes, "ordinary results do not allocate presentation metadata")
			}
			cloned := GetResultColumnsFromPlan(DeepCopyPlan(p))
			require.Len(t, cloned, len(cols))
			for i := range cols {
				require.Equal(t, cols[i].Typ, cloned[i].Typ)
				require.Equal(t, cols[i].MysqlSpecialResultType, cloned[i].MysqlSpecialResultType)
			}
			var decoded pb.Plan
			require.NoError(t, decoded.Unmarshal(before))
			require.Equal(t, cols, GetResultColumnsFromPlan(&decoded))
			require.True(t, proto.Equal(snapshot, p), "extracting metadata must not mutate a cached plan")
		})
	}
}

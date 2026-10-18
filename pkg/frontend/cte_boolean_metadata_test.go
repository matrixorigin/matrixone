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

package frontend

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestComposedBooleanResultsKeepWireMetadata(t *testing.T) {
	mock := plan.NewMockOptimizer(false, newPlanTestProcess(t))
	for _, tc := range []struct {
		name    string
		sql     string
		lengths []uint32
	}{
		{"lag computed default", "select lag(with_grant_option,1,role_id is null) over(order by role_id) from mo_catalog.mo_role_privs", []uint32{8}},
		{"lead stored default", "select lead(with_grant_option,1,with_grant_option) over(order by role_id) from mo_catalog.mo_role_privs", []uint32{1}},
		{"lag null default", "select lag(with_grant_option,1,null) over(order by role_id) from mo_catalog.mo_role_privs", []uint32{1}},
		{"sample computed", "select sample(role_id>0,1 rows) from mo_catalog.mo_role_privs", []uint32{8}},
		{"sample grouped offset", "select sample(role_id>0,1 rows),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option", []uint32{8, 1}},
		{"sample stored", "select sample(with_grant_option,1 rows) from mo_catalog.mo_role_privs", []uint32{1}},
		{"grouped aggregate offset", "select min(role_id>0),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option", []uint32{8, 1}},
		{"distinct repeated predicate", "select distinct p,b,p,b from (select with_grant_option b,role_id>0 p,role_id n from mo_catalog.mo_role_privs) c", []uint32{8, 1, 8, 1}},
		{"minus membership", "select with_grant_option from mo_catalog.mo_role_privs minus select role_id>0 from mo_catalog.mo_role_privs", []uint32{1}},
		{"intersect membership", "select with_grant_option from mo_catalog.mo_role_privs intersect select role_id>0 from mo_catalog.mo_role_privs", []uint32{1}},
		{"recursive predicate", "with recursive c(n,p) as (select 1,1=1 union all select n+1,n<2 from c where n<3) select p from c", []uint32{8}},
		{"recursive stored control", "with recursive c(n,p) as (select 1,with_grant_option from mo_catalog.mo_role_privs union all select n+1,p from c where n<3) select p from c", []uint32{1}},
		{"recursive predicate after stored anchor", "with recursive c(n,p) as (select 1,with_grant_option from mo_catalog.mo_role_privs union all select n+1,n<2 from c where n<3) select p from c", []uint32{8}},
		{"recursive mixed slots", "with recursive c(n,b,p) as (select 1,with_grant_option,role_id is null from mo_catalog.mo_role_privs union all select n+1,b,n<2 from c where n<3) select b,p from c", []uint32{1, 8}},
		{"recursive reordered slots", "with recursive c(n,b,p) as (select 1,with_grant_option,role_id is null from mo_catalog.mo_role_privs union all select n+1,b,n<2 from c where n<3) select p,b from c", []uint32{8, 1}},
		{"recursive repeated slots", "with recursive c(n,b,p) as (select 1,with_grant_option,role_id is null from mo_catalog.mo_role_privs union all select n+1,b,n<2 from c where n<3) select p,b,p,b from c", []uint32{8, 1, 8, 1}},
		{"union all reordered", "select p,b from (select with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) c", []uint32{8, 1}},
		{"union distinct reordered", "select p,b from (select with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union select with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) c", []uint32{8, 1}},
		{"union all original order", "select b,p from (select with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) c", []uint32{1, 8}},
		{"union all repeated", "select p,b,p,b from (select with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) c", []uint32{8, 1, 8, 1}},
		{"shared materialized", "with c as (select l_suppkey n,sum(l_extendedprice*(1-l_discount))>0 p from lineitem group by l_suppkey) select p from c where p=(select max(p) from c)", []uint32{8}},
		{"inline control", "with c as (select with_grant_option b,role_id is null p from mo_catalog.mo_role_privs) select b,p from c", []uint32{1, 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := buildSingleSql(mock, t, tc.sql)
			require.NoError(t, err)
			if strings.HasPrefix(tc.name, "recursive ") || tc.name == "shared materialized" {
				hasProducerSteps := false
				for _, node := range p.GetQuery().Nodes {
					if len(node.SourceStep) > 0 {
						hasProducerSteps = true
					}
				}
				require.True(t, hasProducerSteps, "regression must exercise producer-step boundaries")
			}
			columns := plan.GetResultColumnsFromPlan(p)
			require.Len(t, columns, len(tc.lengths))
			proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
			for i, def := range columns {
				column, err := colDef2MysqlColumn(t.Context(), def)
				require.NoError(t, err)
				for _, cmd := range []int{int(COM_QUERY), int(COM_STMT_PREPARE)} {
					packet := proto.makeColumnDefinition41Payload(column, cmd)
					pos := HeaderOffset
					for range 6 {
						_, next, ok := proto.readStringLenEnc(packet, pos)
						require.True(t, ok)
						pos = next
					}
					_, pos, ok := proto.io.ReadUint8(packet, pos)
					require.True(t, ok)
					_, pos, ok = proto.io.ReadUint16(packet, pos)
					require.True(t, ok)
					length, pos, ok := proto.io.ReadUint32(packet, pos)
					require.True(t, ok)
					require.Equal(t, tc.lengths[i], length, "column %d, command %d", i, cmd)
					typ, _, ok := proto.io.ReadUint8(packet, pos)
					require.True(t, ok)
					require.Equal(t, uint8(defines.MYSQL_TYPE_TINY), typ)
				}
			}
		})
	}
}

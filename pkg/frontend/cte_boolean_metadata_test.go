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

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
		{"returning insert literal", "insert into returning_bool values(1,true,7) returning flag", []uint32{1}},
		{"returning insert null", "insert into returning_bool values(1,null,7) returning flag", []uint32{1}},
		{"returning insert select predicate", "insert into returning_bool select 1,1>0,7 returning flag", []uint32{1}},
		{"returning update literal", "update returning_bool set flag=false returning flag", []uint32{1}},
		{"returning update predicate", "update returning_bool set flag=n>0 returning flag", []uint32{1}},
		{"returning update self", "update returning_bool set flag=flag returning flag", []uint32{1}},
		{"returning delete", "delete from returning_bool returning flag", []uint32{1}},
		{"returning mixed", "update returning_bool set flag=false returning flag,n>0", []uint32{1, 8}},
		{"returning reordered repeated", "insert into returning_bool values(1,true,7) returning n>0,flag,n>0,flag", []uint32{8, 1, 8, 1}},
		{"returning expression", "update returning_bool set flag=false returning not flag,flag,true", []uint32{8, 1, 8}},
		{"returning star", "insert into returning_bool values(1,true,7) returning *", []uint32{0, 1, 0}},
		{"time window stored first", "select with_grant_option,max(role_id>0) from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute)", []uint32{1, 8}},
		{"time window aggregate first", "select max(role_id>0),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute)", []uint32{8, 1}},
		{"time window repeated", "select max(role_id>0),with_grant_option,max(role_id>0),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute)", []uint32{8, 1, 8, 1}},
		{"time window boundaries", "select _wend,with_grant_option,max(role_id>0),_wstart,_wend from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute)", []uint32{0, 1, 8, 0, 0}},
		{"time window aggregate only", "select max(role_id>0) from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute)", []uint32{8}},
		{"time window partition and boundary", "select with_grant_option,_wstart from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute)", []uint32{1, 0}},
		{"time window fill null", "select with_grant_option,max(role_id>0) from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute) fill(null)", []uint32{1, 8}},
		{"time window fill value", "select max(role_id>0),with_grant_option,_wstart from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute) fill(value,0)", []uint32{8, 1, 0}},
		{"time window fill prev", "select with_grant_option,_wend,max(role_id>0) from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute) fill(prev)", []uint32{1, 0, 8}},
		{"time window fill next", "select max(role_id>0),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute) fill(next)", []uint32{8, 1}},
		{"time window computed partition", "select role_id>0,with_grant_option,max(role_id is null) from mo_catalog.mo_role_privs group by with_grant_option,role_id>0 interval(granted_time,1,minute)", []uint32{8, 1, 8}},
		{"time window pruned derived", "select p,b,p,b from (select _wstart s,max(role_id>0) p,with_grant_option b,min(role_id is null) q from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute) fill(prev)) d", []uint32{8, 1, 8, 1}},
		{"time window linear numeric aggregate", "select with_grant_option,max(role_id),_wstart from mo_catalog.mo_role_privs group by with_grant_option interval(granted_time,1,minute) fill(linear)", []uint32{1, 0, 0}},
		{"values predicates", "values row(1=1),row(1=0)", []uint32{8}},
		{"values derived", "select p from (values row(1=1),row(1=0)) d(p)", []uint32{8}},
		{"values distinct", "select distinct p from (values row(1=1),row(1=0)) d(p)", []uint32{8}},
		{"values null first", "select p from (values row(null),row(1=0)) d(p)", []uint32{8}},
		{"values reordered repeated", "select q,p,q from (values row(1=1,1=0),row(1=0,1=1)) d(p,q)", []uint32{8, 8, 8}},
		{"values union", "select p from (values row(1=1),row(1=0)) d(p) union all select with_grant_option from mo_catalog.mo_role_privs", []uint32{8}},
		{"values window", "select first_value(p) over () from (values row(1=1),row(1=0)) d(p)", []uint32{8}},
		{"sample omitted stored grouping prefix", "select sample(role_id,1 rows),role_id>0 from mo_catalog.mo_role_privs group by with_grant_option,role_id>0", []uint32{0, 8}},
		{"sample omitted computed grouping prefix", "select sample(role_id,1 rows),with_grant_option from mo_catalog.mo_role_privs group by role_id>0,with_grant_option", []uint32{0, 1}},
		{"sample omitted two grouping keys", "select sample(role_id,1 rows),role_id>0 from mo_catalog.mo_role_privs group by with_grant_option,role_id+1,role_id>0", []uint32{0, 8}},
		{"sample computed grouping first control", "select sample(role_id,1 rows),role_id>0 from mo_catalog.mo_role_privs group by role_id>0,with_grant_option", []uint32{0, 8}},
		{"sample reordered grouping outputs", "select role_id>0,sample(role_id,1 rows),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option,role_id>0", []uint32{8, 0, 1}},
		{"sample derived grouping", "select p,p from (select sample(role_id,1 rows) n,role_id>0 p from mo_catalog.mo_role_privs group by with_grant_option,role_id>0) d", []uint32{8, 8}},
		{"lag computed default", "select lag(with_grant_option,1,role_id is null) over(order by role_id) from mo_catalog.mo_role_privs", []uint32{8}},
		{"lead stored default", "select lead(with_grant_option,1,with_grant_option) over(order by role_id) from mo_catalog.mo_role_privs", []uint32{1}},
		{"lag null default", "select lag(with_grant_option,1,null) over(order by role_id) from mo_catalog.mo_role_privs", []uint32{1}},
		{"sample computed", "select sample(role_id>0,1 rows) from mo_catalog.mo_role_privs", []uint32{8}},
		{"sample grouped offset", "select sample(role_id>0,1 rows),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option", []uint32{8, 1}},
		{"sample stored", "select sample(with_grant_option,1 rows) from mo_catalog.mo_role_privs", []uint32{1}},
		{"sample forwarded siblings", "select sample(role_id+1,100 percent),b,p from (select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) d", []uint32{0, 1, 8}},
		{"sample forwarded reordered", "select p,b,sample(role_id+1,100 percent) from (select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) d", []uint32{8, 1, 0}},
		{"sample forwarded repeated", "select sample(role_id+1,100 percent),p,b,p,b from (select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) d", []uint32{0, 8, 1, 8, 1}},
		{"sample forwarded multiple samples", "select sample(role_id+1,role_id+2,100 percent),b,p from (select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) d", []uint32{0, 0, 1, 8}},
		{"sample forwarded distinct input", "select sample(role_id+1,100 percent),b,p from (select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) d", []uint32{0, 1, 8}},
		{"sample forwarded derived", "select p,b,p,b from (select sample(role_id+1,100 percent) s,b,p from (select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs union all select role_id,with_grant_option b,role_id>0 p from mo_catalog.mo_role_privs) d) c", []uint32{8, 1, 8, 1}},
		{"grouped aggregate offset", "select min(role_id>0),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option", []uint32{8, 1}},
		{"multiple grouping slots", "select min(role_id>0),with_grant_option from mo_catalog.mo_role_privs group by with_grant_option,role_id", []uint32{8, 1}},
		{"distinct repeated predicate", "select distinct p,b,p,b from (select with_grant_option b,role_id>0 p,role_id n from mo_catalog.mo_role_privs) c", []uint32{8, 1, 8, 1}},
		{"minus membership", "select with_grant_option from mo_catalog.mo_role_privs minus select role_id>0 from mo_catalog.mo_role_privs", []uint32{1}},
		{"intersect membership", "select with_grant_option from mo_catalog.mo_role_privs intersect select role_id>0 from mo_catalog.mo_role_privs", []uint32{1}},
		{"minus computed left", "select role_id>0 from mo_catalog.mo_role_privs minus select with_grant_option from mo_catalog.mo_role_privs", []uint32{8}},
		{"intersect computed left", "select role_id>0 from mo_catalog.mo_role_privs intersect select with_grant_option from mo_catalog.mo_role_privs", []uint32{8}},
		{"minus stored control", "select with_grant_option from mo_catalog.mo_role_privs minus select with_grant_option from mo_catalog.mo_role_privs", []uint32{1}},
		{"intersect stored control", "select with_grant_option from mo_catalog.mo_role_privs intersect select with_grant_option from mo_catalog.mo_role_privs", []uint32{1}},
		{"recursive predicate", "with recursive c(n,p) as (select 1,1=1 union all select n+1,n<2 from c where n<3) select p from c", []uint32{8}},
		{"recursive stored control", "with recursive c(n,p) as (select 1,with_grant_option from mo_catalog.mo_role_privs union all select n+1,p from c where n<3) select p from c", []uint32{1}},
		{"recursive predicate after stored anchor", "with recursive c(n,p) as (select 1,with_grant_option from mo_catalog.mo_role_privs union all select n+1,n<2 from c where n<3) select p from c", []uint32{8}},
		{"recursive mixed slots", "with recursive c(n,b,p) as (select 1,with_grant_option,role_id is null from mo_catalog.mo_role_privs union all select n+1,b,n<2 from c where n<3) select b,p from c", []uint32{1, 8}},
		{"recursive reordered slots", "with recursive c(n,b,p) as (select 1,with_grant_option,role_id is null from mo_catalog.mo_role_privs union all select n+1,b,n<2 from c where n<3) select p,b from c", []uint32{8, 1}},
		{"recursive repeated slots", "with recursive c(n,b,p) as (select 1,with_grant_option,role_id is null from mo_catalog.mo_role_privs union all select n+1,b,n<2 from c where n<3) select p,b,p,b from c", []uint32{8, 1, 8, 1}},
		{"recursive distinct repeated slots", "with recursive c(n,b,p) as (select 1,with_grant_option,role_id is null from mo_catalog.mo_role_privs union all select n+1,b,n<2 from c where n<3) select distinct p,b,p,b from c", []uint32{8, 1, 8, 1}},
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
				if tc.lengths[i] == 0 {
					// These controls also project a non-BOOLEAN sampled column.
					require.NotEqual(t, int32(types.T_bool), def.Typ.Id)
					continue
				}
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

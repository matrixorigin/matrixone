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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestMarkJoinBooleanResultsUseNumericWireMetadata(t *testing.T) {
	mock := plan.NewMockOptimizer(false, newPlanTestProcess(t))
	const in = "role_id in (select n_nationkey from nation)"
	const source = " from mo_catalog.mo_role_privs"
	for _, tc := range []struct {
		name    string
		sql     string
		lengths []uint32
	}{
		{"in", "select " + in + source, []uint32{8}},
		{"any", "select role_id = any (select n_nationkey from nation)" + source, []uint32{8}},
		{"distinct", "select distinct " + in + source, []uint32{8}},
		{"stored first", "select with_grant_option, " + in + source, []uint32{boolColumnLength, 8}},
		{"stored last", "select " + in + ", with_grant_option" + source, []uint32{8, boolColumnLength}},
		{"two marks", "select " + in + ", with_grant_option, " + in + source, []uint32{8, boolColumnLength, 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := buildSingleSql(mock, t, tc.sql)
			require.NoError(t, err)
			columns := plan.GetResultColumnsFromPlan(query)
			require.Len(t, columns, len(tc.lengths))
			proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
			for i, colDef := range columns {
				column, err := colDef2MysqlColumn(t.Context(), colDef)
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

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

package frontend

import (
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestPreparedPublicationWireMetadata(t *testing.T) {
	for _, tt := range []struct {
		sql       string
		names     []string
		wireTypes []defines.MysqlType
	}{
		{sql: "alter publication pub account all"},
		{
			sql:       "show publications",
			names:     []string{"publication", "database", "tables", "sub_account", "subscribed_accounts", "create_time", "update_time", "comments"},
			wireTypes: []defines.MysqlType{defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TIMESTAMP, defines.MYSQL_TYPE_TIMESTAMP, defines.MYSQL_TYPE_BLOB},
		},
		{
			sql:       "show publication coverage pub",
			names:     []string{"Database", "Table"},
			wireTypes: []defines.MysqlType{defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VAR_STRING},
		},
	} {
		t.Run(tt.sql, func(t *testing.T) {
			conn := &prepareResponseCaptureConn{}
			proto, _, prepared := newBinaryPrepareProtocolTestCaseWithConn(t, tt.sql, conn)
			t.Cleanup(prepared.Close)
			proto.capability &^= CLIENT_DEPRECATE_EOF
			require.NoError(t, proto.SendPrepareResponse(t.Context(), prepared))
			packets := splitProtocolPackets(t, conn.writes)
			packetCount := 1
			if len(tt.names) > 0 {
				packetCount += len(tt.names) + 1
			}
			require.Len(t, packets, packetCount)
			require.Equal(t, uint16(len(tt.names)), binary.LittleEndian.Uint16(packets[0][5:]))
			require.Zero(t, binary.LittleEndian.Uint16(packets[0][7:]))
			for i, name := range tt.names {
				col := parsePrepareColumnDefinition(t, packets[i+1])
				require.Equal(t, name, col.name)
				require.Equal(t, tt.wireTypes[i], col.typ)
			}
		})
	}
}

func TestPreparedPublicationFrontendExecution(t *testing.T) {
	tests := []struct {
		sql   string
		names []string
		types []types.T
	}{
		{sql: "create publication pub database db account all"},
		{sql: "create publication if not exists pub database db account all"},
		{sql: "alter publication pub account all"},
		{sql: "drop publication pub"},
		{sql: "drop publication if exists pub"},
		{
			sql:   "show publications like 'pub%'",
			names: []string{"publication", "database", "tables", "sub_account", "subscribed_accounts", "create_time", "update_time", "comments"},
			types: []types.T{types.T_varchar, types.T_varchar, types.T_text, types.T_text, types.T_text, types.T_timestamp, types.T_timestamp, types.T_text},
		},
		{
			sql:   "show publication coverage pub",
			names: []string{"Database", "Table"},
			types: []types.T{types.T_varchar, types.T_varchar},
		},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			_, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 121, tt.sql)
			t.Cleanup(prepared.Close)
			inner := prepared.PreparePlan.GetDcl().GetPrepare()
			require.Empty(t, inner.ParamTypes)
			require.True(t, inner.Plan.IsPrepare)
			require.Nil(t, inner.Plan.Plan)

			columns := getPreparedResultColumns(prepared, false)
			require.Len(t, columns, len(tt.names))
			for i, column := range columns {
				require.Equal(t, tt.names[i], column.OriginName)
				require.Equal(t, int32(tt.types[i]), column.Typ.Id)
				_, err := colDef2MysqlColumn(execCtx.reqCtx, column)
				require.NoError(t, err)
			}
			compiled, err := cw.Compile(execCtx, nil)
			require.NoError(t, err)
			require.Nil(t, compiled)
			require.Nil(t, cw.compile)
			require.Equal(t, prepared.PrepareStmt, cw.stmt)
		})
	}
}

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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

func TestResultRowEncodingErrorEmitsOneTerminalPacket(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  CommandType
	}{
		{name: "text query", cmd: COM_QUERY},
		{name: "binary prepared", cmd: COM_STMT_EXECUTE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sv, err := getSystemVariables("test/system_vars_config.toml")
			require.NoError(t, err)
			pu := config.NewParameterUnit(sv, nil, nil, nil)
			raw := &testConn{}
			allocator := NewLeakCheckAllocator()
			t.Cleanup(func() { require.True(t, allocator.CheckBalance()) })
			ioses, err := NewIOSessionWithOptions(raw, pu, "", WithIOSessionAllocator(allocator))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ioses.Close()) })
			proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
			t.Cleanup(proto.Close)
			ses := &Session{}
			ses.SetCmd(tc.cmd)
			proto.ses = ses

			mrs := &MysqlResultSet{}
			column := &MysqlColumn{}
			column.SetName("value")
			column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
			mrs.AddColumn(column)

			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0))
			bat.SetRowCount(1)
			colSlices := &ColumnSlices{
				ctx: context.Background(), dataSet: bat, colIdx2SliceIdx: []int{0},
			}
			t.Cleanup(colSlices.Close)

			// Model a row encoder failure independently of the planner (#30016).
			err = proto.WriteResultSetRow2(mrs, colSlices, 1)
			require.ErrorContains(t, err, "invalid bytes based slice")
			require.Empty(t, raw.data, "the statement handler owns the terminal ERR packet")

			require.NoError(t, proto.WriteERR(1105, DefaultMySQLState, err.Error()))
			packets := splitProtocolPackets(t, raw.data)
			require.Len(t, packets, 1)
			require.Equal(t, byte(defines.ErrHeader), packets[0][0])
		})
	}
}

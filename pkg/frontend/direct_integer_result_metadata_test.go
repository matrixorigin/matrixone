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
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDirectIntegerBinaryRows(t *testing.T) {
	for _, vectorWriter := range []bool{false, true} {
		for _, null := range []bool{false, true} {
			t.Run(fmt.Sprintf("vector=%v/null=%v", vectorWriter, null), func(t *testing.T) {
				ctx := context.Background()
				conn := &prepareResponseCaptureConn{}
				proto, proc, _ := newBinaryPrepareProtocolTestCaseWithConn(t, "select 1", conn)
				rs := &MysqlResultSet{}
				for _, typ := range []defines.MysqlType{defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_LONG} {
					col := new(MysqlColumn)
					col.SetColumnType(typ)
					col.SetSigned(true)
					rs.AddColumn(col)
				}
				if vectorWriter {
					bat := batch.NewWithSize(2)
					defer bat.Clean(proc.Mp())
					for i, v := range []int32{-1, 73} {
						bat.Vecs[i] = vector.NewVec(types.T_int32.ToType())
						require.NoError(t, vector.AppendFixed(bat.Vecs[i], v, null && i == 0, proc.Mp()))
					}
					bat.SetRowCount(1)
					colSlices := &ColumnSlices{ctx: ctx, colIdx2SliceIdx: make([]int, 2), dataSet: bat}
					defer colSlices.Close()
					require.NoError(t, convertBatchToSlices(ctx, proto.GetSession(), bat, colSlices))
					require.NoError(t, proto.appendResultSetBinaryRow2(rs, colSlices, 0))
				} else {
					var value any = int32(-1)
					if null {
						value = nil
					}
					rs.AddRow([]any{value, int32(73)})
					require.NoError(t, proto.appendResultSetBinaryRow(rs, 0))
				}
				require.NoError(t, proto.flush())
				packets := splitProtocolPackets(t, conn.writes)
				require.Len(t, packets, 1)
				payload := packets[0]
				if null {
					require.Len(t, payload, 6)
					require.Equal(t, byte(4), payload[1])
				} else {
					require.Len(t, payload, 14)
					require.Zero(t, payload[1])
					require.Equal(t, int64(-1), int64(binary.LittleEndian.Uint64(payload[2:10])))
				}
				require.Equal(t, uint32(73), binary.LittleEndian.Uint32(payload[len(payload)-4:]))
			})
		}
	}
}

func TestDirectIntegerResultClassification(t *testing.T) {
	for _, tc := range []struct {
		sql   string
		count int
		want  []uint32
	}{
		{"select (strcmp('a','b')) as x, find_in_set('b','a,b') as y", 2, []uint32{2, 3}},
		{"select id, strcmp(s,'a') from t", 2, []uint32{0, 2}},
		{"select cast(strcmp(s,'a') as signed), strcmp(s,'a') + 1 from t", 2, nil},
		{"select distinct strcmp(s,'a') from t", 1, nil},
		{"select strcmp(s,'a') from t group by s", 1, nil},
		{"select strcmp('a','b') union all select strcmp('b','a')", 1, nil},
		{"select * from (select strcmp('a','b') c) a", 1, nil},
		{"select t.*, strcmp(s,'a') from t", 2, nil},
		{"select *, strcmp(s,'a') from t", 2, nil},
		{"select strcmp(s,'a') from t", 2, nil},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			cols := make([]*plan.ColDef, tc.count)
			for i := range cols {
				cols[i] = &plan.ColDef{Typ: plan.Type{Id: int32(types.T_int32)}}
			}
			require.Equal(t, tc.want, directIntegerResultLengths(stmt, cols))
			for _, col := range cols {
				col.Typ.Id = int32(types.T_int64)
			}
			require.Nil(t, directIntegerResultLengths(stmt, cols))
		})
	}
	// The grammar has a DISTINCTROW option, but the current keyword table maps
	// its spelling to UNUSED. Exercise the AST guard without expanding SQL
	// syntax support as part of this protocol fix.
	stmt, err := mysql.ParseOne(context.Background(), "select strcmp(s,'a') from t", 1)
	require.NoError(t, err)
	defer stmt.Free()
	stmt.(*tree.Select).Select.(*tree.SelectClause).Option = tree.QuerySpecOptionDistinctRow
	cols := []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32)}}}
	require.Nil(t, directIntegerResultLengths(stmt, cols))
}

func TestDirectIntegerPrepareWireMetadata(t *testing.T) {
	for _, query := range []string{
		"select find_in_set('b','a,b') pos, strcmp('a','b') cmp",
		"select find_in_set(?,'a,b') pos, strcmp(?,'b') cmp",
	} {
		t.Run(query, func(t *testing.T) {
			ctx := context.Background()
			conn := &prepareResponseCaptureConn{}
			proto, _, stmt := newBinaryPrepareProtocolTestCaseWithConn(t, query, conn)
			proto.capability &^= CLIENT_DEPRECATE_EOF
			cols := getPreparedResultColumns(stmt, false)
			require.Len(t, cols, 2)
			lengths := directIntegerResultLengths(stmt.PrepareStmt, cols)
			require.Equal(t, []uint32{3, 2}, lengths)
			for _, col := range cols {
				require.Equal(t, int32(types.T_int32), col.Typ.Id)
			}
			// The normal preparation/rebuild path precomputes the cached headers.
			cached, err := proto.MakeColumnDefData(ctx, cols, lengths...)
			require.NoError(t, err)
			for _, useCache := range []bool{false, true} {
				conn.writes = nil
				stmt.ColDefData = nil
				if useCache {
					stmt.ColDefData = cached
				}
				require.NoError(t, proto.SendPrepareResponse(ctx, stmt))
				packets := splitProtocolPackets(t, conn.writes)
				resultStart := len(packets) - 3
				for i, wantLength := range []uint32{3, 2} {
					col := parsePrepareColumnDefinition(t, packets[resultStart+i])
					require.Equal(t, defines.MYSQL_TYPE_LONGLONG, col.typ)
					require.Zero(t, col.flags&uint16(defines.UNSIGNED_FLAG))
					require.Equal(t, wantLength, col.length)
					require.Zero(t, col.decimals)
				}
			}
			wrapper := &TxnComputationWrapper{stmt: stmt.PrepareStmt, plan: stmt.PreparePlan.GetDcl().GetPrepare().GetPlan()}
			wire, internal, err := wrapper.getColumnsWithResultColumns(ctx)
			require.NoError(t, err)
			for i, col := range internal {
				require.Equal(t, int32(types.T_int32), col.Typ.Id)
				require.Equal(t, defines.MYSQL_TYPE_LONGLONG, wire[i].(*MysqlColumn).ColumnType())
			}
		})
	}
}

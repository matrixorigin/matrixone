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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestEnumSetColumnDefinitionMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source *pb.Type
		oid    types.T
		wire   defines.MysqlType
		length uint32
		flag   uint32
	}{
		{"enum", &pb.Type{Id: int32(types.T_enum), Enumvalues: "a,bb,"}, types.T_varchar, defines.MYSQL_TYPE_STRING, 8, defines.ENUM_FLAG},
		{"set", &pb.Type{Id: int32(types.T_uint64), Enumvalues: "x,yy"}, types.T_varchar, defines.MYSQL_TYPE_STRING, 16, defines.SET_FLAG},
		{"unicode enum", &pb.Type{Id: int32(types.T_enum), Enumvalues: "甲乙,😀"}, types.T_varchar, defines.MYSQL_TYPE_STRING, 8, defines.ENUM_FLAG},
		{"unicode set", &pb.Type{Id: int32(types.T_uint64), Enumvalues: "甲乙,😀"}, types.T_varchar, defines.MYSQL_TYPE_STRING, 16, defines.SET_FLAG},
		{"empty enum", &pb.Type{Id: int32(types.T_enum)}, types.T_varchar, defines.MYSQL_TYPE_STRING, 0, defines.ENUM_FLAG},
		{"empty set", &pb.Type{Id: int32(types.T_uint64), Enumvalues: ","}, types.T_varchar, defines.MYSQL_TYPE_STRING, 0, defines.SET_FLAG},
		{"plain varchar", nil, types.T_varchar, defines.MYSQL_TYPE_VAR_STRING, 12, 0},
		{"plain char", nil, types.T_char, defines.MYSQL_TYPE_STRING, 12, 0},
		{"plain uint64", nil, types.T_uint64, defines.MYSQL_TYPE_LONGLONG, 64, defines.UNSIGNED_FLAG},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := &pb.ColDef{
				Name: "label", Typ: pb.Type{Id: int32(tc.oid), Width: 3},
			}
			if tc.source != nil {
				def.MysqlSpecialResultType = &pb.MySQLSpecialResultType{Typ: *tc.source}
			}
			kinds := []pb.MySQLSpecialResultType_Kind{pb.MySQLSpecialResultType_COLUMN}
			if tc.source != nil {
				kinds = append(kinds, pb.MySQLSpecialResultType_AGGREGATE, pb.MySQLSpecialResultType_SCALAR_SUBQUERY, pb.MySQLSpecialResultType_ROLLUP,
					pb.MySQLSpecialResultType_ROLLUP_AGGREGATE, pb.MySQLSpecialResultType_CHAR_LABEL)
			}
			if tc.oid.IsMySQLString() {
				def.Typ.Charset = uint32(types.CharsetUTF8)
			}
			for _, kind := range kinds {
				if def.MysqlSpecialResultType != nil {
					def.MysqlSpecialResultType.Kind = kind
				}
				for _, notNull := range []bool{false, true} {
					def.Typ.NotNullable = notNull
					before, err := def.Marshal()
					require.NoError(t, err)
					var restored pb.ColDef
					require.NoError(t, restored.Unmarshal(before))
					col, err := colDef2MysqlColumn(t.Context(), &restored)
					require.NoError(t, err)
					protocol := &MysqlProtocolImpl{io: NewIOPackage(true)}
					cached, err := protocol.MakeColumnDefData(t.Context(), []*pb.ColDef{&restored})
					require.NoError(t, err)
					for _, cmd := range []CommandType{COM_QUERY, COM_STMT_PREPARE, COM_STMT_EXECUTE} {
						packet := protocol.makeColumnDefinition41Payload(col, int(cmd))
						require.Equal(t, cached[0], packet)
						wire := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
						require.Equal(t, tc.wire, wire.typ)
						require.Equal(t, tc.length, wire.length)
						flags := uint16(tc.flag)
						if kind != pb.MySQLSpecialResultType_COLUMN {
							flags = 0
						}
						if notNull {
							flags |= uint16(defines.NOT_NULL_FLAG)
						}
						require.Equal(t, flags, wire.flags)
						if tc.oid.IsMySQLString() {
							require.Equal(t, uint16(Utf8mb4CollationID), wire.charset)
						}
						require.Zero(t, wire.decimals)
					}
					after, err := def.Marshal()
					require.NoError(t, err)
					require.Equal(t, before, after)
				}
			}
		})
	}
}

func TestEnumSetLabelOnlyColumnHeader(t *testing.T) {
	for _, source := range []pb.Type{{Id: int32(types.T_enum), Enumvalues: "a,bb,"}, {Id: int32(types.T_uint64), Enumvalues: "x,yy"}} {
		for _, labelOnly := range []bool{false, true} {
			def := &pb.ColDef{Typ: pb.Type{Id: int32(types.T_varchar)}, MysqlSpecialResultType: &pb.MySQLSpecialResultType{
				Typ: source, Kind: pb.MySQLSpecialResultType_COLUMN, LabelOnly: labelOnly,
			}}
			data, err := def.Marshal()
			require.NoError(t, err)
			var restored pb.ColDef
			require.NoError(t, restored.Unmarshal(data))
			col, err := colDef2MysqlColumn(t.Context(), &restored)
			require.NoError(t, err)
			protocol := &MysqlProtocolImpl{io: NewIOPackage(true)}
			cached, err := protocol.MakeColumnDefData(t.Context(), []*pb.ColDef{&restored})
			require.NoError(t, err)
			for _, command := range []CommandType{COM_QUERY, COM_STMT_PREPARE, COM_STMT_EXECUTE} {
				packet := protocol.makeColumnDefinition41Payload(col, int(command))
				require.Equal(t, cached[0], packet)
				wire := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
				require.Equal(t, defines.MYSQL_TYPE_STRING, wire.typ)
				flags := uint16(defines.ENUM_FLAG)
				if source.Id == int32(types.T_uint64) {
					flags = uint16(defines.SET_FLAG)
				}
				if labelOnly {
					flags = 0
				}
				require.Equal(t, flags, wire.flags)
			}
			after, err := restored.Marshal()
			require.NoError(t, err)
			require.Equal(t, data, after)
		}
	}
}

func TestEnumSetPrepareOnlyMetadataDoesNotLeakIntoExecute(t *testing.T) {
	for _, kind := range []pb.MySQLSpecialResultType_Kind{pb.MySQLSpecialResultType_PREPARE_ONLY, pb.MySQLSpecialResultType_VARCHAR_LABEL,
		pb.MySQLSpecialResultType_SCALAR_SUBQUERY, pb.MySQLSpecialResultType_ROLLUP,
		pb.MySQLSpecialResultType_ROLLUP_AGGREGATE, pb.MySQLSpecialResultType_CHAR_LABEL} {
		t.Run(kind.String(), func(t *testing.T) {
			conn := &prepareResponseCaptureConn{}
			protocol, _, stmt := newBinaryPrepareProtocolTestCaseWithConn(t, "select min('a') over () e, max('x') over () s from (select 1 n union all select 2 n) d", conn)
			protocol.capability &^= CLIENT_DEPRECATE_EOF
			query := stmt.PreparePlan.GetDcl().GetPrepare().GetPlan().GetQuery()
			query.MysqlSpecialResultTypes = []pb.MySQLSpecialResultType{
				{Typ: pb.Type{Id: int32(types.T_enum), Enumvalues: "a,bb"}, Kind: kind},
				{Typ: pb.Type{Id: int32(types.T_uint64), Enumvalues: "x,yy"}, Kind: kind},
			}
			before, err := query.Marshal()
			require.NoError(t, err)
			columns := getPreparedResultColumns(stmt, false)
			cached, err := protocol.MakeColumnDefData(t.Context(), columns)
			require.NoError(t, err)
			for _, useCache := range []bool{false, true, false} {
				conn.writes = nil
				stmt.ColDefData = nil
				if useCache {
					stmt.ColDefData = cached
				}
				require.NoError(t, protocol.SendPrepareResponse(t.Context(), stmt))
				packets := splitProtocolPackets(t, conn.writes)
				for i, width := range []uint32{8, 16} {
					prepare := parsePrepareColumnDefinition(t, packets[len(packets)-3+i])
					want := defines.MYSQL_TYPE_STRING
					if kind == pb.MySQLSpecialResultType_VARCHAR_LABEL {
						want = defines.MYSQL_TYPE_VAR_STRING
					}
					require.Equal(t, want, prepare.typ)
					require.Equal(t, width, prepare.length)
					require.Zero(t, prepare.flags&uint16(defines.ENUM_FLAG|defines.SET_FLAG))
					for _, cmd := range []CommandType{COM_QUERY, COM_STMT_EXECUTE} {
						column, err := colDef2MysqlColumn(t.Context(), columns[i])
						require.NoError(t, err)
						packet := protocol.makeColumnDefinition41Payload(column, int(cmd))
						execute := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
						wantExecute := defines.MYSQL_TYPE_VAR_STRING
						if kind == pb.MySQLSpecialResultType_SCALAR_SUBQUERY || kind == pb.MySQLSpecialResultType_ROLLUP ||
							kind == pb.MySQLSpecialResultType_ROLLUP_AGGREGATE || kind == pb.MySQLSpecialResultType_CHAR_LABEL {
							wantExecute = defines.MYSQL_TYPE_STRING
						}
						require.Equal(t, wantExecute, execute.typ)
						require.Equal(t, width, execute.length)
						require.Zero(t, execute.flags&uint16(defines.ENUM_FLAG|defines.SET_FLAG))
					}
				}
			}
			// Exercise the execute response owner too: its input cache is the
			// statement's CHAR prepare packet, not an independently built packet.
			conn.writes = nil
			ses := protocol.GetSession()
			ses.SetCmd(COM_STMT_EXECUTE)
			ses.SetMysqlResultSet(&MysqlResultSet{})
			ctx := &ExecCtx{reqCtx: t.Context(), prepareStmt: stmt, prepareColDef: cached, input: &UserInput{isCursorExecute: true}}
			wireColumns := make([]any, len(columns))
			for i, column := range columns {
				wireColumns[i], err = colDef2MysqlColumn(t.Context(), column)
				require.NoError(t, err)
			}
			applyCursorEnumSetResultMetadata(wireColumns, columns)
			require.NoError(t, NewMysqlResp(protocol).writeColumnDefs(ses, ctx, wireColumns))
			require.NoError(t, protocol.flush())
			packets := splitProtocolPackets(t, conn.writes)
			for i := range columns {
				wire := parsePrepareColumnDefinition(t, packets[i+1])
				wantCursor := defines.MYSQL_TYPE_VAR_STRING
				if kind == pb.MySQLSpecialResultType_ROLLUP_AGGREGATE || kind == pb.MySQLSpecialResultType_CHAR_LABEL {
					wantCursor = defines.MYSQL_TYPE_STRING
				}
				require.Equal(t, wantCursor, wire.typ)
			}
			require.Equal(t, cached, stmt.ColDefData, "execute must leave prepare metadata intact")
			after, err := query.Marshal()
			require.NoError(t, err)
			require.Equal(t, before, after, "publication must not specialize the cached execution plan")
		})
	}
}

func TestEnumSetExecutionParametersReachMetadataOwner(t *testing.T) {
	_, _, prepared := newBinaryPrepareProtocolTestCase(t, "select min('a') over(),max('x') over() from (select 1 n union all select 2 n) d limit ?")
	p := prepared.PreparePlan.GetDcl().GetPrepare().GetPlan()
	p.GetQuery().MysqlSpecialResultTypes = []pb.MySQLSpecialResultType{
		{Typ: pb.Type{Id: int32(types.T_enum), Enumvalues: "a,bb"}, Kind: pb.MySQLSpecialResultType_PREPARE_ONLY},
		{Typ: pb.Type{Id: int32(types.T_uint64), Enumvalues: "x,yy"}, Kind: pb.MySQLSpecialResultType_PREPARE_ONLY},
	}
	snapshot := proto.Clone(p)
	wrapper := &TxnComputationWrapper{stmt: prepared.PrepareStmt, plan: p}
	for _, value := range []any{"0", "1", nil, false, true, "0"} {
		kind := vector.PrepareParamInteger
		if _, boolean := value.(bool); boolean {
			kind = vector.PrepareParamBoolean
		}
		wrapper.paramVals = []any{plan2.ParamValue{Value: value, IsBinaryProtocol: true, PrepareParamKind: kind}}
		zero := value == "0" || value == nil || value == false
		columns, defs, err := wrapper.getColumnsWithResultColumns(t.Context())
		require.NoError(t, err)
		legacy, err := wrapper.GetColumns(t.Context())
		require.NoError(t, err)
		want := defines.MYSQL_TYPE_VAR_STRING
		if zero {
			want = defines.MYSQL_TYPE_STRING
		}
		for i := range columns {
			require.Equal(t, want, columns[i].(*MysqlColumn).ColumnType())
			require.Equal(t, want, legacy[i].(*MysqlColumn).ColumnType())
			require.Equal(t, zero, defs[i].MysqlSpecialResultType.MaterializationElided)
		}
		require.True(t, proto.Equal(snapshot, p))
	}
}

func TestEnumSetConstantEmptyAndCursorMetadata(t *testing.T) {
	for _, kind := range []pb.MySQLSpecialResultType_Kind{pb.MySQLSpecialResultType_PREPARE_ONLY, pb.MySQLSpecialResultType_VARCHAR_LABEL,
		pb.MySQLSpecialResultType_SCALAR_SUBQUERY, pb.MySQLSpecialResultType_ROLLUP} {
		for _, elided := range []bool{false, true} {
			def := &pb.ColDef{Typ: pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)},
				MysqlSpecialResultType: &pb.MySQLSpecialResultType{Typ: pb.Type{Id: int32(types.T_enum), Enumvalues: "a,bb"}, Kind: kind, MaterializationElided: elided}}
			before, err := def.Marshal()
			require.NoError(t, err)
			for _, preparing := range []bool{false, true, false} {
				column, err := colDef2MysqlColumn(t.Context(), def, preparing)
				require.NoError(t, err)
				want := defines.MYSQL_TYPE_VAR_STRING
				if kind == pb.MySQLSpecialResultType_PREPARE_ONLY && (preparing || elided) ||
					kind == pb.MySQLSpecialResultType_SCALAR_SUBQUERY || kind == pb.MySQLSpecialResultType_ROLLUP {
					want = defines.MYSQL_TYPE_STRING
				}
				require.Equal(t, want, column.ColumnType())
				require.Equal(t, uint32(8), column.Length())
				require.Zero(t, column.Flag()&uint16(defines.ENUM_FLAG|defines.SET_FLAG))
				if !preparing {
					applyCursorEnumSetResultMetadata([]any{column}, []*pb.ColDef{def})
					require.Equal(t, defines.MYSQL_TYPE_VAR_STRING, column.ColumnType())
				}
			}
			after, err := def.Marshal()
			require.NoError(t, err)
			require.Equal(t, before, after)
		}
	}
}

func TestEnumSetResultMetadataPreservesCollation(t *testing.T) {
	for _, tc := range []struct {
		charset uint8
		wire    uint16
		length  uint32
	}{
		{types.CharsetLegacy, charsetVarchar, 6},
		{types.CharsetUTF8, uint16(Utf8mb4CollationID), 8},
		{types.CharsetUTF8MB4Bin, uint16(utf8mb4BinCollationID), 8},
	} {
		col, err := colDef2MysqlColumn(t.Context(), &pb.ColDef{
			Typ:                    pb.Type{Id: int32(types.T_varchar), Charset: uint32(tc.charset)},
			MysqlSpecialResultType: &pb.MySQLSpecialResultType{Typ: pb.Type{Id: int32(types.T_enum), Enumvalues: "甲乙"}},
		})
		require.NoError(t, err)
		require.Equal(t, tc.wire, col.Charset())
		require.Equal(t, tc.length, col.Length())
	}
}

func TestEnumSetResultMetadataRejectsInvalidDomain(t *testing.T) {
	for _, presentation := range []*pb.MySQLSpecialResultType{
		{Typ: pb.Type{Id: int32(types.T_varchar)}},
		{Typ: pb.Type{Id: int32(types.T_uint64)}},
		{Typ: pb.Type{Id: int32(types.T_enum)}, Kind: 999},
	} {
		_, err := colDef2MysqlColumn(t.Context(), &pb.ColDef{
			Typ: pb.Type{Id: int32(types.T_varchar)}, MysqlSpecialResultType: presentation,
		})
		require.Error(t, err)
	}
}

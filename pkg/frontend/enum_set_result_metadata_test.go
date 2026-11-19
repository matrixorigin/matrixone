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
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		{"unicode set", &pb.Type{Id: int32(types.T_uint64), Enumvalues: "x,😀"}, types.T_varchar, defines.MYSQL_TYPE_STRING, 12, defines.SET_FLAG},
		{"empty enum", &pb.Type{Id: int32(types.T_enum)}, types.T_varchar, defines.MYSQL_TYPE_STRING, 0, defines.ENUM_FLAG},
		{"empty set", &pb.Type{Id: int32(types.T_uint64), Enumvalues: ","}, types.T_varchar, defines.MYSQL_TYPE_STRING, 0, defines.SET_FLAG},
		{"varchar control", nil, types.T_varchar, defines.MYSQL_TYPE_VAR_STRING, 12, 0},
		{"char control", nil, types.T_char, defines.MYSQL_TYPE_STRING, 12, 0},
		{"uint64 control", nil, types.T_uint64, defines.MYSQL_TYPE_LONGLONG, 64, defines.UNSIGNED_FLAG},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := &pb.ColDef{Name: "label", Typ: pb.Type{Id: int32(tc.oid), Width: 3}, EnumSetResultType: tc.source}
			if tc.oid.IsMySQLString() {
				def.Typ.Charset = uint32(types.CharsetUTF8)
			}
			for _, notNull := range []bool{false, true} {
				def.Typ.NotNullable = notNull
				data, err := def.Marshal()
				require.NoError(t, err)
				var restored pb.ColDef
				require.NoError(t, restored.Unmarshal(data))
				snapshot := proto.Clone(&restored)
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
					if notNull {
						flags |= uint16(defines.NOT_NULL_FLAG)
					}
					require.Equal(t, flags, wire.flags)
					if tc.oid.IsMySQLString() {
						require.Equal(t, uint16(Utf8mb4CollationID), wire.charset)
					}
					require.Zero(t, wire.decimals)
				}
				require.True(t, proto.Equal(snapshot, &restored), "publication must not mutate cached metadata")
			}
		})
	}
}

func TestEnumSetResultMetadataCollation(t *testing.T) {
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
			Typ:               pb.Type{Id: int32(types.T_varchar), Charset: uint32(tc.charset)},
			EnumSetResultType: &pb.Type{Id: int32(types.T_enum), Enumvalues: "甲乙"},
		})
		require.NoError(t, err)
		require.Equal(t, tc.wire, col.Charset())
		require.Equal(t, tc.length, col.Length())
	}
}

func TestEnumSetResultMetadataInvalidDomain(t *testing.T) {
	for _, source := range []*pb.Type{{Id: int32(types.T_varchar)}, {Id: int32(types.T_uint64)}} {
		_, err := colDef2MysqlColumn(t.Context(), &pb.ColDef{
			Typ: pb.Type{Id: int32(types.T_varchar)}, EnumSetResultType: source,
		})
		require.Error(t, err)
	}
}

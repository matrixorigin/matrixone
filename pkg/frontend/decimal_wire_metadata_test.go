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
	"context"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// TestDecimalColumnDefinitionWireMetadata proves that DECIMAL engine types keep
// the legacy MYSQL_TYPE_DECIMAL identifier on internal MysqlColumn objects while
// every outgoing ColumnDefinition41 packet advertises MYSQL_TYPE_NEWDECIMAL,
// matching MySQL 8 wire metadata (issue #30283). The display length, scale,
// charset and flags must survive the translation unchanged.
func TestDecimalColumnDefinitionWireMetadata(t *testing.T) {
	cases := []struct {
		name      string
		typ       types.Type
		wantWire  byte
		wantLen   uint32
		wantScale uint8
	}{
		// Issue reproduction: SELECT CAST(1.123456 AS DECIMAL(24,6)).
		{name: "issue decimal(24,6) decimal128", typ: types.New(types.T_decimal128, 24, 6), wantWire: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantLen: 26, wantScale: 6},
		{name: "decimal64 max precision scale 0", typ: types.New(types.T_decimal64, 18, 0), wantWire: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantLen: 19, wantScale: 0},
		{name: "decimal64 minimum", typ: types.New(types.T_decimal64, 1, 0), wantWire: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantLen: 2, wantScale: 0},
		{name: "decimal128 lower boundary", typ: types.New(types.T_decimal128, 19, 3), wantWire: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantLen: 21, wantScale: 3},
		{name: "decimal128 max precision", typ: types.New(types.T_decimal128, 38, 10), wantWire: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantLen: 40, wantScale: 10},
		{name: "decimal256 lower boundary", typ: types.New(types.T_decimal256, 39, 2), wantWire: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantLen: 41, wantScale: 2},
		{name: "decimal256 supported precision 65", typ: types.New(types.T_decimal256, 65, 30), wantWire: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantLen: 67, wantScale: 30},
		{name: "decimal256 max scale equals precision", typ: types.New(types.T_decimal256, 65, 65), wantWire: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantLen: 67, wantScale: 65},
	}

	proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			col, err := colDef2MysqlColumn(context.Background(), &plan2.ColDef{
				Name: "metric_value",
				Typ: plan2.Type{
					Id:    int32(tt.typ.Oid),
					Width: tt.typ.Width,
					Scale: tt.typ.Scale,
				},
			})
			require.NoError(t, err)
			// The internal column keeps the legacy identifier; normalization is
			// a wire-boundary responsibility only.
			require.Equal(t, defines.MYSQL_TYPE_DECIMAL, col.ColumnType())
			require.Equal(t, tt.wantLen, col.Length())
			require.Equal(t, tt.wantScale, col.Decimal())

			packet := proto.makeColumnDefinition41Payload(col, int(COM_QUERY))
			definition := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
			require.Equal(t, "metric_value", definition.name)
			require.Equal(t, defines.MysqlType(tt.wantWire), definition.typ)
			require.Equal(t, tt.wantLen, definition.length)
			require.Equal(t, tt.wantScale, definition.decimals)
			require.Equal(t, uint16(charsetBinary), definition.charset)

			// Repeated serialization must not mutate the source column and must
			// produce byte-identical packets.
			again := proto.makeColumnDefinition41Payload(col, int(COM_QUERY))
			require.Equal(t, packet, again)
			require.Equal(t, defines.MYSQL_TYPE_DECIMAL, col.ColumnType())
			require.Equal(t, tt.wantLen, col.Length())
			require.Equal(t, tt.wantScale, col.Decimal())
		})
	}
}

// TestColumnDefinitionWireMetadataControls proves the wire normalization is
// scoped to MYSQL_TYPE_DECIMAL: an already-modern NEWDECIMAL column stays
// unchanged, and unrelated types keep their established wire identifiers,
// including the pre-existing BOOL-to-TINY boundary translation.
func TestColumnDefinitionWireMetadataControls(t *testing.T) {
	proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
	cases := []struct {
		name        string
		typ         byte
		want        byte
		wantCharset uint16
		wantLength  uint32
	}{
		{name: "preconstructed newdecimal stays", typ: byte(defines.MYSQL_TYPE_NEWDECIMAL), want: byte(defines.MYSQL_TYPE_NEWDECIMAL), wantCharset: uint16(charsetBinary), wantLength: 22},
		{name: "tiny unchanged", typ: byte(defines.MYSQL_TYPE_TINY), want: byte(defines.MYSQL_TYPE_TINY), wantCharset: uint16(charsetBinary), wantLength: 1},
		{name: "long unchanged", typ: byte(defines.MYSQL_TYPE_LONG), want: byte(defines.MYSQL_TYPE_LONG), wantCharset: uint16(charsetBinary), wantLength: 4},
		{name: "double unchanged", typ: byte(defines.MYSQL_TYPE_DOUBLE), want: byte(defines.MYSQL_TYPE_DOUBLE), wantCharset: uint16(charsetBinary), wantLength: 8},
		{name: "varchar unchanged", typ: byte(defines.MYSQL_TYPE_VARCHAR), want: byte(defines.MYSQL_TYPE_VARCHAR), wantCharset: uint16(charsetBinary), wantLength: 128},
		{name: "blob unchanged", typ: byte(defines.MYSQL_TYPE_BLOB), want: byte(defines.MYSQL_TYPE_BLOB), wantCharset: uint16(charsetBinary), wantLength: 1024},
		{name: "date unchanged", typ: byte(defines.MYSQL_TYPE_DATE), want: byte(defines.MYSQL_TYPE_DATE), wantCharset: uint16(charsetBinary), wantLength: 10},
		{name: "null unchanged", typ: byte(defines.MYSQL_TYPE_NULL), want: byte(defines.MYSQL_TYPE_NULL), wantCharset: uint16(charsetBinary), wantLength: 0},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			col := new(MysqlColumn)
			col.SetName("c")
			col.SetColumnType(defines.MysqlType(tt.typ))
			col.SetLength(tt.wantLength)
			packet := proto.makeColumnDefinition41Payload(col, int(COM_QUERY))
			definition := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
			require.Equal(t, defines.MysqlType(tt.want), definition.typ)
			require.Equal(t, tt.wantLength, definition.length)
			require.Equal(t, defines.MysqlType(tt.typ), col.ColumnType())
		})
	}

	t.Run("bool keeps tiny wire translation", func(t *testing.T) {
		col := new(MysqlColumn)
		col.SetName("b")
		col.SetColumnType(defines.MYSQL_TYPE_BOOL)
		col.SetLength(1)
		packet := proto.makeColumnDefinition41Payload(col, int(COM_QUERY))
		definition := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
		require.Equal(t, defines.MYSQL_TYPE_TINY, definition.typ)
		require.Equal(t, uint16(charsetBinary), definition.charset)
		require.Equal(t, uint32(boolColumnLength), definition.length)
		require.Equal(t, defines.MYSQL_TYPE_BOOL, col.ColumnType())
	})
}

// TestDecimalWireMetadataFromSql builds a real query plan for the issue's
// reproduction and verifies the exact metadata MySQL 8 clients observe for the
// ordinary SELECT (COM_QUERY) path.
func TestDecimalWireMetadataFromSql(t *testing.T) {
	mock := plan.NewMockOptimizer(false, newPlanTestProcess(t))
	queryPlan, err := buildSingleSql(mock, t,
		"select cast(1.123456 as decimal(24,6)) as metric_value")
	require.NoError(t, err)

	columns := plan.GetResultColumnsFromPlan(queryPlan)
	require.Len(t, columns, 1)
	require.Equal(t, "metric_value", columns[0].Name)

	mysqlColumn, err := colDef2MysqlColumn(context.Background(), columns[0])
	require.NoError(t, err)
	require.Equal(t, defines.MYSQL_TYPE_DECIMAL, mysqlColumn.ColumnType())
	require.Equal(t, uint32(26), mysqlColumn.Length())
	require.Equal(t, uint8(6), mysqlColumn.Decimal())

	proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
	packet := proto.makeColumnDefinition41Payload(mysqlColumn, int(COM_QUERY))
	definition := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
	require.Equal(t, "metric_value", definition.name)
	require.Equal(t, defines.MYSQL_TYPE_NEWDECIMAL, definition.typ)
	require.Equal(t, uint32(26), definition.length)
	require.Equal(t, uint8(6), definition.decimals)
}

// TestDecimalPrepareAndCachedMetadata covers the COM_STMT_PREPARE response:
// the first send serializes fresh column definitions, the second reuses the
// cached ColDefData bytes. Both must advertise NEWDECIMAL, and the cached
// packets must be byte-identical to the fresh ones. Prepared DECIMAL parameter
// columns must advertise NEWDECIMAL as well.
func TestDecimalPrepareAndCachedMetadata(t *testing.T) {
	ctx := context.TODO()
	conn := &prepareResponseCaptureConn{}
	proto, proc, prepareStmt := newBinaryPrepareProtocolTestCaseWithConn(t,
		"select cast(1.123456 as decimal(24,6)) as metric_value", conn)
	proto.capability &^= CLIENT_DEPRECATE_EOF
	defer func() {
		proc.SetPrepareParams(nil)
		prepareStmt.clearBinaryParamState(proc)
	}()

	require.NoError(t, proto.SendPrepareResponse(ctx, prepareStmt))
	first := append([]byte(nil), conn.writes...)
	require.NoError(t, proto.SendPrepareResponse(ctx, prepareStmt))
	second := conn.writes[len(first):]

	firstPackets := splitProtocolPackets(t, first)
	secondPackets := splitProtocolPackets(t, second)
	// header, column definition, EOF; no result parameters are declared.
	require.Len(t, firstPackets, 3)
	require.Len(t, secondPackets, 3)

	firstColumn := parsePrepareColumnDefinition(t, firstPackets[1])
	require.Equal(t, "metric_value", firstColumn.name)
	require.Equal(t, defines.MYSQL_TYPE_NEWDECIMAL, firstColumn.typ)
	require.Equal(t, uint32(26), firstColumn.length)
	require.Equal(t, uint8(6), firstColumn.decimals)

	require.Equal(t, firstPackets[1], secondPackets[1],
		"cached prepare column definitions must match the fresh serialization")

	t.Run("decimal parameter columns advertise newdecimal", func(t *testing.T) {
		paramConn := &prepareResponseCaptureConn{}
		paramProto, paramProc, paramStmt := newBinaryPrepareProtocolTestCaseWithConn(t,
			"select cast(? + 1 as decimal(30,4)) as metric_value", paramConn)
		paramProto.capability &^= CLIENT_DEPRECATE_EOF
		defer func() {
			paramProc.SetPrepareParams(nil)
			paramStmt.clearBinaryParamState(paramProc)
		}()

		// Declare the parameter type as decimal, as an external client binding does.
		prepare := paramStmt.PreparePlan.GetDcl().GetPrepare()
		prepare.ParamTypes = []int32{int32(types.T_decimal128)}
		require.NoError(t, paramProto.SendPrepareResponse(ctx, paramStmt))

		packets := splitProtocolPackets(t, paramConn.writes)
		require.Len(t, packets, 5)
		parameter := parsePrepareColumnDefinition(t, packets[1])
		require.Equal(t, "?", parameter.name)
		require.Equal(t, defines.MYSQL_TYPE_NEWDECIMAL, parameter.typ)
		result := parsePrepareColumnDefinition(t, packets[3])
		require.Equal(t, "metric_value", result.name)
		require.Equal(t, defines.MYSQL_TYPE_NEWDECIMAL, result.typ)
	})
}

// TestDecimalMakeColumnDefData covers the MakeColumnDefData serialization used
// for cached prepare and pre-serialized column definitions outside
// SendPrepareResponse.
func TestDecimalMakeColumnDefData(t *testing.T) {
	proto := &MysqlProtocolImpl{io: NewIOPackage(true)}
	cols := []*plan2.ColDef{
		{Name: "d64", Typ: plan2.Type{Id: int32(types.T_decimal64), Width: 18, Scale: 2}},
		{Name: "d128", Typ: plan2.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 4}},
		{Name: "d256", Typ: plan2.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 0}},
		{Name: "i", Typ: plan2.Type{Id: int32(types.T_int32)}},
	}
	data, err := proto.MakeColumnDefData(context.Background(), cols)
	require.NoError(t, err)
	require.Len(t, data, len(cols))

	expect := []struct {
		name  string
		typ   byte
		scale uint8
	}{
		{name: "d64", typ: byte(defines.MYSQL_TYPE_NEWDECIMAL), scale: 2},
		{name: "d128", typ: byte(defines.MYSQL_TYPE_NEWDECIMAL), scale: 4},
		{name: "d256", typ: byte(defines.MYSQL_TYPE_NEWDECIMAL), scale: 0},
		{name: "i", typ: byte(defines.MYSQL_TYPE_LONG), scale: 0},
	}
	for i, packet := range data {
		definition := parsePrepareColumnDefinition(t, packet[HeaderOffset:])
		require.Equal(t, expect[i].name, definition.name)
		require.Equal(t, defines.MysqlType(expect[i].typ), definition.typ)
		require.Equal(t, expect[i].scale, definition.decimals)
	}

}

// decodedBinaryValue is one independently decoded field of a binary result
// row, decoded with only the advertised wire metadata as a client would.
type decodedBinaryValue struct {
	null bool
	text string
}

// decodeBinaryResultRow decodes one binary-protocol result row packet payload
// using only the advertised wire column definitions, the same way a MySQL
// client does after COM_STMT_EXECUTE: the 0x00 header, the offset-2 NULL
// bitmap, then each non-NULL field encoded according to its wire type.
func decodeBinaryResultRow(t *testing.T, payload []byte, defs []prepareColumnDefinition) []decodedBinaryValue {
	t.Helper()
	require.NotEmpty(t, payload)
	require.Equal(t, byte(defines.OKHeader), payload[0])
	pos := 1
	nullBitmapLen := (len(defs) + 7 + 2) / 8
	require.GreaterOrEqual(t, len(payload), pos+nullBitmapLen)
	nullBitmap := payload[pos : pos+nullBitmapLen]
	pos += nullBitmapLen

	values := make([]decodedBinaryValue, len(defs))
	for i, definition := range defs {
		bit := i + 2
		if nullBitmap[bit/8]&(1<<uint(bit%8)) != 0 {
			values[i] = decodedBinaryValue{null: true}
			continue
		}
		switch definition.typ {
		case defines.MYSQL_TYPE_NEWDECIMAL:
			// MySQL 8 ships NEWDECIMAL binary values as length-encoded strings.
			values[i] = decodedBinaryValue{text: readPrepareLenEncString(t, payload, &pos)}
		case defines.MYSQL_TYPE_TINY:
			require.GreaterOrEqual(t, len(payload)-pos, 1)
			values[i] = decodedBinaryValue{text: strconv.FormatInt(int64(int8(payload[pos])), 10)}
			pos++
		case defines.MYSQL_TYPE_SHORT:
			require.GreaterOrEqual(t, len(payload)-pos, 2)
			values[i] = decodedBinaryValue{text: strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(payload[pos:]))), 10)}
			pos += 2
		case defines.MYSQL_TYPE_LONG:
			require.GreaterOrEqual(t, len(payload)-pos, 4)
			values[i] = decodedBinaryValue{text: strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(payload[pos:]))), 10)}
			pos += 4
		case defines.MYSQL_TYPE_LONGLONG:
			require.GreaterOrEqual(t, len(payload)-pos, 8)
			values[i] = decodedBinaryValue{text: strconv.FormatInt(int64(binary.LittleEndian.Uint64(payload[pos:])), 10)}
			pos += 8
		default:
			t.Fatalf("decodeBinaryResultRow: unsupported control wire type %v", definition.typ)
		}
	}
	require.Equal(t, len(payload), pos, "decoded row must consume the whole packet")
	return values
}

// TestDecimalPrepareExecuteBinaryRoundTrip completes the COM_STMT_PREPARE ->
// repeated COM_STMT_EXECUTE compatibility proof for issue #30283 at the
// protocol encoder level. The prepare phase serializes the column definitions
// a client caches; the execute phase row-encodes through the same internal
// columns that kept the legacy MYSQL_TYPE_DECIMAL identifier. A client
// decodes every binary row trusting the advertised NEWDECIMAL metadata, so
// this test independently decodes the captured packets with only that
// metadata, covering exact decimal text, a NULL decimal field and a
// neighboring non-decimal column across two executions of the statement.
func TestDecimalPrepareExecuteBinaryRoundTrip(t *testing.T) {
	ctx := context.TODO()
	conn := &prepareResponseCaptureConn{}
	proto, proc, prepareStmt := newBinaryPrepareProtocolTestCaseWithConn(t,
		"select cast(1.234 as decimal(18,3)) as metric_value, 1 as neighbor", conn)
	proto.capability &^= CLIENT_DEPRECATE_EOF
	defer func() {
		proc.SetPrepareParams(nil)
		prepareStmt.clearBinaryParamState(proc)
	}()

	// Phase 1: COM_STMT_PREPARE. The client caches these column definitions.
	require.NoError(t, proto.SendPrepareResponse(ctx, prepareStmt))
	preparePackets := splitProtocolPackets(t, conn.writes)
	// prepare header, two column definitions, EOF; no parameters are declared.
	require.Len(t, preparePackets, 4)
	decimalDef := parsePrepareColumnDefinition(t, preparePackets[1])
	require.Equal(t, "metric_value", decimalDef.name)
	require.Equal(t, defines.MYSQL_TYPE_NEWDECIMAL, decimalDef.typ)
	require.Equal(t, uint32(20), decimalDef.length) // MySQL display length: precision + 2 for sign and point
	require.Equal(t, uint8(3), decimalDef.decimals)
	neighborDef := parsePrepareColumnDefinition(t, preparePackets[2])
	require.Equal(t, "neighbor", neighborDef.name)
	require.Contains(t, []defines.MysqlType{
		defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT,
		defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_LONGLONG,
	}, neighborDef.typ)
	wireDefs := []prepareColumnDefinition{decimalDef, neighborDef}

	// Phase 2: COM_STMT_EXECUTE. The internal result columns still carry the
	// legacy identifier the row encoder dispatches on; only the wire type
	// changed. The rows include a NULL decimal field and exact texts with
	// sign, zero and trailing zeros.
	// Source the execute-side columns exactly the way SendPrepareResponse
	// does, so the row encoder below runs against the same objects.
	columns := getPreparedResultColumns(prepareStmt, sessionTxnHaveDDL(proto.GetSession()))
	require.Len(t, columns, 2)
	decimalColumn, err := colDef2MysqlColumn(ctx, columns[0])
	require.NoError(t, err)
	neighborColumn, err := colDef2MysqlColumn(ctx, columns[1])
	require.NoError(t, err)
	require.Equal(t, defines.MYSQL_TYPE_DECIMAL, decimalColumn.ColumnType())
	require.Equal(t, defines.MysqlType(uint8(neighborDef.typ)), neighborColumn.ColumnType())

	mrs := &MysqlResultSet{}
	mrs.AddColumn(decimalColumn)
	mrs.AddColumn(neighborColumn)
	rows := [][]any{
		{"1.234", int64(1)},
		{"-1.234", int64(-2)},
		{"0.000", int64(0)},
		{nil, int64(7)},
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	wantDecimals := []decodedBinaryValue{
		{text: "1.234"},
		{text: "-1.234"},
		{text: "0.000"},
		{null: true},
	}
	wantNeighbors := []string{"1", "-2", "0", "7"}

	// Two executions of the same statement must produce byte-identical row
	// packets; the client keeps using the cached NEWDECIMAL metadata
	// (byte-identical to the fresh one, proven in
	// TestDecimalPrepareAndCachedMetadata) for both.
	executions := make([][][]byte, 2)
	for exec := 0; exec < 2; exec++ {
		for rowIdx := range rows {
			before := len(conn.writes)
			require.NoError(t, proto.appendResultSetBinaryRow(mrs, uint64(rowIdx)))
			require.NoError(t, proto.flush())
			rowPackets := splitProtocolPackets(t, conn.writes[before:])
			require.Len(t, rowPackets, 1)
			executions[exec] = append(executions[exec], rowPackets[0])
		}
	}
	require.Equal(t, executions[0], executions[1],
		"repeated execution of one prepared statement must emit identical rows")

	for rowIdx, payload := range executions[1] {
		values := decodeBinaryResultRow(t, payload, wireDefs)
		require.Equal(t, wantDecimals[rowIdx], values[0], "row %d decimal field", rowIdx)
		require.False(t, values[1].null, "row %d neighbor field", rowIdx)
		require.Equal(t, wantNeighbors[rowIdx], values[1].text, "row %d neighbor field", rowIdx)
	}
}

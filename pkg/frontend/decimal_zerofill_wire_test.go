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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// zerofillDecimalColDef builds the plan column definition of an attributed
// DECIMAL(10,2) projection the way the planner produces it for a user SELECT
// of an attributed column (#30940): only the numeric attribute mask
// distinguishes it from a plain signed DECIMAL projection.
func zerofillDecimalColDef(mask uint32) *plan2.ColDef {
	return &plan2.ColDef{
		Name: "amount",
		Typ: plan2.Type{
			Id:                int32(types.T_decimal64),
			Width:             10,
			Scale:             2,
			NumericAttributes: mask,
		},
	}
}

// TestDecimalZerofillResultColumnMetadata proves the result-metadata contract of
// D4: an UNSIGNED DECIMAL projection sets UNSIGNED_FLAG and the unsigned
// display length (no sign digit), ZEROFILL additionally sets ZEROFILL_FLAG and
// the render pad digits, and a plain or derived (mask 0) DECIMAL keeps the
// established signed metadata without any attribute leak.
func TestDecimalZerofillResultColumnMetadata(t *testing.T) {
	ctx := context.Background()
	proto := &MysqlProtocolImpl{io: NewIOPackage(true)}

	t.Run("unsigned decimal", func(t *testing.T) {
		col, err := colDef2MysqlColumn(ctx, zerofillDecimalColDef(types.NumericAttributeUnsigned))
		require.NoError(t, err)
		require.Equal(t, defines.MYSQL_TYPE_DECIMAL, col.ColumnType())
		require.NotZero(t, col.Flag()&uint16(defines.UNSIGNED_FLAG))
		require.Zero(t, col.Flag()&uint16(defines.ZEROFILL_FLAG))
		require.False(t, col.IsSigned())
		require.Equal(t, uint32(11), col.Length())
		require.Equal(t, uint8(2), col.Decimal())
		require.Zero(t, col.zerofillPadDigitsForRender())

		definition := parsePrepareColumnDefinition(t, proto.makeColumnDefinition41Payload(col, int(COM_QUERY))[HeaderOffset:])
		require.NotZero(t, definition.flags&uint16(defines.UNSIGNED_FLAG))
		require.Zero(t, definition.flags&uint16(defines.ZEROFILL_FLAG))
		require.Equal(t, uint32(11), definition.length)
		require.Equal(t, uint8(2), definition.decimals)
	})

	t.Run("zerofill decimal implies unsigned", func(t *testing.T) {
		col, err := colDef2MysqlColumn(ctx, zerofillDecimalColDef(types.NumericAttributeZerofillMask))
		require.NoError(t, err)
		require.NotZero(t, col.Flag()&uint16(defines.UNSIGNED_FLAG))
		require.NotZero(t, col.Flag()&uint16(defines.ZEROFILL_FLAG))
		require.Equal(t, uint32(11), col.Length())
		require.Equal(t, 8, col.zerofillPadDigitsForRender())

		definition := parsePrepareColumnDefinition(t, proto.makeColumnDefinition41Payload(col, int(COM_QUERY))[HeaderOffset:])
		require.NotZero(t, definition.flags&uint16(defines.UNSIGNED_FLAG))
		require.NotZero(t, definition.flags&uint16(defines.ZEROFILL_FLAG))
		require.Equal(t, uint32(11), definition.length)

		// Repeated serialization must neither drift nor consume the pad digits.
		again := proto.makeColumnDefinition41Payload(col, int(COM_QUERY))
		definition2 := parsePrepareColumnDefinition(t, again[HeaderOffset:])
		require.Equal(t, definition.flags, definition2.flags)
		require.Equal(t, 8, col.zerofillPadDigitsForRender())
	})

	t.Run("plain and derived decimal keep signed metadata", func(t *testing.T) {
		col, err := colDef2MysqlColumn(ctx, zerofillDecimalColDef(0))
		require.NoError(t, err)
		require.Zero(t, col.Flag()&uint16(defines.UNSIGNED_FLAG))
		require.Zero(t, col.Flag()&uint16(defines.ZEROFILL_FLAG))
		require.True(t, col.IsSigned())
		require.Equal(t, uint32(12), col.Length())
		require.Zero(t, col.zerofillPadDigitsForRender())
	})
}

// TestPadZerofillDecimalText pins the shared pad helper against MySQL's
// ZEROFILL rendering contract (field.cc val_str fixed precision): the integer
// part is padded to the declared pre-point digits; the scale part is never
// re-rendered, and negative/empty/wide text passes through unchanged.
func TestPadZerofillDecimalText(t *testing.T) {
	cases := []struct {
		text   string
		digits int
		want   string
	}{
		{"123.45", 8, "00000123.45"},      // issue contract: DECIMAL(10,2) ZEROFILL
		{"0.50", 8, "00000000.50"},        // zero integer part still pads
		{"99999999.99", 8, "99999999.99"}, // already at full width
		{"123.45", 2, "123.45"},           // narrower target: unchanged
		{"42", 3, "042"},                  // scale-0 declaration
		{"0", 3, "000"},                   // zero rendering, scale 0
		{"123.45", 0, "123.45"},           // not gated: unchanged
		{"-1.50", 8, "-1.50"},             // defensive: the unsigned domain forbids this
		{"", 8, ""},                       // nulls are handled before the helper
	}
	for _, tt := range cases {
		require.Equal(t, tt.want, padZerofillDecimalText(tt.text, tt.digits), "%q digits %d", tt.text, tt.digits)
	}
}

// zfLenenc builds a MySQL length-encoded string payload (single-byte length form).
func zfLenenc(t *testing.T, s string) []byte {
	t.Helper()
	require.Less(t, len(s), 251)
	out := append([]byte{byte(len(s))}, []byte(s)...)
	return out
}

// TestDecimalZerofillTextAndBinaryRowWire drives all four protocol row
// serializers over one ZEROFILL DECIMAL(10,2) column plus a plain signed
// control, asserting the exact packet payload bytes for the padded value, a
// zero-integer value, a NULL and the plain control on both the text
// (appendResultSetTextRow) and binary (appendResultSetBinaryRow) channels,
// and on their ColumnSlices variants (appendResultSetTextRow2 /
// appendResultSetBinaryRow2).
func TestDecimalZerofillTextAndBinaryRowWire(t *testing.T) {
	ctx := context.TODO()
	conn := &prepareResponseCaptureConn{}
	proto, proc, stmt := newBinaryPrepareProtocolTestCaseWithConn(t, "select 1", conn)
	cleanupGeometryWireFixture(t, proto, stmt)

	zfCol, err := colDef2MysqlColumn(ctx, zerofillDecimalColDef(types.NumericAttributeZerofillMask))
	require.NoError(t, err)
	plainColDef := zerofillDecimalColDef(0)
	plainColDef.Name = "plain"
	plainCol, err := colDef2MysqlColumn(ctx, plainColDef)
	require.NoError(t, err)

	mrs := &MysqlResultSet{}
	mrs.AddColumn(zfCol)
	mrs.AddColumn(plainCol)
	mrs.AddRow([]any{"123.45", "123.45"})
	mrs.AddRow([]any{"0.50", "99999999.99"})
	mrs.AddRow([]any{nil, "123.45"})

	pad := "00000123.45"
	padZero := "00000000.50"
	plain := "123.45"
	plainWide := "99999999.99"

	t.Run("text rows", func(t *testing.T) {
		expected := [][]byte{
			append(zfLenenc(t, pad), zfLenenc(t, plain)...),
			append(zfLenenc(t, padZero), zfLenenc(t, plainWide)...),
			append([]byte{0xFB}, zfLenenc(t, plain)...),
		}
		for rowIdx, payload := range expected {
			before := len(conn.writes)
			require.NoError(t, proto.appendResultSetTextRow(mrs, uint64(rowIdx)))
			require.NoError(t, proto.flush())
			packets := splitProtocolPackets(t, conn.writes[before:])
			require.Len(t, packets, 1)
			require.Equal(t, payload, packets[0], "text row %d", rowIdx)
		}
	})

	t.Run("binary rows carry identical padded string payload", func(t *testing.T) {
		expected := [][]byte{
			append(append([]byte{0, 0}, zfLenenc(t, pad)...), zfLenenc(t, plain)...),
			append(append([]byte{0, 0}, zfLenenc(t, padZero)...), zfLenenc(t, plainWide)...),
			append(append([]byte{0, 0x04}, zfLenenc(t, plain)...), []byte{}...),
		}
		for rowIdx, payload := range expected {
			before := len(conn.writes)
			require.NoError(t, proto.appendResultSetBinaryRow(mrs, uint64(rowIdx)))
			require.NoError(t, proto.flush())
			packets := splitProtocolPackets(t, conn.writes[before:])
			require.Len(t, packets, 1)
			require.Equal(t, payload, packets[0], "binary row %d", rowIdx)
		}
	})

	// ColumnSlices variants read directly from the execution batch: the
	// vector carries DECIMAL64 values, the format scale comes from the vector
	// type, and the pad must match the mrs.GetString channel byte for byte.
	newSlices := func(t *testing.T) (*batch.Batch, *ColumnSlices) {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 10, 2))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_decimal64, 10, 2))
		decA, err := types.ParseDecimal64("123.45", 10, 2)
		require.NoError(t, err)
		decB, err := types.ParseDecimal64("0.50", 10, 2)
		require.NoError(t, err)
		decWide, err := types.ParseDecimal64("99999999.99", 10, 2)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], decA, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], decB, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], decA, true, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], decA, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], decWide, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], decA, false, proc.Mp()))
		bat.SetRowCount(3)
		slices := &ColumnSlices{ctx: ctx, colIdx2SliceIdx: make([]int, 2), dataSet: bat}
		return bat, slices
	}

	packetsForRows := func(t *testing.T, binary2 bool, text2 bool) [][]byte {
		bat, slices := newSlices(t)
		defer bat.Clean(proc.Mp())
		defer slices.Close()
		require.NoError(t, convertBatchToSlices(ctx, proto.GetSession(), bat, slices))
		out := make([][]byte, 3)
		for rowIdx := 0; rowIdx < 3; rowIdx++ {
			before := len(conn.writes)
			switch {
			case binary2:
				require.NoError(t, proto.appendResultSetBinaryRow2(mrs, slices, uint64(rowIdx)))
			default:
				require.NoError(t, proto.appendResultSetTextRow2(mrs, slices, uint64(rowIdx)))
			}
			require.NoError(t, proto.flush())
			packets := splitProtocolPackets(t, conn.writes[before:])
			require.Len(t, packets, 1)
			out[rowIdx] = packets[0]
		}
		return out
	}

	t.Run("text rows from ColumnSlices", func(t *testing.T) {
		expected := [][]byte{
			append(zfLenenc(t, pad), zfLenenc(t, plain)...),
			append(zfLenenc(t, padZero), zfLenenc(t, plainWide)...),
			append([]byte{0xFB}, zfLenenc(t, plain)...),
		}
		require.Equal(t, expected, packetsForRows(t, false, true))
	})

	t.Run("binary rows from ColumnSlices", func(t *testing.T) {
		expected := [][]byte{
			append(append([]byte{0, 0}, zfLenenc(t, pad)...), zfLenenc(t, plain)...),
			append(append([]byte{0, 0}, zfLenenc(t, padZero)...), zfLenenc(t, plainWide)...),
			append(append([]byte{0, 0x04}, zfLenenc(t, plain)...), []byte{}...),
		}
		require.Equal(t, expected, packetsForRows(t, true, false))
	})
}

// TestDecimalZerofillExport asserts the export-file byte contract of D4 for
// the two text export channels: CSV/OUTFILE constructByte and JSONLINE
// constructJSONLine both gate on the same result-column ZEROFILL flag as the
// protocol serializers, pad attributed cells, and leave plain controls and
// NULL markers unchanged. NULLs stay the formal \N / json null.
func TestDecimalZerofillExport(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	zfCol, err := colDef2MysqlColumn(ctx, zerofillDecimalColDef(types.NumericAttributeZerofillMask))
	require.NoError(t, err)
	plainDef := zerofillDecimalColDef(0)
	plainDef.Name = "plain"
	plainCol, err := colDef2MysqlColumn(ctx, plainDef)
	require.NoError(t, err)

	newMRS := func() *MysqlResultSet {
		mrs := &MysqlResultSet{}
		mrs.AddColumn(zfCol)
		mrs.AddColumn(plainCol)
		return mrs
	}
	newExportBatch := func(t *testing.T) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 10, 2))
		bat.Vecs[1] = vector.NewVec(types.New(types.T_decimal64, 10, 2))
		v1, err := types.ParseDecimal64("123.45", 10, 2)
		require.NoError(t, err)
		v2, err := types.ParseDecimal64("0.50", 10, 2)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], v1, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], v2, true, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], v2, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], v1, false, mp))
		bat.SetRowCount(2)
		return bat
	}
	newEP := func() *ExportConfig {
		return &ExportConfig{
			userConfig: &tree.ExportParam{
				Fields: tree.NewFields("", false, 0, '\\'),
				Lines:  tree.NewLines("", ""),
			},
			Symbol:     [][]byte{[]byte(","), []byte("\n")},
			ColumnFlag: []bool{false, false},
			mrs:        newMRS(),
		}
	}

	t.Run("csv", func(t *testing.T) {
		ep := newEP()
		bat := newExportBatch(t)
		ch := make(chan *BatchByte, 1)
		constructByte(ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.Equal(t, "00000123.45,0.50\n"+`\N,123.45`+"\n", string(result.writeByte))
	})

	t.Run("jsonline", func(t *testing.T) {
		ep := newEP()
		bat := newExportBatch(t)
		ch := make(chan *BatchByte, 1)
		constructJSONLine(ctx, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
		result := <-ch
		require.NoError(t, result.err)
		require.Equal(t,
			`{"amount":"00000123.45","plain":"0.50"}`+"\n"+
				`{"amount":null,"plain":"123.45"}`+"\n",
			string(result.writeByte))
	})
}

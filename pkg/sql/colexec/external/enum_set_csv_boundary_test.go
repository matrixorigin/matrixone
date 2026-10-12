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

package external

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCSVEnumSetBoundaryMatchesConversion(t *testing.T) {
	for _, tc := range []struct {
		name, domain, raw, text  string
		oid                      types.T
		want                     uint64
		null, fieldNull, invalid bool
		nullValues               []string
	}{
		{name: "padded enum", oid: types.T_enum, domain: " ,red,", raw: " red ", text: "red", want: 2},
		{name: "declared space", oid: types.T_enum, domain: " ,red,", raw: " ", text: " ", want: 1},
		{name: "declared empty", oid: types.T_enum, domain: " ,red,", want: 3},
		{name: "single empty enum", oid: types.T_enum, want: 1},
		{name: "padded single empty enum", oid: types.T_enum, raw: "\u3000", want: 1},
		{name: "nullable enum empty", oid: types.T_enum, domain: "red,blue", null: true},
		{name: "nullable enum whitespace", oid: types.T_enum, domain: "red,blue", raw: "\u3000 ", null: true},
		{name: "enum case folding", oid: types.T_enum, domain: " ,red,", raw: " RED ", text: "RED", want: 2},
		{name: "enum unicode member", oid: types.T_enum, domain: "\u00a0red,red,", raw: "\u00a0red", text: "\u00a0red", want: 1},
		{name: "enum unicode fallback", oid: types.T_enum, domain: "\u00a0red,red,", raw: "\u3000red\u3000", text: "red", want: 2},
		{name: "padded set bitmap", oid: types.T_uint64, domain: " a,a,中文", raw: " 4 ", text: "4", want: 4},
		{name: "set leading space member", oid: types.T_uint64, domain: " a,a,中文", raw: " a", text: " a", want: 1},
		{name: "set plain member", oid: types.T_uint64, domain: " a,a,中文", raw: "a", text: "a", want: 2},
		{name: "set unicode member", oid: types.T_uint64, domain: "\u00a0a,a", raw: "\u00a0a", text: "\u00a0a", want: 1},
		{name: "set empty", oid: types.T_uint64, domain: "0,a"},
		{name: "set empty fallback", oid: types.T_uint64, domain: "0,a", raw: " "},
		{name: "set numeric member", oid: types.T_uint64, domain: "0,a", raw: "0", text: "0", want: 1},
		{name: "single empty set", oid: types.T_uint64, domain: ","},
		{name: "single empty set bitmap", oid: types.T_uint64, domain: ",", raw: "1", text: "1", want: 1},
		{name: "parser null", oid: types.T_enum, domain: "red,blue", raw: "invalid", fieldNull: true, null: true},
		{name: "normalized mapped null", oid: types.T_uint64, domain: " a,a,中文", raw: " 4 ", nullValues: []string{"4"}, null: true},
		{name: "raw marker is not normalized marker", oid: types.T_enum, domain: " ,red,", raw: " red ", text: "red", nullValues: []string{" red "}, want: 2},
		{name: "mapped declared empty", oid: types.T_enum, nullValues: []string{""}, null: true},
		{name: "invalid enum", oid: types.T_enum, domain: "red,blue", raw: "missing", invalid: true},
		{name: "enum ordinal overflow", oid: types.T_enum, domain: "red,blue", raw: " 3 ", invalid: true},
		{name: "invalid set", oid: types.T_uint64, domain: " a,a,中文", raw: "missing", invalid: true},
		{name: "set bitmap overflow", oid: types.T_uint64, domain: " a,a,中文", raw: " 8 ", invalid: true},
		{name: "ordinary uint64", oid: types.T_uint64, raw: " 4 ", text: "4", want: 4},
		{name: "bit space is data", oid: types.T_bit, raw: " ", text: " ", want: 32},
	} {
		for _, mode := range []string{"strict", "parallel", "local-nonstrict"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				defer proc.Free()
				typ := plan.Type{Id: int32(tc.oid), Enumvalues: tc.domain, Width: 8}
				param := enumSetLoadParam(typ, mode)
				param.Extern.NullMap = map[string][]string{"d": tc.nullValues}
				field := csvparser.Field{Val: tc.raw, IsNull: tc.fieldNull, HasStringQuote: true}
				line := []csvparser.Field{field}
				require.Equal(t, !tc.invalid, isLegalLine(param.Extern, param.Cols, line))
				require.Equal(t, field, line[0], "boundary validation must not change the parser's field")
				if tc.invalid && mode == "parallel" {
					// Parallel row conversion defers type validation to the cast;
					// the boundary validator must still reject these fields.
					return
				}
				vecTyp := tc.oid.ToType()
				if mode == "parallel" {
					vecTyp = types.T_varchar.ToType()
				}
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(vecTyp)
				defer bat.Clean(proc.Mp())
				err := getColData(bat, line, 0, param, proc.Mp(), param.Attrs[0], proc)
				if tc.invalid {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, field, line[0])
				require.Equal(t, tc.null, bat.Vecs[0].GetNulls().Contains(0))
				if !tc.null {
					switch {
					case mode == "parallel":
						require.Equal(t, tc.text, bat.Vecs[0].GetStringAt(0))
					case tc.oid == types.T_enum:
						require.Equal(t, types.Enum(tc.want), vector.MustFixedColWithTypeCheck[types.Enum](bat.Vecs[0])[0])
					default:
						require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[0])[0])
					}
				}
			})
		}
	}
}

func TestReadFileOffsetStrictEnumSet(t *testing.T) {
	for _, tc := range []struct {
		name, values string
		types        []plan.Type
	}{
		{"padded enum", " red ", []plan.Type{{Id: int32(types.T_enum), Enumvalues: " ,red,"}}},
		{"padded set bitmap", " 4 ", []plan.Type{{Id: int32(types.T_uint64), Enumvalues: " a,a,中文"}}},
		{"nullable enum empty", "", []plan.Type{{Id: int32(types.T_enum), Enumvalues: "red,blue"}}},
		{"mixed", " red , 4 ,", []plan.Type{{Id: int32(types.T_enum), Enumvalues: " ,red,"}, {Id: int32(types.T_uint64), Enumvalues: " a,a,中文"}, {Id: int32(types.T_enum), Enumvalues: "red,blue"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs := testutil.NewFS(t)
			t.Cleanup(func() { fs.Close(ctx) })
			cols := make([]*plan.ColDef, 0, 1+len(tc.types))
			cols = append(cols, &plan.ColDef{Name: "id", Typ: plan.Type{Id: int32(types.T_uint64)}})
			for i, typ := range tc.types {
				cols = append(cols, &plan.ColDef{Name: fmt.Sprintf("v%d", i), Typ: typ})
			}
			// The caller's size threshold is not part of ReadFileOffset. A
			// small real file exercises its exact boundary and parser path.
			const rows = 121
			const header = "skip this header\n"
			var content strings.Builder
			content.WriteString(header)
			for i := 0; i < rows; i++ {
				fmt.Fprintf(&content, "%03d,%s\n", i, tc.values)
			}
			data := []byte(content.String())
			filePath := "etl:/enum-set-boundaries.csv"
			require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: filePath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
			param := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Filepath: filePath, FileStartOff: int64(len(header)), Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{}},
				ExParam:      tree.ExParam{FileService: fs, Ctx: ctx, Strict: true},
			}
			for _, readers := range []int{2, 4} {
				t.Run(fmt.Sprintf("readers=%d", readers), func(t *testing.T) {
					offsets, err := ReadFileOffset(param, readers, int64(len(data)), cols)
					require.NoError(t, err)
					require.Len(t, offsets, 2*readers, "valid rows must not collapse to one EOF range")
					require.Equal(t, param.FileStartOff, offsets[0])
					require.Equal(t, int64(-1), offsets[len(offsets)-1])
					rowSize := int64(len("000," + tc.values + "\n"))
					batchSize := (int64(len(data)) - param.FileStartOff) / int64(readers)
					var gotIDs []int
					for i := 0; i < len(offsets); i += 2 {
						start, end := offsets[i], offsets[i+1]
						if end == -1 {
							end = int64(len(data))
						}
						require.Less(t, start, end)
						if i > 0 {
							require.Equal(t, offsets[i-1], start)
							require.Equal(t, byte('\n'), data[start-1])
							requested := offsets[i-2] + batchSize
							require.Greater(t, start, requested)
							require.LessOrEqual(t, start-requested, 2*rowSize, "boundary search must stop near the split")
						}
						parser, err := newCSVParserFromReader(param, bytes.NewReader(data[start:end]))
						require.NoError(t, err)
						var fields []csvparser.Field
						for {
							fields, err = parser.Read(fields)
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							require.Len(t, fields, len(cols))
							id, err := strconv.Atoi(fields[0].Val)
							require.NoError(t, err)
							gotIDs = append(gotIDs, id)
						}
					}
					wantIDs := make([]int, rows)
					for i := range wantIDs {
						wantIDs[i] = i
					}
					require.Equal(t, wantIDs, gotIDs, "partition parsing must neither duplicate nor omit rows")
				})
			}
		})
	}
}

func TestStrictEnumBoundarySkipsInvalidPrefix(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV, Tail: &tree.TailParameter{}}}
	cols := []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_uint64)}}, {Name: "v", Typ: plan.Type{Id: int32(types.T_enum), Enumvalues: "red,blue"}}}
	for _, input := range []string{"partial\n001, red \n", "001,missing\n002, red \n", "001,\\N\n"} {
		tail, err := getTailSizeStrict(param, cols, io.NopCloser(strings.NewReader(input)))
		require.NoError(t, err)
		require.Equal(t, int64(len(input)), tail)
	}
	tail, err := getTailSizeStrict(param, cols, io.NopCloser(strings.NewReader("001,missing\n002,missing\n")))
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, tail)
}

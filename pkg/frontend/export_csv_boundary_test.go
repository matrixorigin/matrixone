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
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestOutfileBITCSVAllBytes(t *testing.T) {
	// Translated escape letters are valid enclosure characters, too. Both
	// integer IDs (notably 101 with enclosure '0') and BIT bytes must survive.
	for _, enclosed := range []byte{0, '"', '0', 'b', 'n', 'r', 't', 'Z', 'N'} {
		for _, force := range []bool{false, true} {
			for _, resultSet := range []bool{false, true} {
				t.Run(fmt.Sprintf("enclosed=%d/force=%t/resultset=%t", enclosed, force, resultSet), func(t *testing.T) {
					ep := &ExportConfig{
						userConfig: &tree.ExportParam{
							Fields: tree.NewFields(",", false, enclosed, '\\'),
							Lines:  tree.NewLines("", "\n"),
						},
						Symbol:     [][]byte{[]byte(","), []byte("\n")},
						ColumnFlag: []bool{false, force},
					}
					var data []byte
					if resultSet {
						ep.mrs = &MysqlResultSet{}
						id, bit := &MysqlColumn{}, &MysqlColumn{}
						id.SetColumnType(defines.MYSQL_TYPE_LONG)
						bit.SetColumnType(defines.MYSQL_TYPE_BIT)
						bit.SetLength(8)
						ep.mrs.AddColumn(id)
						ep.mrs.AddColumn(bit)
						stubs := gostub.Stub(&writeDataToCSVFile, func(_ *ExportConfig, b []byte) error {
							data = append(data, b...)
							return nil
						})
						defer stubs.Reset()
						for n := 0; n <= 256; n++ {
							var value any = uint64(n)
							if n == 256 {
								value = nil
							}
							ep.mrs.Data = [][]any{{int32(n), value}}
							require.NoError(t, exportDataFromResultSetToCSVFile(ep))
						}
					} else {
						mp := mpool.MustNewZero()
						bat := batch.NewWithSize(2)
						bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
						bat.Vecs[1] = vector.NewVec(types.New(types.T_bit, 8, 0))
						defer func() { bat.Clean(mp); require.Zero(t, mp.CurrNB()) }()
						for n := 0; n <= 256; n++ {
							require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(n), false, mp))
							require.NoError(t, vector.AppendFixed(bat.Vecs[1], uint64(n), n == 256, mp))
						}
						bat.SetRowCount(257)
						ch := make(chan *BatchByte, 1)
						constructByte(context.Background(), &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, 0, ch, ep)
						result := <-ch
						require.NoError(t, result.err)
						data = result.writeByte
					}
					quote := ""
					if enclosed != 0 {
						quote = string(enclosed)
					}
					parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
						FieldsTerminatedBy: ",", FieldsEnclosedBy: quote,
						FieldsEscapedBy: "\\", LinesTerminatedBy: "\n",
						Null: []string{`\N`}, UnescapedQuote: true,
					}, bytes.NewReader(data), 17, false)
					require.NoError(t, err)
					for n := 0; n <= 256; n++ {
						row, err := parser.Read(nil)
						require.NoError(t, err, "byte %d", n)
						require.Len(t, row, 2, "byte %d", n)
						require.Equal(t, fmt.Sprint(n), row[0].Val)
						require.Equal(t, n == 256, row[1].IsNull, "byte %d", n)
						if n != 256 {
							require.Equal(t, []byte{byte(n)}, []byte(row[1].Val), "byte %d", n)
						}
					}
					_, err = parser.Read(nil)
					require.ErrorIs(t, err, io.EOF)
				})
			}
		}
	}
}

func TestCSVValueEscapingDoesNotMutateOrAllocatePlainValues(t *testing.T) {
	ep := newOutfileExportConfig(tree.NewFields(",", false, 0, '\\'), nil, "\n")
	plain := []byte("ordinary value")
	require.Zero(t, testing.AllocsPerRun(100, func() {
		_ = addEscapeToString(ep, plain, 0)
	}))
	input := []byte("a,b\n\\N")
	require.Equal(t, []byte("a\\,b\\\n\\\\N"), addEscapeToString(ep, input, 0))
	require.Equal(t, []byte("a,b\n\\N"), input)
}

func TestCSVFramingRejectsAmbiguousBareSeparators(t *testing.T) {
	for _, separator := range []string{"0", "b", "n", "r", "t", "Z"} {
		t.Run(separator, func(t *testing.T) {
			ep := newOutfileExportConfig(tree.NewFields(separator, false, 0, '\\'), nil, "\n")
			require.ErrorContains(t, ep.validateCSVFraming(context.Background()), "require a nonempty ENCLOSED BY")
			ep.userConfig.Fields.EnclosedBy.Value = '"'
			require.NoError(t, ep.validateCSVFraming(context.Background()))
			var buf bytes.Buffer
			require.NoError(t, formatOutputString(ep, []byte(separator), []byte("\n"), '"', false, &buf))
			parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
				FieldsTerminatedBy: separator, FieldsEnclosedBy: `"`,
				FieldsEscapedBy: "\\", LinesTerminatedBy: "\n",
			}, bytes.NewReader(buf.Bytes()), 17, false)
			require.NoError(t, err)
			row, err := parser.Read(nil)
			require.NoError(t, err)
			require.Len(t, row, 1)
			require.Equal(t, separator, row[0].Val)
		})
	}
}

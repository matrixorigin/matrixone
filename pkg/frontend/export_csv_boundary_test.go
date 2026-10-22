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
	type csvFormat struct {
		field, line string
		enclosed    byte
		blockSize   int64
		wide        bool
	}
	formats := make([]csvFormat, 0, 22)
	for _, enclosed := range []byte{0, '"', '0', 'b', 'n', 'r', 't', 'Z', 'N', '\\'} {
		formats = append(formats, csvFormat{",", "\n", enclosed, 17, false})
	}
	// Escaping the bare BIT byte N for either separator must not create
	// LOAD's NULL marker. Include multi-byte terminators and both exporters.
	// Backslash separators must also protect generated CR escapes and NULL.
	for _, separator := range []string{"N", "N:", `\`, `\r`} {
		// Use LOAD's production read size for the new terminator cases;
		// retain the existing tiny-block enclosure controls above.
		formats = append(formats, csvFormat{separator, "\n", '"', csvparser.ReadBlockSize, false},
			csvFormat{",", separator, '"', csvparser.ReadBlockSize, false})
	}
	// Generated CR escapes must survive enclosure parsing even when their
	// translated letter is also the next field separator.
	for _, separator := range []string{"r", "r:"} {
		formats = append(formats, csvFormat{separator, "\n", '\\', csvparser.ReadBlockSize, false},
			csvFormat{separator, "\n", '\\', csvparser.ReadBlockSize, true})
	}
	for _, format := range formats {
		enclosed := format.enclosed
		width := int32(8)
		values := make([]uint64, 256)
		for n := range values {
			values[n] = uint64(n)
		}
		if format.wide {
			width = 24
			values = append(values, 0x0d6162, 0x610d62, 0x0d3a62, 0x610d3a, 0x5c4e)
		}
		for _, force := range []bool{false, true} {
			for _, resultSet := range []bool{false, true} {
				t.Run(fmt.Sprintf("field=%q/line=%q/enclosed=%d/width=%d/force=%t/resultset=%t", format.field, format.line, enclosed, width, force, resultSet), func(t *testing.T) {
					ep := &ExportConfig{
						userConfig: &tree.ExportParam{
							Fields: tree.NewFields(format.field, false, enclosed, '\\'),
							Lines:  tree.NewLines("", format.line),
						},
						Symbol:     [][]byte{[]byte(format.field), []byte(format.line)},
						ColumnFlag: []bool{false, force},
					}
					var data []byte
					if resultSet {
						ep.mrs = &MysqlResultSet{}
						id, bit := &MysqlColumn{}, &MysqlColumn{}
						id.SetColumnType(defines.MYSQL_TYPE_LONG)
						bit.SetColumnType(defines.MYSQL_TYPE_BIT)
						bit.SetLength(uint32(width))
						ep.mrs.AddColumn(id)
						ep.mrs.AddColumn(bit)
						stubs := gostub.Stub(&writeDataToCSVFile, func(_ *ExportConfig, b []byte) error {
							data = append(data, b...)
							return nil
						})
						defer stubs.Reset()
						for n := 0; n <= len(values); n++ {
							var value any
							if n < len(values) {
								value = values[n]
							}
							ep.mrs.Data = [][]any{{int32(n), value}}
							require.NoError(t, exportDataFromResultSetToCSVFile(ep))
						}
					} else {
						mp := mpool.MustNewZero()
						bat := batch.NewWithSize(2)
						bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
						bat.Vecs[1] = vector.NewVec(types.New(types.T_bit, width, 0))
						defer func() { bat.Clean(mp); require.Zero(t, mp.CurrNB()) }()
						for n := 0; n <= len(values); n++ {
							var value uint64
							if n < len(values) {
								value = values[n]
							}
							require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(n), false, mp))
							require.NoError(t, vector.AppendFixed(bat.Vecs[1], value, n == len(values), mp))
						}
						bat.SetRowCount(len(values) + 1)
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
						FieldsTerminatedBy: format.field, FieldsEnclosedBy: quote,
						FieldsEscapedBy: "\\", LinesTerminatedBy: format.line,
						Null: []string{`\N`}, UnescapedQuote: true,
					}, bytes.NewReader(data), format.blockSize, false)
					require.NoError(t, err)
					for n := 0; n <= len(values); n++ {
						row, err := parser.Read(nil)
						require.NoError(t, err, "byte %d", n)
						require.Len(t, row, 2, "byte %d", n)
						require.Equal(t, fmt.Sprint(n), row[0].Val)
						require.Equal(t, n == len(values), row[1].IsNull, "byte %d", n)
						if n < len(values) {
							expected := []byte{byte(values[n])}
							if format.wide {
								expected = []byte{byte(values[n] >> 16), byte(values[n] >> 8), byte(values[n])}
							}
							require.Equal(t, expected, []byte(row[1].Val), "value %x", values[n])
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
	for _, enclosed := range []byte{0, '"', '\\'} {
		require.Zero(t, testing.AllocsPerRun(100, func() {
			_ = addEscapeToString(ep, plain, enclosed)
		}))
	}
	input := []byte("a,b\n\\N")
	require.Equal(t, []byte("a\\,b\\\n\\\\N"), addEscapeToString(ep, input, 0))
	require.Equal(t, []byte("a,b\n\\N"), input)
	literalNull := []byte(`\N`)
	require.Equal(t, []byte(`\\\\N`), addEscapeToString(ep, literalNull, '\\'))
	require.Equal(t, []byte(`\N`), literalNull, "escaping must not mutate the source")
	generated := []byte("\r:x")
	require.Equal(t, []byte(`\\r:x`), addEscapeToString(ep, generated, '\\'))
	require.Equal(t, []byte("\r:x"), generated)
}

func TestCSVFramingRejectsAmbiguousBareSeparators(t *testing.T) {
	for _, separator := range []string{"0", "b", "n", "r", "t", "Z", "N", "N:", `\`, `\r`} {
		for _, line := range []bool{false, true} {
			t.Run(fmt.Sprintf("separator=%s/line=%t", separator, line), func(t *testing.T) {
				fieldTerm, lineTerm := separator, "\n"
				if line {
					fieldTerm, lineTerm = ",", separator
				}
				ep := newOutfileExportConfig(tree.NewFields(fieldTerm, false, 0, '\\'), nil, lineTerm)
				require.ErrorContains(t, ep.validateCSVFraming(context.Background()), "require a nonempty ENCLOSED BY")
				ep.userConfig.Fields.EnclosedBy.Value = '"'
				require.NoError(t, ep.validateCSVFraming(context.Background()))
				var buf bytes.Buffer
				require.NoError(t, formatOutputString(ep, []byte(separator), []byte(lineTerm), '"', false, &buf))
				parser, err := csvparser.NewCSVParser(&csvparser.CSVConfig{
					FieldsTerminatedBy: fieldTerm, FieldsEnclosedBy: `"`,
					FieldsEscapedBy: "\\", LinesTerminatedBy: lineTerm,
					Null: []string{`\N`},
				}, bytes.NewReader(buf.Bytes()), 17, false)
				require.NoError(t, err)
				row, err := parser.Read(nil)
				require.NoError(t, err)
				require.Len(t, row, 1)
				require.False(t, row[0].IsNull)
				require.Equal(t, separator, row[0].Val)
			})
		}
	}
	// Keep the physical-prefix guard as well as normalized newline checks.
	ep := newOutfileExportConfig(tree.NewFields(",", false, 0, '\r'), nil, "\r\n")
	require.ErrorContains(t, ep.validateCSVFraming(t.Context()), "require a nonempty ENCLOSED BY")
}

func TestCSVFramingRejectsEnclosureSeparatorPrefix(t *testing.T) {
	for _, enclosed := range []byte{'0', 'b', 'n', 'r', 't', 'Z', 'N', '"', '|', '\\'} {
		for _, line := range []bool{false, true} {
			for _, escape := range []byte{'\\', 0} {
				t.Run(fmt.Sprintf("enclosed=%d/line=%t/escape=%d", enclosed, line, escape), func(t *testing.T) {
					fieldTerm, lineTerm := string(enclosed)+":", "\n"
					if line {
						fieldTerm, lineTerm = ",", string(enclosed)+":"
					}
					ep := newOutfileExportConfig(tree.NewFields(fieldTerm, false, enclosed, escape), nil, lineTerm)
					ep.userConfig.FilePath = t.TempDir() + "/rejected.csv"
					const message = "ENCLOSED BY must not be the first byte"
					require.ErrorContains(t, ep.validateCSVFraming(t.Context()), message)
					require.ErrorContains(t, openNewFile(t.Context(), ep, nil), message)
					require.NoFileExists(t, ep.userConfig.FilePath)
					require.Nil(t, ep.AsyncReader)
					require.Nil(t, ep.AsyncWriter)
					require.Nil(t, ep.AsyncGroup)
					// These options do not frame CSV fields in other formats.
					for _, format := range []string{"jsonline", "parquet"} {
						ep.userConfig.ExportFormat = format
						require.NoError(t, ep.validateCSVFraming(t.Context()))
					}
				})
			}
		}
	}
	// Both the physical CRLF prefix and LOAD's normalized LF are boundaries.
	for _, quote := range []byte{'\r', '\n'} {
		for _, escape := range []byte{'\\', 0} {
			ep := newOutfileExportConfig(tree.NewFields(`\`, false, quote, escape), nil, "\r\n")
			ep.userConfig.FilePath = t.TempDir() + "/rejected.csv"
			require.ErrorContains(t, ep.validateCSVFraming(t.Context()), "ENCLOSED BY must not be the first byte")
			require.ErrorContains(t, openNewFile(t.Context(), ep, nil), "ENCLOSED BY must not be the first byte")
			require.NoFileExists(t, ep.userConfig.FilePath)
			require.Nil(t, ep.AsyncReader)
			require.Nil(t, ep.AsyncWriter)
			require.Nil(t, ep.AsyncGroup)
		}
	}
}

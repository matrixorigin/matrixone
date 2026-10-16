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
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Reproduction for https://github.com/matrixorigin/matrixone/issues/30378:
// a parallel non-strict CSV LOAD must not split inside a quoted field that
// spans multiple physical lines.
func TestParallelNonStrictKeepsQuotedMultilineSplitParseable(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })

	data := []byte("1,\"aaaaaaaaa\nb\"\n2,x\n")
	filePath := "etl:/nonstrict-multiline-boundary.csv"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: filePath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{Filepath: filePath, Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{}},
		ExParam:      tree.ExParam{FileService: fs, Ctx: ctx, Strict: false},
	}

	offsets, err := ReadFileOffset(param, 2, int64(len(data)), nil)
	require.NoError(t, err)
	require.Len(t, offsets, 4)

	var got []string
	for i := 0; i < len(offsets); i += 2 {
		got = append(got, parseRange(t, param, data, offsets[i], offsets[i+1])...)
	}
	require.Equal(t, []string{"1,aaaaaaaaa\nb", "2,x"}, got)
}

// Neighboring cases for the same non-strict boundary mechanism: every
// emitted range must parse with the production parser and the concatenated
// rows must equal a single-range parse (no duplication or omission).
func TestParallelNonStrictBoundaryCases(t *testing.T) {
	newEnclosed := func(v byte) *tree.EnclosedBy {
		return &tree.EnclosedBy{Value: v}
	}
	for _, tc := range []struct {
		name     string
		data     string
		readers  int
		enclosed *tree.EnclosedBy
	}{
		{"multiline first of many", "1,\"a\nb\"\n2,x\n3,y\n4,z\n5,w\n6,v\n", 3, nil},
		{"multiline middle", "1,a\n2,\"x\ny\"\n3,b\n4,c\n5,d\n6,e\n", 3, nil},
		{"consecutive multiline", "1,\"a\nb\"\n2,\"c\nd\"\n3,e\n4,f\n", 2, nil},
		{"doubled quote inside multiline", "1,\"a\"\"b\nc\"\n2,x\n3,y\n4,z\n", 2, nil},
		{"escaped quote inside multiline", "1,\"a\\\"b\nc\"\n2,x\n3,y\n4,z\n", 2, nil},
		{"custom enclosed single quote", "1,'a\nb'\n2,x\n3,y\n4,z\n", 2, newEnclosed('\'')},
		{"ragged rows tolerated", "1,a,b,c\n2\n3,x,y\n4\n5,z\n6,w\n", 3, nil},
		{"empty lines", "\n1,a\n\n2,b\n\n3,c\n", 2, nil},
		{"tiny file many readers", "1,a\n", 4, nil},
		{"quoted empty and plain", "1,\"\"\n2,a\n3,\"b\"\n4,c\n", 2, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs := testutil.NewFS(t)
			t.Cleanup(func() { fs.Close(ctx) })
			data := []byte(tc.data)
			filePath := "etl:/nonstrict-boundary-case.csv"
			require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: filePath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
			tail := &tree.TailParameter{}
			if tc.enclosed != nil {
				tail.Fields = &tree.Fields{EnclosedBy: tc.enclosed}
			}
			param := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Filepath: filePath, Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: tail},
				ExParam:      tree.ExParam{FileService: fs, Ctx: ctx, Strict: false},
			}
			offsets, err := ReadFileOffset(param, tc.readers, int64(len(data)), nil)
			require.NoError(t, err)
			require.Equal(t, int64(0), offsets[0])
			require.Equal(t, int64(-1), offsets[len(offsets)-1])
			for i := 0; i+1 < len(offsets); i += 2 {
				if offsets[i+1] == -1 {
					continue
				}
				require.LessOrEqual(t, offsets[i], offsets[i+1], "range must not be inverted")
			}
			full := parseRange(t, param, data, 0, int64(len(data)))
			var got []string
			for i := 0; i < len(offsets); i += 2 {
				got = append(got, parseRange(t, param, data, offsets[i], offsets[i+1])...)
			}
			require.Equal(t, full, got, "partition parsing must neither duplicate nor omit rows")
		})
	}
}

// A genuinely unbalanced quote is not scannable: ReadFileOffset must keep its
// historical behavior (next physical newline, no error, no hang) instead.
func TestParallelNonStrictUnbalancedQuoteFallback(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })
	data := []byte("1,\"abc\n2,x\n3,y\n4,z\n")
	filePath := "etl:/nonstrict-unbalanced.csv"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: filePath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{Filepath: filePath, Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{}},
		ExParam:      tree.ExParam{FileService: fs, Ctx: ctx, Strict: false},
	}
	offsets, err := ReadFileOffset(param, 2, int64(len(data)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 11, 11, -1}, offsets)
}

func parseRange(t *testing.T, param *tree.ExternParam, data []byte, start, end int64) []string {
	t.Helper()
	if end == -1 {
		end = int64(len(data))
	}
	parser, err := newCSVParserFromReader(param, bytes.NewReader(data[start:end]))
	require.NoError(t, err)
	var got []string
	var fields []csvparser.Field
	for {
		fields, err = parser.Read(fields)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "range [%d,%d) must stay parseable", start, end)
		row := ""
		for i, f := range fields {
			if i > 0 {
				row += ","
			}
			// Field.Val may alias the parser's internal buffer through a
			// zero-copy conversion; clone before retaining past the next Read.
			row += strings.Clone(f.Val)
		}
		got = append(got, row)
	}
	return got
}

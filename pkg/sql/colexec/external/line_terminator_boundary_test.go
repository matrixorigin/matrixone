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
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestNonStrictTailCustomLineTerminator(t *testing.T) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Tail:   &tree.TailParameter{Lines: tree.NewLines("", "||")},
	}}
	const input = "partial\nfield,v0||1,next,v1||"
	tail, err := getTailSize(param, nil, io.NopCloser(strings.NewReader(input)))
	require.NoError(t, err)
	require.Equal(t, int64(len("partial\nfield,v0||")), tail)
}

func TestNonStrictTailTerminators(t *testing.T) {
	for _, tc := range []struct {
		name, term, input string
		want              int64
		eof               bool
	}{
		{"default", "", "abc\nnext", 4, false},
		{"single byte", ";", "abc\n;next", 5, false},
		{"CRLF", "\r\n", "abc\n\r\nnext", 6, false},
		{"overlapping prefix", "ababac", "ababababacnext", 10, false},
		{"repeated prefix", "aab", "aaaabnext", 5, false},
		{"buffer boundary", "||", strings.Repeat("x", 4095) + "||next", 4097, false},
		{"long single byte", "\n", strings.Repeat("x", 8192) + "\nnext", 8193, false},
		{"exact end", "||", "abc||", 5, false},
		{"no terminator", "||", "abc\ndef", 7, true},
		{"partial terminator", "||", "abc|", 4, true},
		{"empty", "||", "", 0, true},
		{"ambiguous start", "||", "||c,d||next", 7, false},
		{"overlapping run", "||", "|||||c,d||next", 10, false},
		{"periodic run", "abab", "abababxababnext", 11, false},
		{"insufficient lookbehind", "aaa", "xaaaxaaaz", 8, false},
		{"sufficient lookbehind", "aaa", "xxaaaz", 5, false},
		{"periodic EOF", "aaa", strings.Repeat("a", 8192), 8192, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
				Tail: &tree.TailParameter{Lines: tree.NewLines("", tc.term)},
			}}
			// A one-byte reader forces delimiters across underlying reads.
			for _, r := range []io.Reader{strings.NewReader(tc.input), &oneByteBoundaryReader{r: strings.NewReader(tc.input)}} {
				got, err := getTailSizeNoStrict(param, r)
				if tc.eof {
					require.ErrorIs(t, err, io.EOF)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.want, got)
			}
		})
	}
	for _, tail := range []*tree.TailParameter{nil, {}, {Lines: &tree.Lines{}}} {
		param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Tail: tail}}
		got, err := getTailSizeNoStrict(param, strings.NewReader("a\nb"))
		require.NoError(t, err)
		require.Equal(t, int64(2), got)
	}
}

type oneByteBoundaryReader struct{ r io.Reader }

func (r *oneByteBoundaryReader) Read(p []byte) (int, error) { return r.r.Read(p[:1]) }

func TestNonStrictTailReadFailure(t *testing.T) {
	sentinel := errors.New("boundary read failed")
	for _, term := range []string{"\n", "||"} {
		param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Tail: &tree.TailParameter{Lines: tree.NewLines("", term)}}}
		got, err := getTailSizeNoStrict(param, io.MultiReader(strings.NewReader("partial|"), &boundaryErrorReader{err: sentinel}))
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, int64(8), got)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		param.Ctx = ctx
		got, err = getTailSizeNoStrict(param, strings.NewReader("a"+term))
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, got)
	}
}

type boundaryErrorReader struct{ err error }

func (r *boundaryErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestReadFileOffsetNonStrictLineTerminators(t *testing.T) {
	for _, tc := range []struct {
		name, term, payload string
		rows                int
		final               bool
	}{
		{"custom embedded LF", "||", "left\nright", 31, true},
		{"custom no final terminator", "||", "left\nright", 31, false},
		{"bordered leading field prefix", "||", "left\nright", 31, true},
		{"unbordered custom", "<END>", "left\nright", 31, true},
		{"single byte", ";", "left\nright", 31, true},
		{"CRLF", "\r\n", "plain", 31, true},
		{"default LF", "\n", "plain", 31, true},
		{"one long record", "||", strings.Repeat("x", 128) + "\nlast", 1, true},
		{"one unterminated record", "||", strings.Repeat("x", 128) + "\nlast", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
			fs := testutil.NewFS(t)
			t.Cleanup(func() { fs.Close(ctx) })
			const header = "ignored header"
			var content strings.Builder
			content.WriteString(header)
			var want [][]string
			for i := 0; i < tc.rows; i++ {
				id, last := fmt.Sprintf("%03d", i), fmt.Sprintf("v%d", i)
				if tc.name == "bordered leading field prefix" {
					id = "|" + id
				}
				fmt.Fprintf(&content, "%s,%s,%s", id, tc.payload, last)
				if i+1 < tc.rows || tc.final {
					content.WriteString(tc.term)
				}
				want = append(want, []string{id, tc.payload, last})
			}
			data := []byte(content.String())
			const filePath = "etl:/line-boundaries.csv"
			require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: filePath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
				Filepath: filePath, FileStartOff: int64(len(header)), Format: tree.CSV, ScanType: tree.INFILE,
				CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{Lines: tree.NewLines("", tc.term)},
			}, ExParam: tree.ExParam{FileService: fs, Ctx: ctx}}
			for _, readers := range []int{2, 4} {
				offsets, err := ReadFileOffset(param, readers, int64(len(data)), nil)
				require.NoError(t, err)
				require.Equal(t, param.FileStartOff, offsets[0])
				require.Equal(t, int64(-1), offsets[len(offsets)-1])
				if tc.rows == 1 {
					require.Equal(t, []int64{param.FileStartOff, -1}, offsets)
				} else {
					require.Len(t, offsets, readers*2)
				}
				var got [][]string
				for i := 0; i < len(offsets); i += 2 {
					start, end := offsets[i], offsets[i+1]
					if end == -1 {
						end = int64(len(data))
					}
					require.Less(t, start, end)
					if i > 0 {
						require.Equal(t, offsets[i-1], start)
						require.True(t, bytes.HasSuffix(data[:start], []byte(tc.term)))
					}
					got = append(got, parseBoundaryRows(t, param, data[start:end])...)
				}
				require.Equal(t, want, got, "every field and record must be preserved")
				require.Equal(t, parseBoundaryRows(t, param, data[param.FileStartOff:]), got)
			}
		})
	}
}

// A match may overlap the delimiter that ends the preceding record. Checking
// only that a range ends in the delimiter cannot detect this corruption.
func TestReadFileOffsetNonStrictOverlappingBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, term, data string
		want             [][]string
	}{
		{"review counterexample", "||", "a,bbb|||c,d||", [][]string{{"a", "bbb"}, {"|c", "d"}}},
		{"repeated byte", "aaa", "a,bbbbbb" + "aaa" + "aa,cdd" + "aaa", [][]string{{"a", "bbbbbb"}, {"aa", "cdd"}}},
		{"periodic delimiter", "abab", "a,bbbbbb" + "abab" + "ab,c" + "abab", [][]string{{"a", "bbbbbb"}, {"ab", "c"}}},
		{"unterminated final record", "||", "a,bb|||c,d", [][]string{{"a", "bb"}, {"|c", "d"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
			fs := testutil.NewFS(t)
			t.Cleanup(func() { fs.Close(ctx) })
			const filePath = "etl:/overlapping-boundaries.csv"
			data := []byte(tc.data)
			require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: filePath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
				Filepath: filePath, Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS,
				Tail: &tree.TailParameter{Lines: tree.NewLines("", tc.term)},
			}, ExParam: tree.ExParam{FileService: fs, Ctx: ctx}}
			require.Equal(t, tc.want, parseBoundaryRows(t, param, data))
			for _, readers := range []int{2, 3, 4} {
				offsets, err := ReadFileOffset(param, readers, int64(len(data)), nil)
				require.NoError(t, err)
				var got [][]string
				for i := 0; i < len(offsets); i += 2 {
					end := offsets[i+1]
					if end == -1 {
						end = int64(len(data))
					}
					got = append(got, parseBoundaryRows(t, param, data[offsets[i]:end])...)
				}
				require.Equal(t, tc.want, got, "readers=%d offsets=%v", readers, offsets)
			}
		})
	}
}

func TestNonStrictFieldTerminatorConflict(t *testing.T) {
	for _, tc := range []struct {
		name, field, line string
		conflict          bool
	}{
		{"contained token", "x||y", "||", true},
		{"crossing suffix", "x|", "||", true},
		{"single byte line", "x;y", ";", true},
		{"default line", "x\ny", "", true},
		{"partial periodic suffix", "xababab", "ababac", true},
		{"reverse overlap only", "|x", "||", false},
		{"line contains field", "|x", "a|xb", false},
		{"shared character only", "x|y", "||", false},
		{"shared prefix only", "||x", "||", false},
		{"ordinary custom line", ",", "||", false},
		{"ordinary multi byte field", "<FIELD>", "<LINE>", false},
		{"defaults", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Tail: &tree.TailParameter{
				Fields: tree.NewFields(tc.field, false, 0, 0), Lines: tree.NewLines("", tc.line),
			}}}
			require.Equal(t, tc.conflict, nonStrictFieldTerminatorConflict(param))
			param.Format = tree.JSONLINE
			require.False(t, nonStrictFieldTerminatorConflict(param), "JSONLINE uses a single tab field token")
		})
	}
	for _, tail := range []*tree.TailParameter{nil, {}, {Fields: &tree.Fields{}, Lines: &tree.Lines{}}} {
		param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Tail: tail}}
		require.False(t, nonStrictFieldTerminatorConflict(param))
	}
}

func TestReadFileOffsetConflictingFieldTerminator(t *testing.T) {
	for _, tc := range []struct {
		name, field, line, data string
		want                    [][]string
	}{
		{"contained line token", "x||y", "||", "aax||yb||ccx||yd||", [][]string{{"aa", "b"}, {"cc", "d"}}},
		{"line crosses field suffix", "x|", "||", "aax||b||ccx||d||", [][]string{{"aa", "|b"}, {"cc", "|d"}}},
		{"single byte line token", "x;y", ";", "aax;yb;ccx;yd;", [][]string{{"aa", "b"}, {"cc", "d"}}},
		{"default line token", "x\ny", "", "aax\nyb\nccx\nyd\n", [][]string{{"aa", "b"}, {"cc", "d"}}},
	} {
		for _, header := range []string{"", "ignored header"} {
			t.Run(fmt.Sprintf("%s/header=%d", tc.name, len(header)), func(t *testing.T) {
				ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
				fs := testutil.NewFS(t)
				t.Cleanup(func() { fs.Close(ctx) })
				const filePath = "etl:/field-boundaries.csv"
				data := []byte(header + tc.data)
				require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: filePath, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
				param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
					Filepath: filePath, FileStartOff: int64(len(header)), Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS,
					Tail: &tree.TailParameter{Fields: tree.NewFields(tc.field, false, 0, 0), Lines: tree.NewLines("", tc.line)},
				}, ExParam: tree.ExParam{FileService: fs, Ctx: ctx}}
				require.Equal(t, tc.want, parseBoundaryRows(t, param, data[param.FileStartOff:]))
				for _, readers := range []int{2, 3, 4} {
					offsets, err := ReadFileOffset(param, readers, int64(len(data)), nil)
					require.NoError(t, err)
					var got [][]string
					for i := 0; i < len(offsets); i += 2 {
						end := offsets[i+1]
						if end == -1 {
							end = int64(len(data))
						}
						got = append(got, parseBoundaryRows(t, param, data[offsets[i]:end])...)
					}
					require.Equal(t, tc.want, got, "readers=%d offsets=%v", readers, offsets)
					require.Equal(t, []int64{param.FileStartOff, -1}, offsets, "conflicting tokens must keep the file together")
				}
				ctx, cancel := context.WithCancel(ctx)
				cancel()
				param.Ctx = ctx
				offsets, err := ReadFileOffset(param, 2, int64(len(data)), nil)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, offsets)
			})
		}
	}
}

func parseBoundaryRows(t *testing.T, param *tree.ExternParam, data []byte) [][]string {
	t.Helper()
	parser, err := newCSVParserFromReader(param, bytes.NewReader(data))
	require.NoError(t, err)
	var rows [][]string
	var fields []csvparser.Field
	for {
		fields, err = parser.Read(fields)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		row := make([]string, len(fields))
		for i, field := range fields {
			require.False(t, field.IsNull)
			row[i] = strings.Clone(field.Val)
		}
		rows = append(rows, row)
	}
	return rows
}

func TestReadFileOffsetProbeOwnership(t *testing.T) {
	readErr, closeErr := errors.New("read failed"), errors.New("close failed")
	for _, tc := range []struct {
		name, data, field          string
		readErr, closeErr, wantErr error
		strict, noProbe            bool
	}{
		{name: "success", data: "tail||"},
		{name: "EOF", data: "tail"},
		{name: "read error", data: "tail", readErr: readErr, wantErr: readErr},
		{name: "close error", data: "tail||", closeErr: closeErr, wantErr: closeErr},
		{name: "read error precedes close", data: "tail", readErr: readErr, closeErr: closeErr, wantErr: readErr},
		{name: "conflicting field fallback", field: "x||y", noProbe: true},
		{name: "strict conflicting field control", data: "aax||yb||", field: "x||y", strict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &boundaryProbeFS{t: t, data: tc.data, readErr: tc.readErr, closeErr: tc.closeErr}
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
				Filepath: "etl:/probe.csv", Format: tree.CSV, Tail: &tree.TailParameter{Lines: tree.NewLines("", "||")},
			}, ExParam: tree.ExParam{Ctx: fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin"), FileService: fs}}
			param.Strict = tc.strict
			if tc.field != "" {
				param.Tail.Fields = tree.NewFields(tc.field, false, 0, 0)
			}
			offsets, err := ReadFileOffset(param, 4, 100, nil)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, offsets)
			} else {
				require.NoError(t, err)
			}
			if tc.noProbe {
				require.Zero(t, fs.opened, "fallback must not acquire a probe")
				require.Equal(t, []int64{param.FileStartOff, -1}, offsets)
			} else {
				require.Positive(t, fs.opened)
			}
			require.Equal(t, fs.opened, fs.closed, "close each probe on every terminal path")
		})
	}
}

type boundaryProbeFS struct {
	fileservice.ETLFileService
	t                 *testing.T
	data              string
	readErr, closeErr error
	opened, closed    int
}

func (fs *boundaryProbeFS) Name() string { return "etl" }
func (fs *boundaryProbeFS) Read(_ context.Context, v *fileservice.IOVector) error {
	require.Equal(fs.t, fs.opened, fs.closed, "close the prior probe before opening the next")
	fs.opened++
	var r io.Reader = strings.NewReader(fs.data)
	if fs.readErr != nil {
		r = io.MultiReader(r, &boundaryErrorReader{err: fs.readErr})
	}
	*v.Entries[0].ReadCloserForRead = &boundaryProbeCloser{Reader: r, fs: fs}
	return nil
}

type boundaryProbeCloser struct {
	io.Reader
	fs *boundaryProbeFS
}

func (r *boundaryProbeCloser) Close() error { r.fs.closed++; return r.fs.closeErr }

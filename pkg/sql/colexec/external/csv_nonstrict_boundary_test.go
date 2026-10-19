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
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Reproduction for https://github.com/matrixorigin/matrixone/issues/30378:
// a parallel non-strict CSV LOAD must not split inside a quoted field that
// spans multiple physical lines.
func TestParallelNonStrictKeepsQuotedMultilineSplitParseable(t *testing.T) {
	ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
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
	require.Equal(t, []string{"1,aaaaaaaaa\nb"}, parseRange(t, param, data, offsets[0], offsets[1]))

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
			ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
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
	ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
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

// ReadFileOffset's consumer uses actual bounded FS streams, not byte slices.
func TestParallelNonStrictCsvReaderRanges(t *testing.T) {
	ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
	fs := testutil.NewFS(t)
	t.Cleanup(func() { fs.Close(ctx) })
	data := []byte("1,\"aaaaaaaaa\nb\"\n2,x\n")
	const path = "etl:/csv-reader-boundaries.csv"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: path, Entries: []fileservice.IOEntry{{Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
	extern := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{Filepath: path, Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{}},
		ExParam:      tree.ExParam{FileService: fs, Ctx: ctx, Parallel: true, Strict: false, ExternType: int32(plan.ExternType_LOAD)},
	}
	offsets, err := ReadFileOffset(extern, 2, int64(len(data)), nil)
	require.NoError(t, err)
	proc := testutil.NewProcess(t, testutil.WithFileService(fs))
	proc.Base.SessionInfo.Account = "sys"
	proc.Base.SessionInfo.Role = "moadmin"
	var ids []int64
	var values []string
	for idx := 0; idx < len(offsets)/2; idx++ {
		func() {
			param := &ExternalParam{
				ExParamConst: ExParamConst{
					Idx: idx, Extern: extern, ColumnListLen: 2, maxBatchSize: 1 << 20,
					Attrs:    []plan.ExternAttr{{ColName: "id", ColIndex: 0, ColFieldIndex: 0}, {ColName: "v", ColIndex: 1, ColFieldIndex: 1}},
					Cols:     []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}, {Name: "v", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}}},
					FileSize: []int64{int64(len(data))}, FileOffsetTotal: []*pipeline.FileOffset{{Offset: offsets}},
				},
				ExParam: ExParam{Fileparam: &ExFileparam{FileIndex: 1, FileCnt: 1, Filepath: path}},
			}
			reader, err := NewCsvReader(param, proc)
			require.NoError(t, err)
			defer reader.Close()
			empty, err := reader.Open(param, proc)
			require.NoError(t, err)
			require.False(t, empty)
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(proc.Mp())
			finished, err := reader.ReadBatch(ctx, bat, proc, nil)
			require.NoError(t, err)
			require.True(t, finished)
			ids = append(ids, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...)
			for row := 0; row < bat.RowCount(); row++ {
				values = append(values, strings.Clone(bat.Vecs[1].GetStringAt(row)))
			}
		}()
	}
	require.Equal(t, []int64{1, 2}, ids)
	require.Equal(t, []string{"aaaaaaaaa\nb", "x"}, values)
}

func TestParallelNonStrictConfiguredBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		start      int64
		tail       *tree.TailParameter
		options    []string
	}{
		{name: "ignored prefix and unterminated final line", data: "header\n1,\"a\nb\"\n2,x", start: 7},
		{name: "custom terminator", data: "1,\"a|b\"||2,x||3,y", tail: &tree.TailParameter{Lines: &tree.Lines{TerminatedBy: &tree.Terminated{Value: "||"}}}},
		{name: "starting prefix", data: "skip\np:1,\"a\nb\"\np:2,x\n", tail: &tree.TailParameter{Lines: &tree.Lines{StartingBy: "p:"}}},
		{name: "comments including trailing comment", data: "#skip\n1,\"a\nb\"\n2,x\n#end", options: []string{"comment", "#"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := testutil.NewFS(t)
			t.Cleanup(func() { fs.Close(context.Background()) })
			data := []byte(tc.data)
			const path = "etl:/configured-boundaries.csv"
			require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{FilePath: path, Entries: []fileservice.IOEntry{{Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}}}))
			if tc.tail == nil {
				tc.tail = &tree.TailParameter{}
			}
			param := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Filepath: path, Format: tree.CSV, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: tc.tail, FileStartOff: tc.start, Option: tc.options},
				ExParam:      tree.ExParam{FileService: fs, Ctx: fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")},
			}
			offsets, err := ReadFileOffset(param, 3, int64(len(data)), nil)
			require.NoError(t, err)
			var got []string
			for i := 0; i < len(offsets); i += 2 {
				got = append(got, parseRange(t, param, data, offsets[i], offsets[i+1])...)
			}
			require.Equal(t, parseRange(t, param, data, tc.start, -1), got)
		})
	}
}

// Only Read/Name are used by the offset planner; the embedded interface supplies
// unrelated methods so fault injection stays local to this boundary.
type boundaryTestFS struct {
	fileservice.ETLFileService
	read func(context.Context, *fileservice.IOVector) error
}

func (f boundaryTestFS) Name() string { return "etl" }
func (f boundaryTestFS) Read(ctx context.Context, vec *fileservice.IOVector) error {
	return f.read(ctx, vec)
}

type boundaryTestReader struct {
	io.Reader
	close func()
}

func (r boundaryTestReader) Close() error { r.close(); return nil }

type boundaryReadFunc func([]byte) (int, error)

func (f boundaryReadFunc) Read(p []byte) (int, error) { return f(p) }

func TestNonStrictBoundaryFailures(t *testing.T) {
	storageErr := errors.New("storage read failed")
	for _, fallback := range []bool{false, true} {
		mode := "csv"
		if fallback {
			mode = "legacy"
		}
		for _, want := range []error{storageErr, context.Canceled} {
			t.Run(mode+"/"+want.Error(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				closed, opened, reads := 0, 0, 0
				fs := boundaryTestFS{read: func(_ context.Context, vec *fileservice.IOVector) error {
					opened++
					input := boundaryReadFunc(func(p []byte) (int, error) {
						reads++
						if want == context.Canceled {
							// Cancel within the same CSV record / physical line.
							cancel()
							return copy(p, "\"a"), nil
						}
						return 0, want
					})
					*vec.Entries[0].ReadCloserForRead = boundaryTestReader{Reader: input, close: func() { closed++ }}
					return nil
				}}
				param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Tail: &tree.TailParameter{}}, ExParam: tree.ExParam{Ctx: ctx}}
				var err error
				if fallback {
					_, err = getNonStrictFallbackBoundary(param, fs, "unused", 0)
				} else {
					_, _, err = getNonStrictBoundary(param, fs, "unused", 0, 1)
				}
				require.ErrorIs(t, err, want)
				require.Equal(t, 1, reads, "cancellation must prevent the next underlying read")
				require.Equal(t, 1, opened)
				require.Equal(t, opened, closed)
			})
		}
	}
	t.Run("comment lookahead error", func(t *testing.T) {
		opened, closed, reads := 0, 0, 0
		input := strings.NewReader("1,x\n")
		fs := boundaryTestFS{read: func(_ context.Context, vec *fileservice.IOVector) error {
			opened++
			r := boundaryReadFunc(func(p []byte) (int, error) {
				reads++
				if reads == 1 {
					return 0, storageErr
				}
				return input.Read(p)
			})
			*vec.Entries[0].ReadCloserForRead = boundaryTestReader{Reader: r, close: func() { closed++ }}
			return nil
		}}
		param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Tail: &tree.TailParameter{}, Option: []string{"comment", "#"},
		}}
		_, legacy, err := getNonStrictBoundary(param, fs, "unused", 0, 1)
		require.ErrorIs(t, err, storageErr)
		require.False(t, legacy)
		require.Equal(t, 1, reads, "lookahead must not retry past a storage error")
		require.Equal(t, 1, opened)
		require.Equal(t, opened, closed)
	})
	t.Run("configuration", func(t *testing.T) {
		closed := false
		fs := boundaryTestFS{read: func(_ context.Context, vec *fileservice.IOVector) error {
			*vec.Entries[0].ReadCloserForRead = boundaryTestReader{Reader: strings.NewReader(""), close: func() { closed = true }}
			return nil
		}}
		param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Tail: &tree.TailParameter{Lines: &tree.Lines{StartingBy: "invalid\n"}}}}
		_, legacy, err := getNonStrictBoundary(param, fs, "unused", 0, 1)
		require.Error(t, err)
		require.False(t, legacy)
		require.True(t, closed)
	})
	for _, fallback := range []bool{false, true} {
		mode := "syntax transition"
		if fallback {
			mode = "physical fallback"
		}
		t.Run(mode+" close error", func(t *testing.T) {
			closeErr := errors.New("probe close failed")
			fs := &boundaryProbeFS{t: t, data: "1,\"unclosed", closeErr: closeErr}
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Tail: &tree.TailParameter{}}}
			var err error
			if fallback {
				_, err = getNonStrictFallbackBoundary(param, fs, "unused", 0)
			} else {
				_, _, err = getNonStrictBoundary(param, fs, "unused", 0, 1)
			}
			require.ErrorIs(t, err, closeErr)
			require.Equal(t, 1, fs.opened)
			require.Equal(t, fs.opened, fs.closed)
		})
	}
}

func TestNonStrictFallbackLongLine(t *testing.T) {
	for _, ending := range []string{"", "\nnext"} {
		t.Run(ending, func(t *testing.T) {
			// Cross bufio's fixed 4096-byte scratch buffer once.
			const lineSize = 4097
			closed := false
			fs := boundaryTestFS{read: func(_ context.Context, vec *fileservice.IOVector) error {
				*vec.Entries[0].ReadCloserForRead = boundaryTestReader{Reader: strings.NewReader(strings.Repeat("a", lineSize) + ending), close: func() { closed = true }}
				return nil
			}}
			boundary, err := getNonStrictFallbackBoundary(&tree.ExternParam{}, fs, "unused", 7)
			if ending == "" {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.NoError(t, err)
			}
			want := int64(7 + lineSize)
			if ending != "" {
				want++
			}
			require.Equal(t, want, boundary)
			require.True(t, closed)
		})
	}
}

func TestNonStrictBoundaryLegacyMode(t *testing.T) {
	for _, format := range []string{tree.CSV, tree.JSONLINE} {
		t.Run(format, func(t *testing.T) {
			// One unterminated quoted record: CSV replay must fail once only.
			data := "1,\"abc\n2,x\n3,y\n4,z\n5,w\n6,v\n7,u\n8,t\n"
			var starts []int64
			closed, bytesRead := 0, 0
			fs := boundaryTestFS{read: func(_ context.Context, vec *fileservice.IOVector) error {
				start := vec.Entries[0].Offset
				starts = append(starts, start)
				input := strings.NewReader(data[start:])
				*vec.Entries[0].ReadCloserForRead = boundaryTestReader{Reader: input, close: func() { bytesRead += len(data) - int(start) - input.Len(); closed++ }}
				return nil
			}}
			param := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Filepath: "etl:/unused", Format: format, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{}},
				ExParam:      tree.ExParam{FileService: fs, Ctx: fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")},
			}
			offsets, err := ReadFileOffset(param, 4, int64(len(data)), nil)
			require.NoError(t, err)
			// Compute the historical next-newline ranges independently.
			want := []int64{0}
			var legacyStarts []int64
			for i := 1; i < 4; i++ {
				guess := want[len(want)-1] + int64(len(data)/4)
				if guess >= int64(len(data)) {
					break
				}
				legacyStarts = append(legacyStarts, guess)
				n := strings.IndexByte(data[guess:], '\n')
				boundary := guess + int64(n+1)
				if boundary >= int64(len(data)) {
					break
				}
				want = append(want, boundary)
			}
			ranges := make([]int64, 0, 2*len(want))
			for i, start := range want {
				end := int64(-1)
				if i+1 < len(want) {
					end = want[i+1]
				}
				ranges = append(ranges, start, end)
			}
			require.Equal(t, ranges, offsets)
			expectedStarts := legacyStarts
			if format == tree.CSV {
				expectedStarts = append([]int64{0}, legacyStarts...)
			}
			require.Equal(t, expectedStarts, starts, "CSV replays at most once after syntax failure; JSONLINE never replays")
			require.Equal(t, len(starts), closed)
			// Tiny streams fit in read-ahead buffers; account for that exactly.
			wantBytes := 0
			for _, start := range expectedStarts {
				wantBytes += len(data) - int(start)
			}
			require.Equal(t, wantBytes, bytesRead)
		})
	}
}

func TestNonStrictBoundaryLegacyTerminators(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		offsets    []int64
		probes     []int64
	}{
		{"custom terminator", "1,\"abc\nz||2,x||3,y||4,z||5,w||6,v||", []int64{0, 15, 15, 30, 30, -1}, []int64{11, 26}},
		{"EOF without terminator", "1,\"abc\nz", []int64{0, -1}, []int64{2}},
	} {
		for _, format := range []string{tree.CSV, tree.JSONLINE} {
			t.Run(tc.name+"/"+format, func(t *testing.T) {
				var starts []int64
				closed := 0
				fs := boundaryTestFS{read: func(_ context.Context, vec *fileservice.IOVector) error {
					require.Equal(t, len(starts), closed, "close each probe before opening its successor")
					start := vec.Entries[0].Offset
					starts = append(starts, start)
					*vec.Entries[0].ReadCloserForRead = boundaryTestReader{Reader: strings.NewReader(tc.data[start:]), close: func() { closed++ }}
					return nil
				}}
				param := &tree.ExternParam{
					ExParamConst: tree.ExParamConst{Filepath: "etl:/unused", Format: format, ScanType: tree.INFILE, CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{Lines: tree.NewLines("", "||")}},
					ExParam:      tree.ExParam{FileService: fs, Ctx: fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")},
				}
				offsets, err := ReadFileOffset(param, 3, int64(len(tc.data)), nil)
				require.NoError(t, err)
				require.Equal(t, tc.offsets, offsets, "embedded LF must not end a fallback range; EOF must not add an empty range")
				wantProbes := tc.probes
				if format == tree.CSV {
					wantProbes = append([]int64{0}, wantProbes...)
				}
				require.Equal(t, wantProbes, starts, "malformed CSV replays only once; JSONLINE goes straight to physical scanning")
				require.Equal(t, len(starts), closed)
			})
		}
	}
}

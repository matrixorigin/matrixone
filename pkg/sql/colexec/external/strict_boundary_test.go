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

package external

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/stretchr/testify/require"
)

type strictBoundaryTestReader struct {
	io.Reader
	start             int64
	bytes, reads      int
	closes            int
	maxRead           int
	readErr, closeErr error
	onRead            func()
}

func (s *strictBoundaryTestReader) Read(p []byte) (int, error) {
	s.reads++
	if s.maxRead > 0 && len(p) > s.maxRead {
		p = p[:s.maxRead]
	}
	if s.onRead != nil {
		defer s.onRead()
	}
	if s.readErr != nil {
		return 0, s.readErr
	}
	n, err := s.Reader.Read(p)
	s.bytes += n
	return n, err
}
func (s *strictBoundaryTestReader) Close() error { s.closes++; return s.closeErr }

type strictBoundaryTestFiles struct {
	stages    []*strictBoundaryTestReader
	failOpen  int
	openErr   error
	configure func(int, *strictBoundaryTestReader)
}

func strictBoundaryParam(t *testing.T, data string) (*tree.ExternParam, *strictBoundaryTestFiles) {
	t.Helper()
	files := &strictBoundaryTestFiles{}
	fs := boundaryTestFS{read: func(_ context.Context, vec *fileservice.IOVector) error {
		for _, old := range files.stages {
			require.Equal(t, 1, old.closes, "close before next open")
		}
		if files.failOpen != 0 && len(files.stages)+1 == files.failOpen {
			return files.openErr
		}
		offset := vec.Entries[0].Offset
		s := &strictBoundaryTestReader{Reader: strings.NewReader(data[offset:]), start: offset}
		if files.configure != nil {
			files.configure(len(files.stages), s)
		}
		files.stages = append(files.stages, s)
		*vec.Entries[0].ReadCloserForRead = s
		return nil
	}}
	p := strictProbeParam(fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin"))
	p.Filepath, p.CompressType, p.FileService = "etl:/strict-boundary.csv", tree.NOCOMPRESS, fs
	return p, files
}

func strictTextCols(n int) []*plan.ColDef {
	cols := make([]*plan.ColDef, n)
	for i := range cols {
		cols[i] = &plan.ColDef{Typ: plan.Type{Id: int32(types.T_text)}}
	}
	return cols
}

func strictRangeFields(t *testing.T, p *tree.ExternParam, data string, start, end int64) [][]csvparser.Field {
	t.Helper()
	if end == -1 {
		end = int64(len(data))
	}
	parser, err := newCSVParserFromReader(p, strings.NewReader(data[start:end]))
	require.NoError(t, err)
	var rows [][]csvparser.Field
	for {
		fields, err := parser.Read(nil)
		if err == io.EOF {
			return rows
		}
		require.NoError(t, err, "range [%d,%d)", start, end)
		row := make([]csvparser.Field, len(fields))
		for i, f := range fields {
			f.Val = strings.Clone(f.Val)
			row[i] = f
		}
		rows = append(rows, row)
	}
}

func requireStrictRangesEqual(t *testing.T, p *tree.ExternParam, data string, ranges []int64) {
	t.Helper()
	want := strictRangeFields(t, p, data, p.FileStartOff, -1)
	var got [][]csvparser.Field
	for i := 0; i < len(ranges); i += 2 {
		if i > 0 {
			require.Equal(t, ranges[i-1], ranges[i])
		}
		got = append(got, strictRangeFields(t, p, data, ranges[i], ranges[i+1])...)
	}
	require.Len(t, got, len(want))
	for i := range want {
		require.Len(t, got[i], len(want[i]))
		for j := range want[i] {
			require.True(t, want[i][j] == got[i][j], "exact field mismatch row=%d col=%d lengths=%d/%d", i, j, len(want[i][j].Val), len(got[i][j].Val))
		}
	}
}

func strictBoundaryStarts(files *strictBoundaryTestFiles) []int64 {
	starts := make([]int64, 0, len(files.stages))
	for _, s := range files.stages {
		starts = append(starts, s.start)
	}
	return starts
}

func TestStrictVerifiedBoundaryPropagation(t *testing.T) {
	const long = 600 * 1024
	t.Run("extended then short multiline", func(t *testing.T) {
		const batch = 1238800
		data := strings.Repeat(strings.Repeat("x", long-1)+"\n", 5)
		data += "\"" + strings.Repeat("y", 20000) + "\nshort\"\n"
		data += strings.Repeat("z", 3*batch-len(data)-1) + "\n"
		p, fs := strictBoundaryParam(t, data)
		got, err := ReadFileOffset(p, 3, int64(len(data)), strictTextCols(1))
		require.NoError(t, err)
		require.Equal(t, []int64{0, 3 * long, 3 * long, -1}, got)
		require.Equal(t, []int64{batch, 0, 3*long + batch, 0}, strictBoundaryStarts(fs))
		require.LessOrEqual(t, fs.stages[2].bytes, strictProbeSoftBytes, "short suffix must still be verified")
		requireStrictRangesEqual(t, p, data, got)
		t.Logf("ranges=%v opens=%v suffix_reads=%d/%d", got, strictBoundaryStarts(fs), fs.stages[0].bytes, fs.stages[2].bytes)
	})
	t.Run("legacy success is not an anchor", func(t *testing.T) {
		const batch = 1000 * 1024
		data := strings.Repeat(strings.Repeat("x", 400*1024-1)+"\n", 3)
		data += strings.Repeat("y", 1536*1024-1) + "\n"
		data += strings.Repeat("z", 3*batch-len(data)-1) + "\n"
		p, fs := strictBoundaryParam(t, data)
		got, err := ReadFileOffset(p, 3, int64(len(data)), strictTextCols(1))
		require.NoError(t, err)
		require.Len(t, got, 6)
		require.Equal(t, []int64{batch, 3*400*1024 + batch, 0}, strictBoundaryStarts(fs))
		require.LessOrEqual(t, fs.stages[0].bytes, strictProbeSoftBytes)
		require.Greater(t, fs.stages[1].bytes, strictProbeSoftBytes)
		requireStrictRangesEqual(t, p, data, got)
		t.Logf("ranges=%v opens=%v", got, strictBoundaryStarts(fs))
	})
}

func TestStrictPrefetchOnlyExpansion(t *testing.T) {
	data := strings.Repeat(strings.Repeat("x", 500*1024-1)+"\n", 5)
	p, fs := strictBoundaryParam(t, data)
	got, err := ReadFileOffset(p, 2, int64(len(data)), strictTextCols(1))
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.LessOrEqual(t, got[1]-int64(len(data)/2), int64(strictProbeSoftBytes))
	require.Greater(t, fs.stages[0].bytes, strictProbeSoftBytes)
	require.Equal(t, []int64{int64(len(data) / 2), 0}, strictBoundaryStarts(fs))
	requireStrictRangesEqual(t, p, data, got)
	t.Logf("tail=%d physical=%d opens=%v", got[1]-int64(len(data)/2), fs.stages[0].bytes, strictBoundaryStarts(fs))
}

// A partial final parser block discovers real EOF only after reading the
// entire suffix. At the old cap, that read used to return a budget error.
func TestStrictBoundarySoftThreshold(t *testing.T) {
	for _, tc := range []struct {
		name, kind string
		delta      int
		wantRanges int
	}{
		{name: "below plain", delta: -1, wantRanges: 2},
		{name: "equal plain", wantRanges: 2},
		{name: "above plain", delta: 1, wantRanges: 2},
		{name: "equal BOM", kind: "BOM", wantRanges: 1},
		{name: "above BOM", kind: "BOM", delta: 1, wantRanges: 1},
		{name: "equal multiline", kind: "multiline", wantRanges: 1},
		{name: "above multiline", kind: "multiline", delta: 1, wantRanges: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const lastBytes = 6144
			suffixBytes := strictProbeSoftBytes + tc.delta
			firstBytes := 2*suffixBytes - lastBytes
			data := strings.Repeat("x", firstBytes-1) + "\n" + strings.Repeat("b", lastBytes-1) + "\n"
			switch tc.kind {
			case "BOM":
				data = strings.Repeat("x", firstBytes-1) + "\n\xef\xbb\xbf" + strings.Repeat("b", lastBytes-4) + "\n"
			case "multiline":
				data = "\"" + strings.Repeat("x", firstBytes-2) + "\n" + strings.Repeat("y", lastBytes-2) + "\"\n"
			}
			p, fs := strictBoundaryParam(t, data)
			got, err := ReadFileOffset(p, 2, int64(len(data)), strictTextCols(1))
			require.NoError(t, err)
			require.Equal(t, suffixBytes, fs.stages[0].bytes, "physical input, including prefetch")
			t.Logf("suffix_start=%d physical_bytes=%d candidate=%d file_bytes=%d ranges=%v", fs.stages[0].start, fs.stages[0].bytes, firstBytes, len(data), got)
			requireStrictRangesEqual(t, p, data, got)
			require.Len(t, got, 2*tc.wantRanges)
			if suffixBytes < strictProbeSoftBytes {
				require.Equal(t, []int64{int64(suffixBytes)}, strictBoundaryStarts(fs))
			} else {
				require.Equal(t, []int64{int64(suffixBytes), 0}, strictBoundaryStarts(fs))
			}
			for _, stream := range fs.stages {
				require.Equal(t, 1, stream.closes)
			}
		})
	}
}

func TestStrictBoundaryRestart(t *testing.T) {
	plain := strings.Repeat("x", 600*1024-1) + "\n"
	for _, tc := range []struct {
		name, data                     string
		start                          int64
		cols, readers, wantRanges      int
		field, line, starting, comment string
	}{
		{name: "short control", data: strings.Repeat(strings.Repeat("x", 400*1024-1)+"\n", 5), cols: 1, readers: 2, wantRanges: 2},
		{name: "long two readers", data: strings.Repeat(plain, 5), cols: 1, readers: 2, wantRanges: 2},
		{name: "long three readers", data: strings.Repeat(plain, 7), cols: 1, readers: 3, wantRanges: 3},
		{name: "long last field", data: strings.Repeat("1,"+strings.Repeat("x", 600*1024-3)+"\n", 5), cols: 2, readers: 2, wantRanges: 2},
		{name: "null first field", data: strings.Repeat("\\N,"+strings.Repeat("x", 600*1024-4)+"\n", 5), cols: 2, readers: 2, wantRanges: 2},
		{name: "no final newline", data: strings.TrimSuffix(strings.Repeat(plain, 5), "\n"), cols: 1, readers: 2, wantRanges: 2},
		{name: "file BOM", data: "\xef\xbb\xbf" + strings.Repeat(plain, 5), cols: 1, readers: 2, wantRanges: 2},
		{name: "nonzero start", data: "header\n" + strings.Repeat(plain, 5), start: 7, cols: 1, readers: 2, wantRanges: 2},
		{name: "BOM after nonzero start", data: "header\n\xef\xbb\xbf" + strings.Repeat(plain, 5), start: 7, cols: 1, readers: 2, wantRanges: 2},
		{name: "new range starts BOM", data: strings.Repeat(plain, 3) + "\xef\xbb\xbf" + strings.Repeat("b", 600*1024-4) + "\n" + plain, cols: 1, readers: 2, wantRanges: 1},
		{name: "single multiline field", data: strings.Repeat("\""+strings.Repeat("x", 900*1024)+"\n"+strings.Repeat("y", 300*1024)+"\"\n", 5), cols: 1, readers: 2, wantRanges: 1},
		{name: "two text columns internal legal row", data: strings.Repeat("left,\""+strings.Repeat("x", 900*1024)+"\ninner,shape\n"+strings.Repeat("y", 300*1024)+"\"\n", 5), cols: 2, readers: 2, wantRanges: 1},
		{name: "escaped multibyte tokens", data: strings.Repeat("left::\""+strings.Repeat("x", 900*1024)+"||inner::shape||"+strings.Repeat("y", 300*1024)+"\\\"z\"||", 5), cols: 2, readers: 2, wantRanges: 1, field: "::", line: "||"},
		{name: "skip restart", data: "header\n" + strings.Repeat("# comment\n\nkeep:1,"+strings.Repeat("x", 600*1024)+"\n", 5), start: 7, cols: 2, readers: 2, wantRanges: 2, starting: "keep:", comment: "#"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, fs := strictBoundaryParam(t, tc.data)
			p.FileStartOff = tc.start
			if tc.field != "" {
				p.Tail.Fields = &tree.Fields{Terminated: &tree.Terminated{Value: tc.field}}
			}
			p.Tail.Lines = &tree.Lines{StartingBy: tc.starting}
			if tc.line != "" {
				p.Tail.Lines.TerminatedBy = &tree.Terminated{Value: tc.line}
			}
			p.Option = []string{"comment", tc.comment}
			got, err := ReadFileOffset(p, tc.readers, int64(len(tc.data)), strictTextCols(tc.cols))
			require.NoError(t, err)
			require.Len(t, got, 2*tc.wantRanges)
			if len(fs.stages) > 1 {
				require.Equal(t, tc.start, fs.stages[1].start)
			}
			requireStrictRangesEqual(t, p, tc.data, got)
			for _, stream := range fs.stages {
				require.Equal(t, 1, stream.closes)
			}
		})
	}
}

// A lazy stream keeps skip-path budget tests independent of fixture size.
type repeatingStrictInput struct {
	pattern string
	offset  int
}

func (r *repeatingStrictInput) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.pattern[r.offset]
		r.offset = (r.offset + 1) % len(r.pattern)
	}
	return len(p), nil
}

func TestStrictAggregateProbeBudget(t *testing.T) {
	p := strictProbeParam(t.Context())
	cfg := *CSVConfigForLoad(p)
	cfg.MaxRecordSize = 16
	const data = "1,a\n2,b\n3,c\n"
	t.Run("two streams one byte budget", func(t *testing.T) {
		a := &strictProbeAttempt{remaining: 20}
		r1 := &strictCountingReader{Reader: strings.NewReader(data)}
		tail, err := getTailSizeStrictAttempt(p, strictTextCols(2), r1, &cfg, a)
		require.NoError(t, err)
		require.Equal(t, int64(4), tail)
		r2 := &strictCountingReader{Reader: strings.NewReader(data)}
		err = verifyStrictTarget(p, &cfg, r2, 8, a)
		require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
		require.Equal(t, 20, r1.bytes+r2.bytes)
		require.Zero(t, a.remaining)
		t.Logf("bytes=%d+%d calls=%d remaining=%d", r1.bytes, r2.bytes, a.calls, a.remaining)
	})
	t.Run("two streams one work budget", func(t *testing.T) {
		a := &strictProbeAttempt{remaining: 1000, calls: 255}
		_, err := getTailSizeStrictAttempt(p, strictTextCols(2), strings.NewReader(data), &cfg, a)
		require.NoError(t, err)
		r2 := &strictCountingReader{Reader: strings.NewReader(data)}
		err = verifyStrictTarget(p, &cfg, r2, 8, a)
		require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
		require.Equal(t, 256, a.calls)
		require.Zero(t, r2.bytes)
	})
	t.Run("huge prefix no read", func(t *testing.T) {
		a := &strictProbeAttempt{remaining: 32}
		r := &strictCountingReader{Reader: &repeatingStrictInput{pattern: "x\n"}}
		err := verifyStrictTarget(p, &cfg, r, 33, a)
		require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
		require.Zero(t, r.bytes)
	})
	t.Run("short records finite work", func(t *testing.T) {
		a := &strictProbeAttempt{remaining: 1 << 20}
		r := &strictCountingReader{Reader: strings.NewReader(strings.Repeat("x\n", 1000))}
		err := verifyStrictTarget(p, &cfg, r, 1500, a)
		require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
		require.Equal(t, 256, a.calls)
		require.LessOrEqual(t, r.bytes, 1<<20)
	})
	for _, tc := range []struct{ name, pattern, comment, starting string }{
		{"comments", "#x\n", "#", ""}, {"empty", "\n", "", ""}, {"starting", "skip\n", "", "keep:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			own := cfg
			own.Comment = tc.comment
			own.LinesStartingBy = tc.starting
			a := &strictProbeAttempt{remaining: 4096}
			r := &strictCountingReader{Reader: &repeatingStrictInput{pattern: tc.pattern}}
			err := verifyStrictTarget(p, &own, r, 8, a)
			require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
			require.Equal(t, 4096, r.bytes)
			require.Equal(t, 1, a.calls)
		})
	}
	t.Run("cancel extended verification", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		own := *p
		own.Ctx = ctx
		a := &strictProbeAttempt{remaining: 1 << 20}
		r := &strictCountingReader{Reader: &repeatingStrictInput{pattern: "x\n"}, onRead: cancel}
		err := verifyStrictTarget(&own, &cfg, r, 900000, a)
		require.ErrorIs(t, err, context.Canceled)
		require.LessOrEqual(t, r.bytes, strictProbeBlockSize*int(csvparser.BufferSizeScale))
	})
}

func TestStrictRestartPrefixReadBudget(t *testing.T) {
	p := strictProbeParam(t.Context())
	cfg := *CSVConfigForLoad(p)
	cfg.MaxRecordSize = strictProbeBlockSize*csvparser.BufferSizeScale + 8
	row := strings.Repeat("x", int(cfg.MaxRecordSize)-9) + "\n"
	target := int64(len(row))
	for _, tc := range []struct {
		name, prefix string
		extra        int64
		want         error
	}{
		{"plain", "xyz\n", 3, nil},
		{"BOM", "\xef\xbb\xbfx\n", 3, errStrictBoundaryUnverified},
		{"budget truncates prefix", "xyz\n", 2, errStrictProbeBudgetExceeded},
		{"real EOF incomplete prefix", "xy", 3, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &strictProbeAttempt{remaining: target + tc.extra}
			r := &strictCountingReader{Reader: strings.NewReader(row + tc.prefix)}
			err := verifyStrictTarget(p, &cfg, r, target, a)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			require.LessOrEqual(t, int64(r.bytes), target+tc.extra)
			require.Equal(t, 1, a.calls)
			t.Logf("target=%d consumed=%d remaining=%d", target, r.bytes, a.remaining)
		})
	}
}

func TestStrictRestartPrefixShortReads(t *testing.T) {
	p := strictProbeParam(t.Context())
	cfg := *CSVConfigForLoad(p)
	cfg.MaxRecordSize = 32
	for _, chunk := range []int{1, 2} {
		for _, prefix := range []string{"xyz\n", "\xef\xbb\xbftail\n"} {
			t.Run(fmt.Sprintf("chunk=%d BOM=%t", chunk, strings.HasPrefix(prefix, "\xef\xbb\xbf")), func(t *testing.T) {
				r := &strictBoundaryTestReader{Reader: strings.NewReader("abc\n" + prefix), maxRead: chunk}
				a := &strictProbeAttempt{remaining: 128}
				err := verifyStrictTarget(p, &cfg, r, 4, a)
				if strings.HasPrefix(prefix, "\xef\xbb\xbf") {
					require.ErrorIs(t, err, errStrictBoundaryUnverified)
				} else {
					require.NoError(t, err)
				}
				require.Greater(t, r.reads, 1)
				require.Zero(t, r.closes, "verification borrows the stream")
				require.Equal(t, int64(r.bytes), 128-a.remaining)
			})
		}
	}
}

func TestStrictTwoStageProbeErrors(t *testing.T) {
	data := strings.Repeat(strings.Repeat("x", 600*1024-1)+"\n", 5)
	fail := errors.New("strict boundary I/O failure")
	for _, tc := range []struct {
		name                       string
		stage                      int
		readErr, closeErr, openErr error
		cancel                     bool
		want                       error
		opens                      int
	}{
		{name: "suffix close", closeErr: fail, want: fail, opens: 1},
		{name: "suffix wrapped cancel wins close", readErr: fmt.Errorf("read: %w", context.Canceled), closeErr: fail, want: context.Canceled, opens: 1},
		{name: "verifier open", stage: 1, openErr: fail, want: fail, opens: 1},
		{name: "verifier read", stage: 1, readErr: fail, opens: 2},
		{name: "verifier close", stage: 1, closeErr: fail, want: fail, opens: 2},
		{name: "verifier read and close", stage: 1, readErr: fail, closeErr: fail, want: fail, opens: 2},
		{name: "verifier EOF and close", stage: 1, readErr: io.EOF, closeErr: fail, want: fail, opens: 2},
		{name: "verifier wrapped cancel wins close", stage: 1, readErr: fmt.Errorf("read: %w", context.Canceled), closeErr: fail, want: context.Canceled, opens: 2},
		{name: "verifier wrapped deadline wins close", stage: 1, readErr: fmt.Errorf("read: %w", context.DeadlineExceeded), closeErr: fail, want: context.DeadlineExceeded, opens: 2},
		{name: "verifier context wins close", stage: 1, closeErr: fail, cancel: true, want: context.Canceled, opens: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, fs := strictBoundaryParam(t, data)
			ctx, cancel := context.WithCancel(p.Ctx)
			defer cancel()
			p.Ctx = ctx
			if tc.openErr != nil {
				fs.failOpen = 2
				fs.openErr = tc.openErr
			}
			fs.configure = func(i int, s *strictBoundaryTestReader) {
				if i != tc.stage {
					return
				}
				s.readErr, s.closeErr = tc.readErr, tc.closeErr
				if tc.cancel {
					s.onRead = cancel
				}
			}
			got, err := ReadFileOffset(p, 2, int64(len(data)), strictTextCols(1))
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, []int64{0, -1}, got)
			}
			require.Len(t, fs.stages, tc.opens)
			for _, stream := range fs.stages {
				require.Equal(t, 1, stream.closes)
			}
		})
	}

	t.Run("verifier failure retains previous", func(t *testing.T) {
		const batch = 1000 * 1024
		data := strings.Repeat(strings.Repeat("x", 400*1024-1)+"\n", 3)
		data += strings.Repeat("y", 1536*1024-1) + "\n"
		data += strings.Repeat("z", 3*batch-len(data)-1) + "\n"
		p, fs := strictBoundaryParam(t, data)
		fs.configure = func(i int, s *strictBoundaryTestReader) {
			if i == 2 {
				s.readErr = fail
			}
		}
		got, err := ReadFileOffset(p, 3, int64(len(data)), strictTextCols(1))
		require.NoError(t, err)
		require.Equal(t, []int64{0, 3 * 400 * 1024, 3 * 400 * 1024, -1}, got)
		for _, s := range fs.stages {
			require.Equal(t, 1, s.closes)
		}
	})
}

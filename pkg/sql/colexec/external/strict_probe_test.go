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

package external

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/stretchr/testify/require"
)

type strictCountingReader struct {
	io.Reader
	bytes  int
	onRead func()
}

func (r *strictCountingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += n
	if r.onRead != nil {
		r.onRead()
	}
	return n, err
}
func strictProbeParam(ctx context.Context) *tree.ExternParam {
	return &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV, Tail: &tree.TailParameter{}}, ExParam: tree.ExParam{Ctx: ctx, Strict: true}}
}
func TestStrictProbeInputBudget(t *testing.T) {
	cols := []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_varchar)}}}
	for _, tc := range []struct {
		name, data, terminator, starting, comment string
		success                                   bool
	}{
		{name: "complete record below cap", data: strings.Repeat("x", strictProbeSoftBytes-2) + "\n", success: true},
		{name: "real EOF below cap without newline", data: strings.Repeat("x", strictProbeSoftBytes-1), success: true},
		{name: "artificial EOF is not a final record", data: strings.Repeat("x", strictProbeSoftBytes)},
		{name: "unterminated", data: strings.Repeat("x", strictProbeSoftBytes+1)},
		{name: "quoted field", data: "x,\"" + strings.Repeat("x", strictProbeSoftBytes) + "\"\n"},
		{name: "quote prefix", data: strings.Repeat("\"", strictProbeSoftBytes+1)},
		{name: "comments", data: strings.Repeat("#x\n", strictProbeSoftBytes/3+1), comment: "#"},
		{name: "starting by", data: strings.Repeat("skip\n", strictProbeSoftBytes/5+1), starting: "keep:"},
		{name: "quoted terminator", data: "x,\"" + strings.Repeat("x", strictProbeSoftBytes-3) + "\"\n"},
		{name: "escape cutoff", data: "x,\"" + strings.Repeat("x", strictProbeSoftBytes-4) + "\\\"\"\n"},
		{name: "CRLF cutoff", data: strings.Repeat("x", strictProbeSoftBytes-1) + "\r\n", terminator: "\r\n"},
		{name: "multibyte cutoff", data: strings.Repeat("x", strictProbeSoftBytes-1) + "||", terminator: "||"},
		{name: "complete record in partial final block", data: strings.Repeat("x", strictProbeSoftBytes-1) + "\n"},
		{name: "complete record on block boundary", data: strings.Repeat("x", strictProbeBlockSize*5*3-1) + "\n", success: true},
		{name: "short record in large file", data: "1\n" + strings.Repeat("x", strictProbeSoftBytes), success: true},
		{name: "real EOF without newline", data: "last", success: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := strictProbeParam(t.Context())
			p.Tail.Lines = &tree.Lines{StartingBy: tc.starting}
			if tc.terminator != "" {
				p.Tail.Lines.TerminatedBy = &tree.Terminated{Value: tc.terminator}
			}
			p.Option = []string{"comment", tc.comment}
			r := &strictCountingReader{Reader: strings.NewReader(tc.data)}
			cfg := *CSVConfigForLoad(p)
			cfg.MaxRecordSize = 2 * strictProbeSoftBytes
			tail, err := getTailSizeStrictWithLimit(p, cols, r, &cfg, strictProbeSoftBytes)
			if tc.success {
				require.NoError(t, err)
				if tc.name == "short record in large file" {
					require.Equal(t, int64(2), tail)
				} else {
					require.Equal(t, int64(len(tc.data)), tail)
				}
			} else {
				require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
				require.Zero(t, tail)
			}
			require.LessOrEqual(t, r.bytes, strictProbeSoftBytes)
		})
	}
}

func TestStrictProbeInputLimit(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		weight                     int64
		field, quote, line, prefix string
		scale                      int64
		want                       int64
	}{
		{name: "default record weight", weight: 64 << 20, field: ",", quote: "\"", line: "\n", scale: 5, want: 403087372},
		{name: "plain small weight", weight: 32, field: ",", scale: 5, want: 434252},
		{name: "tokens and prefix", weight: 32, field: "::", quote: "'", line: "END", prefix: "keep:", scale: 5, want: 434456},
		{name: "field bytes not runes", weight: 32, field: "分", quote: "\"", line: "\n", scale: 5, want: 434508},
		{name: "enclosure bytes", weight: 32, field: ",", quote: "“", line: "\n", scale: 5, want: 434636},
		{name: "read block scale", weight: 1, field: ",", quote: "\"", line: "\n", scale: 1, want: 303122},
		{name: "largest representable budget", weight: (math.MaxInt64-434176)/2 - 6, field: ",", scale: 5, want: math.MaxInt64 - 1},
		{name: "zero weight", field: ",", scale: 5},
		{name: "negative weight", weight: -1, field: ",", scale: 5},
		{name: "empty field token", weight: 32, scale: 5},
		{name: "zero scale", weight: 32, field: ","},
		{name: "negative scale", weight: 32, field: ",", scale: -1},
		{name: "record product overflow", weight: math.MaxInt64, field: ",", quote: "\"", scale: 5},
		{name: "record sum overflow", weight: math.MaxInt64, field: ",", scale: 5},
		{name: "prefix sum overflow", weight: math.MaxInt64 - 2, field: ",", prefix: "p", scale: 5},
		{name: "two records overflow", weight: math.MaxInt64 / 2, field: ",", scale: 5},
		{name: "block product overflow", weight: 32, field: ",", scale: math.MaxInt64/strictProbeBlockSize + 1},
		{name: "read ahead sum overflow", weight: 32, field: ",", scale: math.MaxInt64 / strictProbeBlockSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := csvparser.CSVConfig{MaxRecordSize: tc.weight, FieldsTerminatedBy: tc.field, FieldsEnclosedBy: tc.quote, LinesTerminatedBy: tc.line, LinesStartingBy: tc.prefix}
			got, ok := strictProbeInputLimit(&cfg, tc.scale)
			require.Equal(t, tc.want != 0, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestStrictProbeRecordEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name, field, data string
		columns           int
		weight            int64
	}{
		{name: "field metadata and multibyte separators", field: "::", data: "1" + strings.Repeat("::", 31) + "\n", columns: 32, weight: 33},
		{name: "doubled quotes", field: ",", data: "1,\"" + strings.Repeat("\"\"", 4) + "\"\n", columns: 2, weight: 11},
		{name: "raw backslash pairs", field: ",", data: "1,\"" + strings.Repeat("\\\\", 4) + "\"\n", columns: 2, weight: 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := strictProbeParam(t.Context())
			cfg := *CSVConfigForLoad(p)
			cfg.FieldsTerminatedBy = tc.field
			for _, weight := range []int64{tc.weight, tc.weight - 1} {
				cfg.MaxRecordSize = weight
				limit, ok := strictProbeInputLimit(&cfg, csvparser.BufferSizeScale)
				require.True(t, ok)
				tail, err := getTailSizeStrictWithLimit(p, strictTextCols(tc.columns), strings.NewReader(tc.data), &cfg, limit)
				if weight == tc.weight {
					require.NoError(t, err)
					require.Equal(t, int64(len(tc.data)), tail)
				} else {
					require.ErrorContains(t, err, "max record size")
					require.Zero(t, tail)
				}
			}
		})
	}
}

func TestStrictProbeSoftBudget(t *testing.T) {
	p := strictProbeParam(t.Context())
	cfg := *CSVConfigForLoad(p)
	cfg.MaxRecordSize = 2 * strictProbeSoftBytes
	limit, ok := strictProbeInputLimit(&cfg, csvparser.BufferSizeScale)
	require.True(t, ok)
	cols := []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}, {Typ: plan.Type{Id: int32(types.T_text)}}}
	t.Run("second long candidate completes", func(t *testing.T) {
		data := strings.Repeat("x", 150*1024) + "\n1," + strings.Repeat("y", 150*1024) + "\n"
		a := &strictProbeAttempt{remaining: limit}
		tail, err := getTailSizeStrictAttempt(p, cols, strings.NewReader(data), &cfg, a)
		require.NoError(t, err)
		require.Equal(t, int64(len(data)), tail)
		require.Equal(t, 2, a.calls)
		require.Greater(t, limit-a.remaining, int64(strictProbeSoftBytes))
	})
	for _, total := range []int{strictProbeSoftBytes - 1, strictProbeSoftBytes, strictProbeSoftBytes + 1} {
		t.Run(fmt.Sprintf("third candidate after %d physical bytes", total), func(t *testing.T) {
			const last = "1,ok\n"
			first := (total - len(last)) / 2
			second := total - len(last) - first
			bad := func(size int) string { return "bad," + strings.Repeat("x", size-5) + "\n" }
			data := bad(first) + bad(second) + last
			a := &strictProbeAttempt{remaining: limit}
			tail, err := getTailSizeStrictAttempt(p, cols, strings.NewReader(data), &cfg, a)
			require.Equal(t, int64(total), limit-a.remaining, "include parser prefetch in the soft threshold")
			if total < strictProbeSoftBytes {
				require.NoError(t, err)
				require.Equal(t, int64(total), tail)
				require.Equal(t, 3, a.calls)
			} else {
				require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
				require.Zero(t, tail)
				require.Equal(t, 2, a.calls)
			}
		})
	}
}

func BenchmarkStrictProbeShortRecord(b *testing.B) {
	p := strictProbeParam(b.Context())
	cols := []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := getTailSizeStrict(p, cols, io.NopCloser(strings.NewReader("123\n"))); err != nil {
			b.Fatal(err)
		}
	}
}

func TestStrictProbeCandidateBudget(t *testing.T) {
	cols := []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}
	for _, tc := range []struct {
		name    string
		invalid int
		success bool
	}{
		{"last allowed candidate", strictProbeMaxRecords - 1, true},
		{"next candidate rejected", strictProbeMaxRecords, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := strings.Repeat("x\n", tc.invalid) + "123\n"
			tail, err := getTailSizeStrict(strictProbeParam(t.Context()), cols, io.NopCloser(strings.NewReader(data)))
			if tc.success {
				require.NoError(t, err)
				require.Equal(t, int64(len(data)), tail)
			} else {
				require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
				require.Zero(t, tail)
			}
		})
	}
}

func TestStrictProbeCancellation(t *testing.T) {
	for _, data := range []string{strings.Repeat("x\n", 1000), strings.Repeat("x", strictProbeSoftBytes+1), strings.Repeat("\"", strictProbeSoftBytes+1)} {
		ctx, cancel := context.WithCancel(t.Context())
		r := &strictCountingReader{Reader: strings.NewReader(data), onRead: cancel}
		_, err := getTailSizeStrict(strictProbeParam(ctx), []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}, io.NopCloser(r))
		cancel()
		require.ErrorIs(t, err, context.Canceled)
		require.LessOrEqual(t, r.bytes, strictProbeBlockSize*5)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r := &strictCountingReader{Reader: strings.NewReader("1\n")}
	_, err := getTailSizeStrict(strictProbeParam(ctx), nil, io.NopCloser(r))
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, r.bytes)
}

func TestStrictProbeExpectedFailuresDoNotReport(t *testing.T) {
	var reports atomic.Int64
	old := errutil.GetReportErrorFunc()
	errutil.SetErrorReporter(func(context.Context, error, int) { reports.Add(1) })
	defer errutil.SetErrorReporter(old)
	cols := []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}}}
	_, err := getTailSizeStrict(strictProbeParam(t.Context()), cols, io.NopCloser(strings.NewReader(strings.Repeat("st00000000000000000000\n", strictProbeMaxRecords+1))))
	require.ErrorIs(t, err, errStrictProbeBudgetExceeded)
	require.Zero(t, reports.Load())
}

func TestReadFileOffsetRetainsConfirmedBoundaries(t *testing.T) {
	data := strings.Repeat("0001\n", 400) + strings.Repeat("x\n", 1000)
	fs := newPrescanTrackingFS(t, data)
	p := strictProbeParam(fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin"))
	p.Filepath = "etl:/prescan.csv"
	p.CompressType = tree.NOCOMPRESS
	p.FileService = fs
	offsets, err := ReadFileOffset(p, 3, int64(len(data)), []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}})
	require.NoError(t, err)
	require.Len(t, offsets, 4)
	require.Equal(t, int64(0), offsets[0])
	require.Equal(t, offsets[1], offsets[2])
	require.Equal(t, int64(-1), offsets[3])
	require.Greater(t, offsets[1], int64(len(data)/3))
	require.Less(t, offsets[1], int64(2000))
	require.Equal(t, data, data[:offsets[1]]+data[offsets[2]:])
	require.Len(t, fs.readers, 2)
	for _, r := range fs.readers {
		require.Equal(t, 1, r.closes)
	}
}

func TestReadFileOffsetStrictErrorPrecedence(t *testing.T) {
	closeErr := errors.New("close failed")
	for _, tc := range []struct {
		name                  string
		readErr, errorOnClose error
		cancel                bool
		want                  error
	}{
		{name: "scan cancel", readErr: fmt.Errorf("read: %w", context.Canceled), want: context.Canceled},
		{name: "scan timeout", readErr: fmt.Errorf("read: %w", context.DeadlineExceeded), want: context.DeadlineExceeded},
		{name: "close after success", errorOnClose: closeErr, want: closeErr},
		{name: "close after EOF", readErr: io.EOF, errorOnClose: closeErr, want: closeErr},
		{name: "close after read failure", readErr: errors.New("read failed"), errorOnClose: closeErr, want: closeErr},
		{name: "canceled context wins", errorOnClose: closeErr, cancel: true, want: context.Canceled},
		{name: "canceled read wins close", readErr: context.Canceled, errorOnClose: closeErr, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin"))
			defer cancel()
			fs := newPrescanTrackingFS(t, "1\n2\n3\n4\n5\n6\n")
			fs.readErr = tc.readErr
			fs.closeErr = tc.errorOnClose
			if tc.cancel {
				fs.onRead = cancel
			}
			p := strictProbeParam(ctx)
			p.Filepath = "etl:/prescan.csv"
			p.CompressType = tree.NOCOMPRESS
			p.FileService = fs
			offsets, err := ReadFileOffset(p, 4, 12, nil)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, offsets)
			require.Equal(t, 1, fs.readCalls)
			require.Len(t, fs.readers, 1)
			require.Equal(t, 1, fs.readers[0].closes)
		})
	}
	t.Run("pre-cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		fs := newPrescanTrackingFS(t, "1\n2\n")
		p := strictProbeParam(ctx)
		p.Filepath = "etl:/prescan.csv"
		p.FileService = fs
		for _, n := range []int{1, 4} {
			_, err := ReadFileOffset(p, n, 4, nil)
			require.ErrorIs(t, err, context.Canceled)
		}
		require.Zero(t, fs.readCalls)
	})
}

func TestStrictLegalityQuietConversions(t *testing.T) {
	var reports atomic.Int64
	old := errutil.GetReportErrorFunc()
	errutil.SetErrorReporter(func(context.Context, error, int) { reports.Add(1) })
	defer errutil.SetErrorReporter(old)
	ctx := errutil.ContextWithNoReport(t.Context(), true)
	for _, tc := range []struct {
		name          string
		oid           types.T
		value, domain string
		quoted        bool
	}{
		{name: "bool", oid: types.T_bool, value: "bad"},
		{name: "decimal64", oid: types.T_decimal64, value: "bad"},
		{name: "decimal128", oid: types.T_decimal128, value: "1e999999999"},
		{name: "decimal256", oid: types.T_decimal256, value: "0x" + strings.Repeat("f", 80)},
		{name: "fixed float32", oid: types.T_float32, value: "bad"},
		{name: "fixed float64", oid: types.T_float64, value: "bad"},
		{name: "date", oid: types.T_date, value: "bad"},
		{name: "time", oid: types.T_time, value: "2026-99-99 12:34:56"},
		{name: "datetime", oid: types.T_datetime, value: "bad"},
		{name: "timestamp", oid: types.T_timestamp, value: "bad"},
		{name: "quoted year", oid: types.T_year, value: "100", quoted: true},
		{name: "numeric year", oid: types.T_year, value: "100"},
		{name: "enum", oid: types.T_enum, value: "bad", domain: "red,blue"},
		{name: "set", oid: types.T_uint64, value: "bad", domain: "red,blue"},
		{name: "set metadata normalization", oid: types.T_uint64, value: " bad ", domain: "red,RED"},
		{name: "array32", oid: types.T_array_float32, value: "[NaN]"},
		{name: "array64", oid: types.T_array_float64, value: "[Inf]"},
		{name: "array bf16", oid: types.T_array_bf16, value: "[3.4e38]"},
		{name: "array float16", oid: types.T_array_float16, value: "[70000]"},
		{name: "array int8", oid: types.T_array_int8, value: "[128]"},
		{name: "array uint8", oid: types.T_array_uint8, value: "[-1]"},
		{name: "json syntax", oid: types.T_json, value: `{"x":}`},
		{name: "json representation", oid: types.T_json, value: `{"x":1e9999}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cols := []*plan.ColDef{{Name: "d", Typ: plan.Type{Id: int32(tc.oid), Width: 12, Scale: 2, Enumvalues: tc.domain}}}
			fields := []csvparser.Field{{Val: tc.value, HasStringQuote: tc.quoted}}
			require.False(t, isLegalLineWithContext(ctx, strictProbeParam(t.Context()), cols, fields))
			require.Zero(t, reports.Load())
		})
	}
}

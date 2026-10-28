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
		{name: "unterminated", data: strings.Repeat("x", strictProbeMaxBytes+1)},
		{name: "quoted field", data: "x,\"" + strings.Repeat("x", strictProbeMaxBytes) + "\"\n"},
		{name: "quote prefix", data: strings.Repeat("\"", strictProbeMaxBytes+1)},
		{name: "comments", data: strings.Repeat("#x\n", strictProbeMaxBytes/3+1), comment: "#"},
		{name: "starting by", data: strings.Repeat("skip\n", strictProbeMaxBytes/5+1), starting: "keep:"},
		{name: "quoted terminator", data: "x,\"" + strings.Repeat("x", strictProbeMaxBytes-3) + "\"\n"},
		{name: "escape cutoff", data: "x,\"" + strings.Repeat("x", strictProbeMaxBytes-4) + "\\\"\"\n"},
		{name: "CRLF cutoff", data: strings.Repeat("x", strictProbeMaxBytes-1) + "\r\n", terminator: "\r\n"},
		{name: "multibyte cutoff", data: strings.Repeat("x", strictProbeMaxBytes-1) + "||", terminator: "||"},
		{name: "complete record in partial final block", data: strings.Repeat("x", strictProbeMaxBytes-1) + "\n"},
		{name: "complete record on block boundary", data: strings.Repeat("x", strictProbeBlockSize*5*3-1) + "\n", success: true},
		{name: "short record in large file", data: "1\n" + strings.Repeat("x", strictProbeMaxBytes), success: true},
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
			tail, err := getTailSizeStrict(p, cols, io.NopCloser(r))
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
			require.LessOrEqual(t, r.bytes, strictProbeMaxBytes)
		})
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
	for _, data := range []string{strings.Repeat("x\n", 1000), strings.Repeat("x", strictProbeMaxBytes+1), strings.Repeat("\"", strictProbeMaxBytes+1)} {
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

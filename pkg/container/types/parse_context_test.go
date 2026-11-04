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

package types

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/stretchr/testify/require"
)

type parseContextFamily struct {
	name           string
	normal         func(string) (any, error)
	contextual     func(context.Context, string) (any, error)
	valid, invalid []string
}

func parseFamily[T any](name string, normal func(string) (T, error), contextual func(context.Context, string) (T, error), valid, invalid []string) parseContextFamily {
	return parseContextFamily{name: name, valid: valid, invalid: invalid,
		normal:     func(s string) (any, error) { return normal(s) },
		contextual: func(ctx context.Context, s string) (any, error) { return contextual(ctx, s) },
	}
}
func arrayParseFamily[T ArrayElement](name string, valid, invalid []string) parseContextFamily {
	return parseFamily(name, StringToArrayToBytes[T], StringToArrayToBytesWithContext[T], valid, invalid)
}

// The same parsing algorithms serve normal execution and speculative probes.
// Compare all semantic results while observing reporting at construction time,
// including failures hidden by an eventual successful fallback.
func TestParseContextReporting(t *testing.T) {
	families := make([]parseContextFamily, 0, 21)
	families = append(families,
		parseFamily("bool", ParseBool, ParseBoolWithContext, []string{"TrUe", "2.5", "0"}, []string{"bad"}),
		parseFamily("date", ParseDateCast, ParseDateCastWithContext, []string{"2026-10-27", "0000-00-00"}, []string{"bad", "2026-99-99"}),
		parseFamily("time", func(s string) (Time, error) { return ParseTime(s, 6) }, func(c context.Context, s string) (Time, error) { return ParseTimeWithContext(c, s, 6) }, []string{"12:34:56.123456", "2026-10-27 12:34:56"}, []string{"bad", "12:99:99", "2026-99-99 12:34:56"}),
		parseFamily("datetime", func(s string) (Datetime, error) { return ParseDatetime(s, 6) }, func(c context.Context, s string) (Datetime, error) { return ParseDatetimeWithContext(c, s, 6) }, []string{"2026-10-27 12:34:56.123456", "2026-10-27"}, []string{"bad", "2026-10-27 99:99:99", "2026-10-27 12:34:56.bad"}),
		parseFamily("timestamp", func(s string) (Timestamp, error) { return ParseTimestamp(time.UTC, s, 6) }, func(c context.Context, s string) (Timestamp, error) {
			return ParseTimestampWithContext(c, time.UTC, s, 6)
		}, []string{"2026-10-27 12:34:56", "0000-00-00"}, []string{"bad", "2026-99-99"}),
		parseFamily("year", ParseMoYear, ParseMoYearWithContext, []string{"0", "00", "0000", "69", "1970"}, []string{"", "bad", "0100", "2156"}),
		parseFamily("numeric year", func(s string) (MoYear, error) { return ParseMoYearFromInt(int64(len(s))) }, func(c context.Context, s string) (MoYear, error) {
			return ParseMoYearFromIntWithContext(c, int64(len(s)))
		}, []string{"", "x"}, []string{strings.Repeat("x", 100)}),
		parseFamily("enum", func(s string) (Enum, error) { return ParseEnum("red,blue", s) }, func(c context.Context, s string) (Enum, error) { return ParseEnumWithContext(c, "red,blue", s) }, []string{"RED", "2", "0"}, []string{"bad", "3"}),
		parseFamily("enum metadata", func(s string) (Enum, error) { return ParseEnum(s, "red") }, func(c context.Context, s string) (Enum, error) { return ParseEnumWithContext(c, s, "red") }, []string{"red,blue"}, []string{strings.Repeat("x,", MaxEnumLen)}),
		parseFamily("set", func(s string) (uint64, error) { return ParseSet("red,blue", s) }, func(c context.Context, s string) (uint64, error) { return ParseSetWithContext(c, "red,blue", s) }, []string{"red,blue", "2", ""}, []string{"bad", "4", "red,bad"}),
		parseFamily("set metadata", func(s string) (uint64, error) { return ParseSet(s, "red") }, func(c context.Context, s string) (uint64, error) { return ParseSetWithContext(c, s, "red") }, []string{"red,blue"}, []string{"", "red,RED", strings.Repeat("x,", 65)}),
		parseFamily("json", ParseStringToByteJson, ParseStringToByteJsonWithContext, []string{`{"x":[1,true,null]}`, `18446744073709551616`}, []string{"", `{"x":}`, `1e9999`, `{"x":1e9999}`, `{"` + strings.Repeat("k", 65536) + `":0}`}),
		arrayParseFamily[float32]("vecf32", []string{"[1,2]"}, []string{"bad", "[]", "[bad]", "[NaN]", "[Inf]", "[1e99]", "[" + strings.Repeat("1,", MaxArrayDimension) + "1]"}),
		arrayParseFamily[float64]("vecf64", []string{"[1,2]"}, []string{"[bad]", "[NaN]", "[-Inf]", "[1e9999]"}),
		arrayParseFamily[BF16]("vecbf16", []string{"[1,2]"}, []string{"[bad]", "[NaN]", "[3.4e38]"}),
		arrayParseFamily[Float16]("vecf16", []string{"[1,2]"}, []string{"[bad]", "[NaN]", "[70000]"}),
		arrayParseFamily[int8]("veci8", []string{"[-128,127]"}, []string{"[128]", "[1.4]"}),
		arrayParseFamily[uint8]("vecu8", []string{"[0,255]"}, []string{"[-1]", "[256]"}),
	)
	for _, width := range []int32{18, 38, 76} {
		var f parseContextFamily
		switch width {
		case 18:
			f = parseFamily("decimal64", func(s string) (Decimal64, error) { return ParseDecimal64(s, width, 2) }, func(c context.Context, s string) (Decimal64, error) { return ParseDecimal64WithContext(c, s, width, 2) }, nil, nil)
		case 38:
			f = parseFamily("decimal128", func(s string) (Decimal128, error) { return ParseDecimal128(s, width, 2) }, func(c context.Context, s string) (Decimal128, error) {
				return ParseDecimal128WithContext(c, s, width, 2)
			}, nil, nil)
		case 76:
			f = parseFamily("decimal256", func(s string) (Decimal256, error) { return ParseDecimal256(s, width, 2) }, func(c context.Context, s string) (Decimal256, error) {
				return ParseDecimal256WithContext(c, s, width, 2)
			}, nil, nil)
		}
		f.valid = []string{"1.25", "-1.995", "0x10", "1e-19", "0.001", strings.Repeat("9", int(width)-2) + ".99"}
		f.invalid = []string{"", "st0000", "1e+", "1e2147483648", "1e2147483647", "0x" + strings.Repeat("f", int(width)), strings.Repeat("9", int(width)) + ".995", strings.Repeat("9", int(width)+1)}
		families = append(families, f)
	}
	var reports atomic.Int64
	old := errutil.GetReportErrorFunc()
	errutil.SetErrorReporter(func(context.Context, error, int) { reports.Add(1) })
	defer errutil.SetErrorReporter(old)
	quiet := errutil.ContextWithNoReport(t.Context(), true)
	for _, f := range families {
		t.Run(f.name, func(t *testing.T) {
			for _, group := range []struct {
				name    string
				inputs  []string
				invalid bool
			}{{"valid", f.valid, false}, {"invalid", f.invalid, true}} {
				for i, s := range group.inputs {
					t.Run(group.name+"/"+string(rune('a'+i)), func(t *testing.T) {
						// Cold metadata failures must be silent too, before a legacy call could
						// populate any descriptor cache. Repeat to cover warm successful metadata.
						for range 2 {
							before := reports.Load()
							got, err := f.contextual(quiet, s)
							require.Equal(t, before, reports.Load(), "silent input %q", s)
							want, wantErr := f.normal(s)
							require.Equal(t, group.invalid, err != nil, "input %q: %v", s, err)
							require.Equal(t, want, got)
							if wantErr == nil {
								require.NoError(t, err)
							} else {
								require.EqualError(t, err, wantErr.Error())
								var a, b *moerr.Error
								require.ErrorAs(t, err, &a)
								require.ErrorAs(t, wantErr, &b)
								require.Equal(t, b.ErrorCode(), a.ErrorCode())
								require.Equal(t, b.MySQLCode(), a.MySQLCode())
								require.Equal(t, b.SqlState(), a.SqlState())
								require.Greater(t, reports.Load(), before, "normal failures still report")
							}
						}
					})
				}
			}
		})
	}
}

func TestParseContextConcurrentIsolation(t *testing.T) {
	type contextKey struct{}
	ordinary := context.WithValue(t.Context(), contextKey{}, "ordinary")
	quiet := errutil.ContextWithNoReport(context.WithValue(t.Context(), contextKey{}, "quiet"), true)
	var ordinaryReports, unexpected atomic.Int64
	old := errutil.GetReportErrorFunc()
	errutil.SetErrorReporter(func(ctx context.Context, _ error, _ int) {
		if ctx.Value(contextKey{}) == "ordinary" {
			ordinaryReports.Add(1)
		} else {
			unexpected.Add(1)
		}
	})
	defer errutil.SetErrorReporter(old)
	var wg sync.WaitGroup
	for _, ctx := range []context.Context{ordinary, quiet} {
		wg.Go(func() {
			for range 100 {
				_, _ = ParseDecimal64WithContext(ctx, "invalid", 12, 2)
				_, _ = ParseSetWithContext(ctx, "fresh-cold-red,fresh-cold-blue", "bad")
				_, _ = ParseStringToByteJsonWithContext(ctx, `{"x":1e9999}`)
			}
		})
	}
	wg.Wait()
	require.Equal(t, int64(400), ordinaryReports.Load(), "two decimal constructors plus set and JSON, ordinary calls only")
	require.Zero(t, unexpected.Load())
}

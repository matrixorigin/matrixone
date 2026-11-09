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
	"bufio"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/stretchr/testify/require"
)

func TestJSONLinePendingSplits(t *testing.T) {
	for _, input := range []string{`{"a":[{"x":"}\\\"["},123,1.2e-3,"\u1234"]}`, `[1,{"x":"\\\"}"},[true,null]]`} {
		node, err := bytejson.ParseNodeString(input)
		require.NoError(t, err)
		node.Free()
		for i := 1; i < len(input); i++ {
			var p jsonLinePending
			require.NoError(t, p.append(input[:i], int64(len(input)+1)))
			require.False(t, p.closed, "premature close at %d in %s", i, input)
			require.NoError(t, p.append(input[i:], int64(len(input)+1)))
			require.True(t, p.closed, "missing close at %d in %s", i, input)
			node, err := bytejson.ParseNodeString(string(p.data))
			require.NoError(t, err)
			node.Free()
		}
	}
}

func TestJSONLinePendingGrowthNearLimit(t *testing.T) {
	const limit = 4096
	for _, tc := range []struct {
		name     string
		initial  int
		complete bool
	}{
		{"below-half-complete", 2047, true},
		{"at-half-truncated", 2048, false},
		{"above-half-complete", 2049, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p jsonLinePending
			first := `{"a":"` + strings.Repeat("x", tc.initial-len(`{"a":"`))
			require.NoError(t, p.append(first, limit))
			copied := 0
			appendFragment := func(fragment string) {
				oldLen, oldCap := len(p.data), cap(p.data)
				require.NoError(t, p.append(fragment, limit))
				if cap(p.data) != oldCap {
					copied += oldLen
				}
				require.LessOrEqual(t, cap(p.data), limit-1)
			}
			end := limit - 1
			if tc.complete {
				end -= 2
			}
			for len(p.data) < end {
				appendFragment("x")
			}
			if tc.complete {
				appendFragment(`"}`)
				require.True(t, p.closed)
				node, err := bytejson.ParseNodeString(string(p.data))
				require.NoError(t, err)
				node.Free()
			} else {
				require.False(t, p.closed)
			}
			require.Len(t, p.data, limit-1)
			require.LessOrEqual(t, copied, 2*limit)
			before := string(p.data)
			require.ErrorContains(t, p.append("x", limit), "max record size")
			require.Equal(t, before, string(p.data))
		})
	}
}

func TestJSONLinePendingBudgetAndLimit(t *testing.T) {
	for _, shape := range []string{tree.OBJECT, tree.ARRAY} {
		for _, extra := range []int{0, 1} {
			t.Run(shape+string(rune('0'+extra)), func(t *testing.T) {
				param, proc, bat := errorModeParam(t, tree.JSONLINE, shape, colLine)
				param.ErrorMode.Tolerate = false
				param.maxBatchSize = 1
				var first, second string
				// Avoid CSV enclosure interpretation at the start of a continuation.
				if shape == tree.OBJECT {
					first, second = `{"a":`, `1,"s":"x"}`
				} else {
					first, second = `[1,`, ` "x"]`
				}
				content := first + "\n" + second + "\n"
				r := &CsvReader{param: param, reader: io.NopCloser(strings.NewReader(content))}
				t.Cleanup(func() { require.NoError(t, r.Close()) })
				cfg := *CSVConfigForLoad(param.Extern)
				cfg.MaxRecordSize = int64(len(first) + len(second) + 1 - extra)
				parser, err := csvparser.NewCSVParser(&cfg, bufio.NewReader(r.reader), csvparser.ReadBlockSize, false)
				require.NoError(t, err)
				r.plh = &ParseLineHandler{csvReader: parser}
				finished, err := r.makeBatchRows(proc, bat)
				require.NoError(t, err)
				require.False(t, finished)
				require.Equal(t, 0, bat.RowCount())
				require.Equal(t, first, string(r.pending.data))
				finished, err = r.makeBatchRows(proc, bat)
				if extra == 1 {
					require.ErrorContains(t, err, "max record size")
					require.Empty(t, r.pending.data)
					return
				}
				require.NoError(t, err)
				require.False(t, finished)
				require.Equal(t, 1, bat.RowCount())
				require.Equal(t, int32(1), vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[colA], 0))
				require.Equal(t, "x", string(bat.Vecs[colS].GetBytesAt(0)))
				require.Empty(t, r.pending.data)
			})
		}
	}
}

func TestJSONLineClosedTruncatedRemainsPending(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, colLine)
	param.maxBatchSize = 1
	r := &CsvReader{param: param, reader: io.NopCloser(strings.NewReader("{invalid}\nmore\n"))}
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	parser, err := newCSVParserFromReader(param.Extern, r.reader)
	require.NoError(t, err)
	r.plh = &ParseLineHandler{csvReader: parser}
	_, err = r.makeBatchRows(proc, bat)
	require.NoError(t, err)
	require.True(t, r.pending.sealed)
	_, err = r.makeBatchRows(proc, bat)
	require.NoError(t, err)
	_, err = r.makeBatchRows(proc, bat)
	require.ErrorContains(t, err, "incomplete json record")
	require.Empty(t, r.pending.data)
}

func TestJSONLinePendingCancelAndClose(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, colLine)
	param.maxBatchSize = 1
	r := &CsvReader{param: param, reader: io.NopCloser(strings.NewReader(`{"a":` + "\n1,\n"))}
	parser, err := newCSVParserFromReader(param.Extern, r.reader)
	require.NoError(t, err)
	r.plh = &ParseLineHandler{csvReader: parser}
	_, err = r.makeBatchRows(proc, bat)
	require.NoError(t, err)
	require.NotEmpty(t, r.pending.data)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()
	_, err = r.makeBatchRows(proc, bat)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, r.pending.data)
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
}

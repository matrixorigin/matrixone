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
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestQAExternalCallJSONLineBudget(t *testing.T) {
	for _, budget := range []uint64{0, 5, 1024} {
		t.Run(string(rune('a'+budget%26)), func(t *testing.T) {
			param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, colLine)
			param.maxBatchSize = budget
			param.Extern.ScanType = tree.INLINE
			r := &CsvReader{param: param, reader: io.NopCloser(strings.NewReader("{\"a\":\n1,\"s\":\"x\"}\n{\"a\":2,\"s\":\"y\"}\n"))}
			parser, err := newCSVParserFromReader(param.Extern, r.reader)
			require.NoError(t, err)
			r.plh = &ParseLineHandler{csvReader: parser}
			ext := &External{Es: param, reader: r, fileOpened: true}
			ext.ctr.buf = bat
			ext.OpAnalyzer = process.NewAnalyzer(0, true, true, "qa")
			t.Cleanup(func() { require.NoError(t, r.Close()) })
			var got []string
			calls := 0
			for ; calls < 8; calls++ {
				result, err := ext.Call(proc)
				require.NoError(t, err)
				if result.Status == vm.ExecStop {
					break
				}
				for i := 0; i < result.Batch.RowCount(); i++ {
					got = append(got, string(result.Batch.Vecs[colS].GetBytesAt(i)))
					require.Equal(t, int32(len(got)), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[colA], i))
				}
			}
			require.Less(t, calls, 8, "must make progress to EOF")
			require.Equal(t, []string{"x", "y"}, got)
			if budget <= 5 {
				require.GreaterOrEqual(t, calls, 3)
			}
		})
	}
}

func TestQAJSONLineInvalidSuffix(t *testing.T) {
	for _, suffix := range []string{`1,"s":"x"}garbage`, `1,"s":"x"}{"a":2}`, `1,"s":"x"]`} {
		t.Run(suffix, func(t *testing.T) {
			param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, colLine)
			param.maxBatchSize = 1
			err := readAllText(t, param, proc, bat, "{\"a\":\n"+suffix+"\n")
			require.Error(t, err)
			require.Equal(t, 0, bat.RowCount())
		})
	}
}

type qaFailReader struct{ source *strings.Reader }

func (r *qaFailReader) Read(p []byte) (int, error) {
	n, err := r.source.Read(p)
	if err == io.EOF {
		return 0, errors.New("qa read failure")
	}
	return n, err
}
func (*qaFailReader) Close() error { return nil }

func TestQAExternalCallReadErrorClearsPending(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, colLine)
	param.Extern.ScanType = tree.INLINE
	param.maxBatchSize = 1
	r := &CsvReader{param: param, reader: &qaFailReader{source: strings.NewReader("{\"a\":\n" + strings.Repeat("x\n", 20))}}
	parser, err := newCSVParserFromReaderWithBlockSize(param.Extern, r.reader, 4)
	require.NoError(t, err)
	r.plh = &ParseLineHandler{csvReader: parser}
	ext := &External{Es: param, reader: r, fileOpened: true}
	ext.ctr.buf = bat
	ext.OpAnalyzer = process.NewAnalyzer(0, true, true, "qa")
	_, err = ext.Call(proc)
	require.NoError(t, err)
	require.NotEmpty(t, r.pending.data)
	param.maxBatchSize = 1 << 20
	_, err = ext.Call(proc)
	require.ErrorContains(t, err, "qa read failure")
	require.Empty(t, r.pending.data)
	require.Nil(t, r.reader)
	require.True(t, param.Fileparam.End)
	require.Equal(t, 0, bat.RowCount())
}

func TestQATolerantPhysicalRecordLimit(t *testing.T) {
	param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, numTestCols)
	r := &CsvReader{param: param, reader: io.NopCloser(strings.NewReader(`{"a":123456789,"s":"x"}` + "\n"))}
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	cfg := *CSVConfigForLoad(param.Extern)
	cfg.MaxRecordSize = 12
	parser, err := csvparser.NewCSVParser(&cfg, bufio.NewReader(r.reader), csvparser.ReadBlockSize, false)
	require.NoError(t, err)
	r.plh = &ParseLineHandler{csvReader: parser}
	_, err = r.makeBatchRows(proc, bat)
	require.ErrorContains(t, err, "max record size")
	require.Equal(t, 0, bat.RowCount())
}

// CSV and JSONLINE share the parser's effective limit, including the default.
func TestQAConfigFallbackAndCSVLimit(t *testing.T) {
	for _, configured := range []int64{-1, 0, 7} {
		cfg := *CSVConfigForLoad((&tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV, Tail: &tree.TailParameter{}}}))
		cfg.MaxRecordSize = configured
		parser, err := csvparser.NewCSVParser(&cfg, strings.NewReader(""), 4, false)
		require.NoError(t, err)
		want := configured
		if want <= 0 {
			want = int64(config.LargestEntryLimit)
		}
		require.Equal(t, want, parser.MaxRecordSize())
	}
	for _, n := range []int{6, 7} {
		cfg := *CSVConfigForLoad((&tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV, Tail: &tree.TailParameter{}}}))
		cfg.MaxRecordSize = 7
		parser, err := csvparser.NewCSVParser(&cfg, strings.NewReader(strings.Repeat("a", n)+"\n"), 4, false)
		require.NoError(t, err)
		row, err := parser.Read(nil)
		if n == 6 {
			require.NoError(t, err)
			require.Equal(t, strings.Repeat("a", n), row[0].Val)
		} else {
			require.ErrorContains(t, err, "max record size")
		}
	}
}

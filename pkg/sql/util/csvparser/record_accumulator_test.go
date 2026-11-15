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

package csvparser

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestFragmentedRecordsReuseAccumulator(t *testing.T) {
	want := [][]string{
		{"prefix", strings.Repeat("ab", 113), "tail"},
		{strings.Repeat("x", 177), "quoted\n" + strings.Repeat("y", 91) + "\"end", "last"},
		{"short", "", "row"},
	}
	var input bytes.Buffer
	writer := csv.NewWriter(&input)
	require.NoError(t, writer.WriteAll(want))
	for _, blockSize := range []int64{1, 7, 64} {
		t.Run(fmt.Sprintf("block=%d", blockSize), func(t *testing.T) {
			cfg := &CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n"}
			parser, err := NewCSVParser(cfg, strings.NewReader(input.String()), blockSize, false)
			require.NoError(t, err)
			for pass := range 2 {
				var row []Field
				for _, fields := range want {
					row, err = parser.Read(row)
					require.NoError(t, err)
					require.Len(t, row, len(fields))
					for i, field := range fields {
						require.Equal(t, field, row[i].Val)
						require.False(t, row[i].IsNull)
					}
				}
				_, err = parser.Read(row)
				require.ErrorIs(t, err, io.EOF)
				require.Equal(t, int64(input.Len()), parser.Pos())
				if pass == 0 {
					parser.Reset(strings.NewReader(input.String()))
				}
			}
		})
	}
}

func TestFragmentedFieldReadError(t *testing.T) {
	for _, quoted := range []bool{false, true} {
		t.Run(fmt.Sprintf("quoted=%t", quoted), func(t *testing.T) {
			input := "id," + strings.Repeat("x", 40)
			if quoted {
				input = "id,\"" + strings.Repeat("x", 40)
			}
			cfg := &CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n"}
			parser, err := NewCSVParser(cfg, iotest.TimeoutReader(strings.NewReader(input)), 1, false)
			require.NoError(t, err)
			_, err = parser.Read(nil)
			require.ErrorIs(t, err, iotest.ErrTimeout)
		})
	}
}

// Oversized data after a closed quoted field must respect the raw bound. The
// raw admission bound must still reject it before growing the accumulator.
func TestFragmentedInvalidQuotedSuffix(t *testing.T) {
	for _, blockSize := range []int64{1, 3, 17, 128} {
		t.Run(fmt.Sprintf("block=%d", blockSize), func(t *testing.T) {
			cfg := &CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n"}
			parser, err := NewCSVParser(cfg, strings.NewReader("\"x\""+strings.Repeat("a", 70)+"\n"), blockSize, false)
			require.NoError(t, err)
			parser.LimitRecordBytes(64)
			_, err = parser.Read(nil)
			require.ErrorContains(t, err, "txn-entry-size-limit")
		})
	}
}

func BenchmarkCSVRecordAccumulator(b *testing.B) {
	for _, shape := range []struct {
		name                      string
		columns, fieldBytes, rows int
		quoted                    bool
	}{
		{"small", 8, 12, 64, false},
		{"wide", 128, 12, 16, false},
		{"medium", 8, 1024, 8, false},
		{"field64KiB", 1, 64 << 10, 1, false},
		{"field1MiB", 1, 1 << 20, 1, false},
		{"field8MiB", 1, 8 << 20, 1, false},
		{"largeWide", 8, 128 << 10, 1, false},
		{"quoted1MiB", 1, 1 << 20, 1, true},
	} {
		b.Run(shape.name, func(b *testing.B) {
			fields := make([]string, shape.columns)
			for i := range fields {
				fields[i] = strings.Repeat("x", shape.fieldBytes)
			}
			if shape.quoted {
				fields[0] = strings.Repeat("x", shape.fieldBytes-2) + "\n\""
			}
			var encoded bytes.Buffer
			writer := csv.NewWriter(&encoded)
			for range shape.rows {
				if err := writer.Write(fields); err != nil {
					b.Fatal(err)
				}
			}
			writer.Flush()
			if err := writer.Error(); err != nil {
				b.Fatal(err)
			}
			input := encoded.String()
			cfg := &CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n"}
			for _, reuse := range []bool{false, true} {
				mode := "cold"
				if reuse {
					mode = "reuse"
				}
				b.Run(mode, func(b *testing.B) {
					reader := strings.NewReader(input)
					parser, err := NewCSVParser(cfg, reader, ReadBlockSize, false)
					if err != nil {
						b.Fatal(err)
					}
					var row []Field
					read := func() {
						for range shape.rows {
							var err error
							row, err = parser.Read(row)
							if err != nil {
								b.Fatal(err)
							}
							if len(row) != shape.columns || len(row[0].Val) != len(fields[0]) {
								b.Fatal("incorrect decoded record")
							}
						}
						if _, err := parser.Read(row); err != io.EOF {
							b.Fatalf("expected EOF, got %v", err)
						}
					}
					read() // warm the reusable row and input buffers outside the measurement.
					b.ReportAllocs()
					b.SetBytes(int64(len(input)))
					b.ResetTimer()
					for range b.N {
						reader.Reset(input)
						if reuse {
							parser.Reset(reader)
						} else {
							parser, err = NewCSVParser(cfg, reader, ReadBlockSize, false)
							if err != nil {
								b.Fatal(err)
							}
						}
						read()
					}
				})
			}
		})
	}
}

func BenchmarkCSVRecordAccumulatorParallel(b *testing.B) {
	for _, size := range []int{12, 1 << 20} {
		b.Run(fmt.Sprintf("fieldBytes=%d", size), func(b *testing.B) {
			columns, rows := 8, 64
			if size > 12 {
				columns, rows = 1, 1
			}
			input := strings.Repeat(strings.Repeat(strings.Repeat("x", size)+",", columns-1)+strings.Repeat("x", size)+"\n", rows)
			cfg := &CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n"}
			type worker struct {
				parser *CSVParser
				reader *strings.Reader
				row    []Field
			}
			workers := make([]worker, runtime.GOMAXPROCS(0))
			for i := range workers {
				w := &workers[i]
				w.reader = strings.NewReader(input)
				var err error
				w.parser, err = NewCSVParser(cfg, w.reader, ReadBlockSize, false)
				if err != nil {
					b.Fatal(err)
				}
				for range rows {
					w.row, err = w.parser.Read(w.row)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			var next atomic.Int64
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				// The prepared slots are immutable after handoff. Keep the hot row
				// header local so independent readers do not share a writable cache line.
				w := workers[next.Add(1)-1]
				for pb.Next() {
					w.reader.Reset(input)
					w.parser.Reset(w.reader)
					for range rows {
						var err error
						w.row, err = w.parser.Read(w.row)
						if err != nil {
							b.Error(err)
							return
						}
						if len(w.row) != columns || len(w.row[0].Val) != size {
							b.Error("incorrect decoded record")
							return
						}
					}
					if _, err := w.parser.Read(w.row); err != io.EOF {
						b.Errorf("expected EOF, got %v", err)
						return
					}
				}
			})
		})
	}
}

func TestFragmentedRecordGrowthDenied(t *testing.T) {
	for _, prefix := range []string{"", "row:"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			cfg := &CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n", LinesStartingBy: prefix}
			parser, err := NewCSVParser(cfg, strings.NewReader(prefix+"id,"+strings.Repeat("x", 40)+"\n"), 1, false)
			require.NoError(t, err)
			parser.LimitRecordBytes(128)
			denied := errors.New("fragment growth denied")
			calls := 0
			parser.SetRecordBudget(8, func(need int64) (int64, error) {
				calls++
				require.Greater(t, need, int64(8))
				return 0, denied
			})
			_, err = parser.Read(nil)
			require.ErrorIs(t, err, denied)
			require.Equal(t, 1, calls)
		})
	}
}

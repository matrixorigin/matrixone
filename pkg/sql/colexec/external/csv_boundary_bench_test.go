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
	"io"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
)

// Compare the former Read-based planner with record-only framing under the
// same production parser configuration, eight ranges, and input/read-ahead.
// Timing is diagnostic evidence, not a machine-dependent pass threshold.
func BenchmarkNonStrictCSVBoundaryFraming(b *testing.B) {
	for _, tc := range []struct{ name, record string }{
		{"plain", "1,alpha,beta,gamma\n"},
		{"escaped_multiline", "1,\"a\"\"b\\\\c\nx\",beta,gamma\n"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			data := bytes.Repeat([]byte(tc.record), (16<<20)/len(tc.record))
			want, wantRead, err := benchmarkCSVBoundaryOffsets(data, true)
			if err != nil {
				b.Fatal(err)
			}
			for _, decode := range []bool{true, false} {
				name := "ScanRecord"
				if decode {
					name = "Read"
				}
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(data)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						got, read, err := benchmarkCSVBoundaryOffsets(data, decode)
						if err != nil || read != wantRead || !slices.Equal(want, got) {
							b.Fatalf("boundary/read mismatch: offsets=%v want=%v read=%d want=%d err=%v", got, want, read, wantRead, err)
						}
					}
					b.ReportMetric(float64(wantRead), "read-B/op")
				})
			}
		})
	}
}

func benchmarkCSVBoundaryOffsets(data []byte, decode bool) ([]int64, int64, error) {
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV, Tail: &tree.TailParameter{}}}
	offsets := make([]int64, 1, 9)
	var bytesRead int64
	for i := 1; i < 8; i++ {
		start := offsets[len(offsets)-1]
		reader := &boundaryBenchmarkReader{Reader: bytes.NewReader(data[start:])}
		parser, err := newCSVParserFromReader(param, reader)
		if err != nil {
			return nil, bytesRead, err
		}
		guess := start + int64(len(data)/8)
		var fields []csvparser.Field
		for start+parser.Pos() < guess {
			if decode {
				fields, err = parser.Read(fields)
			} else {
				err = parser.ScanRecord()
			}
			if err != nil {
				return nil, bytesRead, err
			}
		}
		offsets = append(offsets, start+parser.Pos())
		bytesRead += reader.bytes
	}
	return append(offsets, int64(len(data))), bytesRead, nil
}

type boundaryBenchmarkReader struct {
	io.Reader
	bytes int64
}

func (r *boundaryBenchmarkReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += int64(n)
	return n, err
}

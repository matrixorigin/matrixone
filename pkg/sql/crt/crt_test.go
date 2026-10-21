// Copyright 2022 - 2025 Matrix Origin
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

package crt

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/pierrec/lz4/v4"
	"github.com/smartystreets/goconvey/convey"
)

func Test_getCompressType(t *testing.T) {
	convey.Convey("getCompressType succ", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				CompressType: tree.GZIP,
			},
			ExParam: tree.ExParam{
				Ctx: context.Background(),
			},
		}
		compress := GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, param.CompressType)

		param.CompressType = tree.AUTO
		param.Filepath = "a.gz"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.GZIP)

		param.Filepath = "a.bz2"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.BZIP2)

		param.Filepath = "a.lz4"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.LZ4)

		param.Filepath = "a.csv"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)

		param.Filepath = "a"
		compress = GetCompressType(param.CompressType, param.Filepath)
		convey.So(compress, convey.ShouldEqual, tree.NOCOMPRESS)
	})
}

func Test_getUnCompressReader(t *testing.T) {
	convey.Convey("getUnCompressReader succ", t, func() {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				CompressType: tree.NOCOMPRESS,
			},
		}

		ctx := context.Background()

		read, err := getUnCompressReader(ctx, param.CompressType, param.Filepath, nil)
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.BZIP2
		read, err = getUnCompressReader(ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.FLATE
		read, err = getUnCompressReader(ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZ4
		read, err = getUnCompressReader(param.Ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)

		param.CompressType = tree.LZW
		read, err = getUnCompressReader(ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		param.CompressType = "abc"
		read, err = getUnCompressReader(ctx, param.CompressType, param.Filepath, &os.File{})
		convey.So(read, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

// csvLoadPart renders one ten-row CSV part, the unit each compressed frame
// carries in the #30652 repro.
func csvLoadPart(start int) []byte {
	var b bytes.Buffer
	for i := start; i < start+10; i++ {
		fmt.Fprintf(&b, "%d,row-%d\n", i, i)
	}
	return b.Bytes()
}

// buildCompressedParts writes one independently-closed compression stream per
// part: a complete multi-frame/multi-member file, like the zstd -c pipeline
// that produced the repro files.
func buildCompressedParts(
	t *testing.T,
	newWriter func(io.Writer) io.WriteCloser,
	parts ...[]byte,
) []byte {
	var out bytes.Buffer
	for _, part := range parts {
		w := newWriter(&out)
		_, err := w.Write(part)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return out.Bytes()
}

// newZstdWriter adapts klauspost's two-value constructor for the formats
// table; default options match a plain `zstd -c` run.
func newZstdWriter(t *testing.T) func(io.Writer) io.WriteCloser {
	return func(w io.Writer) io.WriteCloser {
		zw, err := zstd.NewWriter(w)
		if err != nil {
			t.Fatal(err)
		}
		return zw
	}
}

// TestDecompressReaderRejectsTruncatedStreams pins #30652: a compressed
// stream whose framing ends early must surface a load error instead of a
// clean io.EOF that makes consumers commit the decompressed prefix.  The
// complete-prefix control stays a legal single-frame file.
func TestDecompressReaderRejectsTruncatedStreams(t *testing.T) {
	part1 := csvLoadPart(1)
	part2 := csvLoadPart(11)
	full := append(append([]byte{}, part1...), part2...)

	formats := []struct {
		name      string
		compType  string
		newWriter func(io.Writer) io.WriteCloser
		cut       int
	}{
		{"zstd", tree.ZSTD, newZstdWriter(t), 6},
		{"gzip", tree.GZIP, func(w io.Writer) io.WriteCloser { return gzip.NewWriter(w) }, 6},
		{"lz4", tree.LZ4, func(w io.Writer) io.WriteCloser { return lz4.NewWriter(w) }, 6},
	}

	for _, f := range formats {
		t.Run(f.name, func(t *testing.T) {
			good := buildCompressedParts(t, f.newWriter, part1, part2)
			frame1 := buildCompressedParts(t, f.newWriter, part1)
			bad := good[:len(good)-f.cut]

			ctx := context.Background()

			// control 1: complete file decompresses cleanly
			r, err := getUnCompressReader(ctx, f.compType, "load.csv", io.NopCloser(bytes.NewReader(good)))
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("complete stream errored: %v", err)
			}
			if !bytes.Equal(data, full) {
				t.Fatalf("complete stream mismatch: got %d bytes, want %d", len(data), len(full))
			}

			// control 2: a file that stops at the first intact frame is legal
			r, err = getUnCompressReader(ctx, f.compType, "load.csv", io.NopCloser(bytes.NewReader(frame1)))
			if err != nil {
				t.Fatal(err)
			}
			data, err = io.ReadAll(r)
			if err != nil {
				t.Fatalf("single-frame stream errored: %v", err)
			}
			if !bytes.Equal(data, part1) {
				t.Fatalf("single-frame stream mismatch: got %d bytes, want %d", len(data), len(part1))
			}

			// the issue: frame 2 cut short must fail the load, not end cleanly
			r, err = getUnCompressReader(ctx, f.compType, "load.csv", io.NopCloser(bytes.NewReader(bad)))
			if err != nil {
				t.Fatal(err)
			}
			data, err = io.ReadAll(r)
			if err == nil {
				t.Fatalf("truncated stream reported success with %d of %d bytes", len(data), len(full))
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncation surfaced a bare EOF sentinel: %v", err)
			}
			if !moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
				t.Fatalf("truncation surfaced unexpected error: %v", err)
			}
			if len(data) != len(full) {
				t.Logf("truncated stream stopped at %d of %d bytes with %v", len(data), len(full), err)
			}
		})
	}
}

// TestCSVParserRejectsTruncatedCompressedLoad closes the loop end to end:
// CSVParser.readBlock treats io.ErrUnexpectedEOF from a plain file as a legal
// short final block, and before #30652 the same sentinel from a truncated
// decompressor made the load commit the decompressed prefix and succeed.
func TestCSVParserRejectsTruncatedCompressedLoad(t *testing.T) {
	part1 := csvLoadPart(1)
	part2 := csvLoadPart(11)
	newZstd := newZstdWriter(t)
	good := buildCompressedParts(t, newZstd, part1, part2)
	bad := good[:len(good)-6]

	cases := []struct {
		name     string
		raw      []byte
		wantRows int
		wantFail bool
	}{
		{"complete two-frame file", good, 20, false},
		{"truncated second frame", bad, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := getUnCompressReader(
				context.Background(), tree.ZSTD, "load.csv.zst",
				io.NopCloser(bytes.NewReader(tc.raw)))
			if err != nil {
				t.Fatal(err)
			}

			cfg := csvparser.CSVConfig{
				FieldsTerminatedBy: ",",
				LinesTerminatedBy:  "\n",
			}
			parser, err := csvparser.NewCSVParser(&cfg, rc, csvparser.ReadBlockSize, false)
			if err != nil {
				t.Fatal(err)
			}

			rows := 0
			var loadErr error
			for {
				_, err := parser.Read(nil)
				if err == io.EOF {
					break
				}
				if err != nil {
					loadErr = err
					break
				}
				rows++
				if rows > 20 {
					t.Fatalf("runaway parser: %d rows", rows)
				}
			}

			if tc.wantFail {
				if loadErr == nil {
					t.Fatalf("truncated load committed %d rows and reported success", rows)
				}
				if !moerr.IsMoErrCode(loadErr, moerr.ErrInvalidInput) {
					t.Fatalf("unexpected load error: %v", loadErr)
				}
				// The repro file fits in one 64 KiB block, so the truncation
				// surfaces during the first fill and zero rows parse; a
				// large real-world file fails the same way at its tail block,
				// aborting the load transaction before any commit.
				if rows != 0 {
					t.Fatalf("expected no parsed rows before the truncation error, got %d", rows)
				}
			} else {
				if loadErr != nil {
					t.Fatalf("complete load failed: %v", loadErr)
				}
				if rows != tc.wantRows {
					t.Fatalf("complete load parsed %d rows, want %d", rows, tc.wantRows)
				}
			}
		})
	}
}

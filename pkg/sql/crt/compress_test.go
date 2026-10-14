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

package crt

import (
	"archive/tar"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"io"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func TestGetCompressType(t *testing.T) {
	for _, c := range []struct {
		option, path, want string
	}{
		{"", "a.csv", tree.NOCOMPRESS},
		{"auto", "a.csv.gz", tree.GZIP},
		{"AUTO", "a.csv.GZ", tree.GZIP},
		{"", "a.csv.gzip", tree.GZIP},
		{"", "a.csv.bz2", tree.BZIP2},
		{"", "a.csv.bzip2", tree.BZIP2},
		{"", "a.csv.lz4", tree.LZ4},
		{"", "a.csv.LZ4", tree.LZ4},
		{"", "a.csv.zst", tree.ZSTD},
		{"", "a.csv.ZSTD", tree.ZSTD},
		{"", "a.tar.gz", tree.TAR_GZ},
		{"", "a.TAR.GZ", tree.TAR_GZ},
		{"", "a.tar.bz2", tree.TAR_BZ2},
		{"", "dir.gz/a.csv", tree.NOCOMPRESS},
		// An explicit option wins over the extension and is lower-cased, so a
		// planner test against NOCOMPRESS cannot be fooled by "NONE".
		{"GZIP", "a.csv", tree.GZIP},
		{"NONE", "a.csv.gz", tree.NOCOMPRESS},
		{"flate", "a.csv", tree.FLATE},
	} {
		require.Equal(t, c.want, GetCompressType(c.option, c.path), "%q %q", c.option, c.path)
	}
}

type closeCounter struct {
	io.Reader
	closes int
}

func (c *closeCounter) Close() error {
	c.closes++
	return nil
}

func compressWith(t *testing.T, kind string, payload []byte) []byte {
	var buf bytes.Buffer
	switch kind {
	case tree.GZIP:
		w := gzip.NewWriter(&buf)
		_, err := w.Write(payload)
		require.NoError(t, err)
		require.NoError(t, w.Close())
	case tree.FLATE:
		w, err := flate.NewWriter(&buf, flate.DefaultCompression)
		require.NoError(t, err)
		_, err = w.Write(payload)
		require.NoError(t, err)
		require.NoError(t, w.Close())
	case tree.ZLIB:
		w := zlib.NewWriter(&buf)
		_, err := w.Write(payload)
		require.NoError(t, err)
		require.NoError(t, w.Close())
	case tree.LZ4:
		w := lz4.NewWriter(&buf)
		_, err := w.Write(payload)
		require.NoError(t, err)
		require.NoError(t, w.Close())
	case tree.TAR_GZ:
		gz := gzip.NewWriter(&buf)
		tw := tar.NewWriter(gz)
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: "a.csv", Mode: 0o644, Size: int64(len(payload))}))
		_, err := tw.Write(payload)
		require.NoError(t, err)
		require.NoError(t, tw.Close())
		require.NoError(t, gz.Close())
	case tree.ZSTD:
		w, err := zstd.NewWriter(&buf)
		require.NoError(t, err)
		_, err = w.Write(payload)
		require.NoError(t, err)
		require.NoError(t, w.Close())
	case tree.NOCOMPRESS:
		buf.Write(payload)
	default:
		t.Fatalf("no writer for %s", kind)
	}
	return buf.Bytes()
}

// Every decompressor owns its source: reading through it round-trips the
// payload, and closing it closes the source exactly once.  Before, the
// decoders' Close left the file or object stream open until a finalizer ran.
func TestUnCompressReaderClosesSource(t *testing.T) {
	payload := bytes.Repeat([]byte("1,alpha,2026-09-23\n"), 1000)
	for _, kind := range []string{tree.NOCOMPRESS, tree.GZIP, tree.FLATE, tree.ZLIB, tree.LZ4, tree.ZSTD, tree.TAR_GZ} {
		src := &closeCounter{Reader: bytes.NewReader(compressWith(t, kind, payload))}
		r, err := getUnCompressReader(context.Background(), kind, "", src)
		require.NoError(t, err, kind)
		got, err := io.ReadAll(r)
		require.NoError(t, err, kind)
		require.Equal(t, payload, got, kind)
		require.NoError(t, r.Close(), kind)
		require.Equal(t, 1, src.closes, kind)
	}

	// bzip2 has no writer in the standard library: use a committed resource.
	for kind, path := range map[string]string{
		tree.BZIP2:   "../../../test/distributed/resources/load_data/integer_numbers_1.jl.bz2",
		tree.TAR_BZ2: "../../../test/distributed/resources/load_data/text.csv.tar.bz2",
	} {
		f, err := os.Open(path)
		require.NoError(t, err)
		src := &closeCounter{Reader: f}
		r, err := getUnCompressReader(context.Background(), "", path, src)
		require.NoError(t, err, kind)
		got, err := io.ReadAll(r)
		require.NoError(t, err, kind)
		require.NotEmpty(t, got, kind)
		require.NoError(t, r.Close(), kind)
		require.Equal(t, 1, src.closes, kind)
		require.NoError(t, f.Close())
	}
}

// On error the source is still the caller's: the reader does not close it.
func TestUnCompressReaderErrorLeavesSourceOpen(t *testing.T) {
	src := &closeCounter{Reader: bytes.NewReader([]byte("not gzip"))}
	_, err := getUnCompressReader(context.Background(), tree.GZIP, "", src)
	require.Error(t, err)
	require.Zero(t, src.closes)

	_, err = getUnCompressReader(context.Background(), tree.LZW, "", src)
	require.Error(t, err)
	require.Zero(t, src.closes)
}

// The zstd CLI writes a large input as several frames; they decode as one
// stream.
func TestZstdConcatenatedFrames(t *testing.T) {
	var buf bytes.Buffer
	for _, part := range []string{"1,a\n", "2,b\n", "3,c\n"} {
		w, err := zstd.NewWriter(&buf)
		require.NoError(t, err)
		_, err = w.Write([]byte(part))
		require.NoError(t, err)
		require.NoError(t, w.Close())
	}
	src := &closeCounter{Reader: &buf}
	r, err := getUnCompressReader(context.Background(), "", "x.csv.zst", src)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "1,a\n2,b\n3,c\n", string(got))
	require.NoError(t, r.Close())
	require.Equal(t, 1, src.closes)
}

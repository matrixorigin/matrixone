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

package fileservice_test

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type cosProbeTransport func(*http.Request) (*http.Response, error)

func (f cosProbeTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type cosProbeBody struct {
	io.Reader
	reads, bytes int
	size         int
	closed       atomic.Bool // S3FS also retains a finalizer as a cleanup fallback.
}

func (b *cosProbeBody) Read(p []byte) (int, error) {
	b.reads++
	n, err := b.Reader.Read(p)
	b.bytes += n
	return n, err
}

func (b *cosProbeBody) Close() error { b.closed.Store(true); return nil }

type cosProbeFS struct {
	*fileservice.S3FS
	t      *testing.T
	bodies *[]*cosProbeBody
}

func (fs *cosProbeFS) Read(ctx context.Context, vec *fileservice.IOVector) error {
	for _, body := range *fs.bodies {
		require.True(fs.t, body.closed.Load(), "previous probe must close before the next opens")
	}
	if err := fs.S3FS.Read(ctx, vec); err != nil {
		return err
	}
	require.Len(fs.t, vec.Entries, 1)
	ptr := vec.Entries[0].ReadCloserForRead
	*ptr = &cosProbeCloser{ReadCloser: *ptr, body: (*fs.bodies)[len(*fs.bodies)-1], t: fs.t}
	return nil
}

type cosProbeCloser struct {
	io.ReadCloser
	body *cosProbeBody
	t    *testing.T
}

func (r *cosProbeCloser) Close() error {
	reads, consumed := r.body.reads, r.body.bytes
	// The fixture must actually leave a tail; otherwise a draining Close passes.
	require.Less(r.t, consumed, r.body.size)
	err := r.ReadCloser.Close()
	require.Equal(r.t, reads, r.body.reads, "probe Close must not read the COS response")
	require.Equal(r.t, consumed, r.body.bytes, "probe Close must not download the tail")
	require.True(r.t, r.body.closed.Load())
	return err
}

func TestReadFileOffsetCOSCloseDoesNotReadTail(t *testing.T) {
	for _, strict := range []bool{false, true} {
		for _, term := range []string{"\n", "||"} {
			t.Run(fmt.Sprintf("strict=%v/term=%q", strict, term), func(t *testing.T) {
				// Leave more than the parser's 320 KiB read-ahead after every guess.
				// Unique IDs make independent partition parsing detect lost/repeated rows.
				const rows = 12288
				payload := strings.Repeat("x", 120)
				var content strings.Builder
				for i := 0; i < rows; i++ {
					fmt.Fprintf(&content, "%06d,%s%s", i, payload, term)
				}
				data := content.String()
				var bodies []*cosProbeBody
				fs := fileservice.NewQCloudFileServiceWithTransportForTest(cosProbeTransport(func(req *http.Request) (*http.Response, error) {
					rang := strings.TrimPrefix(req.Header.Get("Range"), "bytes=")
					start, end, ok := strings.Cut(rang, "-")
					require.True(t, ok)
					require.Empty(t, end, "probe retains its unbounded range semantics")
					offset, err := strconv.Atoi(start)
					require.NoError(t, err)
					body := &cosProbeBody{Reader: strings.NewReader(data[offset:]), size: len(data) - offset}
					bodies = append(bodies, body)
					t.Cleanup(func() { _ = body.Close() })
					return &http.Response{StatusCode: http.StatusPartialContent,
						Header:        http.Header{"Content-Length": {strconv.Itoa(body.size)}},
						ContentLength: int64(body.size), Body: body, Request: req}, nil
				}))
				ctx := fileservice.WithSQLFileAccess(t.Context(), "sys", "moadmin")
				t.Cleanup(func() { fs.Close(ctx) })
				param := &tree.ExternParam{
					ExParamConst: tree.ExParamConst{Filepath: "etl:/probe.csv", Format: tree.CSV, ScanType: tree.INFILE,
						CompressType: tree.NOCOMPRESS, Tail: &tree.TailParameter{Lines: tree.NewLines("", term)}},
					ExParam: tree.ExParam{Ctx: ctx, FileService: &cosProbeFS{S3FS: fs, t: t, bodies: &bodies}, Strict: strict},
				}
				offsets, err := external.ReadFileOffset(param, 4, int64(len(data)), nil)
				require.NoError(t, err)
				rowSize := int64(6 + 1 + len(payload) + len(term))
				var want []int64
				start := int64(0)
				for i := 0; i < 3; i++ {
					next := start + int64(len(data))/4
					if strict {
						next += rowSize // strict probing reads the row starting at guess.
					}
					want = append(want, start, next)
					start = next
				}
				want = append(want, start, -1)
				require.Equal(t, want, offsets)
				require.Len(t, bodies, 3)
				id := 0
				for i := 0; i < len(offsets); i += 2 {
					end := offsets[i+1]
					if end < 0 {
						end = int64(len(data))
					}
					partition := strings.ReplaceAll(data[offsets[i]:end], term, "\n")
					parsed, err := csv.NewReader(strings.NewReader(partition)).ReadAll()
					require.NoError(t, err)
					for _, row := range parsed {
						require.Equal(t, []string{fmt.Sprintf("%06d", id), payload}, row)
						id++
					}
				}
				require.Equal(t, rows, id)
			})
		}
	}
}

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

package fileservice

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type minioRangeResponse struct {
	http.ResponseWriter
	bytes int
}

func (w *minioRangeResponse) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func TestMinioSDKReadRanges(t *testing.T) {
	const data = "0123456789abcdef"
	for _, tc := range []struct {
		name, wantRange, identity, consumer string
		min, max                            *int64
		want                                string
		policy                              Policy
	}{
		{name: "bounded", min: ptrTo[int64](1), max: ptrTo[int64](4), wantRange: "bytes=1-3", want: "123"},
		{name: "zero start", min: ptrTo[int64](0), max: ptrTo[int64](4), wantRange: "bytes=0-3", want: "0123"},
		{name: "nil start", max: ptrTo[int64](4), wantRange: "bytes=0-3", want: "0123"},
		{name: "first byte", max: ptrTo[int64](1), wantRange: "bytes=0-0", want: "0"},
		{name: "one byte", min: ptrTo[int64](1), max: ptrTo[int64](2), wantRange: "bytes=1-1", want: "1"},
		{name: "tail", min: ptrTo[int64](13), max: ptrTo[int64](16), wantRange: "bytes=13-15", want: "def"},
		{name: "past end", min: ptrTo[int64](13), max: ptrTo[int64](20), wantRange: "bytes=13-19", want: "def"},
		{name: "unbounded", want: data},
		{name: "unbounded suffix", min: ptrTo[int64](1), wantRange: "bytes=1-", want: data[1:]},
		{name: "etag", identity: "etag", min: ptrTo[int64](1), max: ptrTo[int64](4), wantRange: "bytes=1-3", want: "123"},
		{name: "version", identity: "version", min: ptrTo[int64](1), max: ptrTo[int64](4), wantRange: "bytes=1-3", want: "123"},
		{name: "identity nil start", identity: "etag", max: ptrTo[int64](4), wantRange: "bytes=0-3", want: "0123"},
		{name: "file service buffered", consumer: "buffered", policy: SkipAllCache, min: ptrTo[int64](1), max: ptrTo[int64](4), wantRange: "bytes=1-3", want: "123"},
		{name: "file service stream", consumer: "stream", policy: SkipAllCache, min: ptrTo[int64](1), max: ptrTo[int64](4), wantRange: "bytes=1-3", want: "123"},
		{name: "file service default policy buffered", consumer: "buffered", min: ptrTo[int64](1), max: ptrTo[int64](4), wantRange: "bytes=1-3", want: "123"},
		{name: "file service default policy stream", consumer: "stream", min: ptrTo[int64](1), max: ptrTo[int64](4), wantRange: "bytes=1-3", want: "123"},
		{name: "file service default policy suffix", consumer: "buffered", min: ptrTo[int64](13), wantRange: "bytes=13-", want: "def"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type transfer struct {
				rangeHeader, ifMatch, version string
				bytes                         int
			}
			transfers := make(chan transfer, 1)
			var gets atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"etag"`)
				response := &minioRangeResponse{ResponseWriter: w}
				http.ServeContent(response, r, "object", time.Unix(1, 0), strings.NewReader(data))
				if r.Method == http.MethodGet {
					gets.Add(1)
					select {
					case transfers <- transfer{r.Header.Get("Range"), r.Header.Get("If-Match"), r.URL.Query().Get("versionId"), response.bytes}:
					default:
					}
				}
			}))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(cancel)
			sdk, err := NewMinioSDK(ctx, ObjectStorageArguments{
				Endpoint: server.URL, Region: "us-east-1", Bucket: "bucket",
				NoDefaultCredentials: true, NoBucketValidation: true,
			}, nil)
			require.NoError(t, err)
			var reader io.ReadCloser
			if tc.consumer != "" {
				fs, err := NewS3FS(ctx, ObjectStorageArguments{
					Name: "range", Endpoint: server.URL, IsMinio: true,
					Region: "us-east-1", Bucket: "bucket",
					NoDefaultCredentials: true, NoBucketValidation: true,
				}, DisabledCacheConfig, nil, false, false)
				require.NoError(t, err)
				t.Cleanup(func() { fs.Close(context.Background()) })
				size := int64(-1)
				if tc.max != nil {
					size = *tc.max - *tc.min
				}
				vector := &IOVector{FilePath: "object", Policy: tc.policy, Entries: []IOEntry{{Offset: *tc.min, Size: size}}}
				t.Cleanup(vector.Release)
				if tc.consumer == "stream" {
					vector.Entries[0].ReadCloserForRead = &reader
				}
				require.NoError(t, fs.Read(ctx, vector))
				if tc.consumer == "buffered" {
					reader = io.NopCloser(strings.NewReader(string(vector.Entries[0].Data)))
				}
			} else if tc.identity != "" {
				identity := ObjectIdentity{ETag: "etag", Size: int64(len(data))}
				if tc.identity == "version" {
					identity.VersionID = "version"
				}
				reader, err = sdk.ReadObjectWithIdentity(ctx, "object", tc.min, tc.max, identity)
				require.NoError(t, err)
			} else {
				reader, err = sdk.Read(ctx, "object", tc.min, tc.max)
				require.NoError(t, err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Equal(t, tc.want, string(got))
			select {
			case sent := <-transfers:
				assert.Equal(t, tc.wantRange, sent.rangeHeader)
				assert.Equal(t, len(tc.want), sent.bytes)
				require.Equal(t, int32(1), gets.Load())
				if tc.identity == "etag" {
					require.Equal(t, `"etag"`, sent.ifMatch)
				}
				if tc.identity == "version" {
					require.Equal(t, "version", sent.version)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

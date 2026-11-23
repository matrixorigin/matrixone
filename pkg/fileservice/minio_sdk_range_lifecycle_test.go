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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

// Object.Close releases its response from the SDK worker, so counters and the
// completion signal must also be safe to observe from the test goroutine.
type minioRangeBody struct {
	io.Reader
	reads, bytes, closes atomic.Int64
	closed               chan struct{}
	once                 sync.Once
}

func (b *minioRangeBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	n, err := b.Reader.Read(p)
	b.bytes.Add(int64(n))
	return n, err
}

func (b *minioRangeBody) Close() error {
	b.closes.Add(1)
	b.once.Do(func() { close(b.closed) })
	return nil
}

func (b *minioRangeBody) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-b.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("MinIO response was not closed")
	}
	require.Equal(t, int64(1), b.closes.Load())
}

func newMinioRangeTestSDK(t *testing.T, transport http.RoundTripper) *MinioSDK {
	t.Helper()
	client, err := minio.New("minio.invalid", &minio.Options{
		Creds: credentials.NewStaticV4("", "", ""), Region: "us-east-1", Transport: transport,
	})
	require.NoError(t, err)
	return &MinioSDK{bucket: "bucket", client: client}
}

func minioRangeTestResponse(req *http.Request, body io.ReadCloser, size int) *http.Response {
	return &http.Response{
		StatusCode: http.StatusPartialContent, Request: req, Body: body, ContentLength: int64(size),
		Header: http.Header{
			"Content-Length": {strconv.Itoa(size)},
			"Last-Modified":  {time.Unix(1, 0).UTC().Format(http.TimeFormat)},
			"Etag":           {`"etag"`},
		},
	}
}

func openMinioRangeTestReader(t *testing.T, sdk *MinioSDK, identity string) io.ReadCloser {
	t.Helper()
	min, max := int64(1), int64(6)
	var reader io.ReadCloser
	var err error
	if identity == "" {
		reader, err = sdk.Read(t.Context(), "object", &min, &max)
	} else {
		expected := ObjectIdentity{ETag: "etag", Size: 16}
		if identity == "version" {
			expected.VersionID = "version"
		}
		reader, err = sdk.ReadObjectWithIdentity(t.Context(), "object", &min, &max, expected)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

func TestMinioSDKBoundedCloseDoesNotRead(t *testing.T) {
	for _, tc := range []struct {
		name, identity string
		take           int
	}{
		{name: "unread"},
		{name: "partial", take: 2},
		{name: "full", take: 5},
		{name: "etag", identity: "etag", take: 2},
		{name: "version", identity: "version", take: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &minioRangeBody{Reader: strings.NewReader("12345"), closed: make(chan struct{})}
			sdk := newMinioRangeTestSDK(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return minioRangeTestResponse(req, body, 5), nil
			}))
			reader := openMinioRangeTestReader(t, sdk, tc.identity)
			prefix := make([]byte, tc.take)
			_, err := io.ReadFull(reader, prefix)
			require.NoError(t, err)
			require.Equal(t, "12345"[:tc.take], string(prefix))
			reads := body.reads.Load()
			require.NoError(t, reader.Close())
			body.waitClosed(t)
			require.Equal(t, reads, body.reads.Load(), "Close must not read the response")
			require.Equal(t, int64(tc.take), body.bytes.Load())
		})
	}
}

func TestMinioSDKBoundedReadRetry(t *testing.T) {
	for _, tc := range []struct {
		name, identity string
		prefix         bool
		failure        int
	}{
		{name: "delivered prefix", prefix: true},
		{name: "bytes and error in one read"},
		{name: "etag", identity: "etag", prefix: true},
		{name: "version", identity: "version", prefix: true},
		{name: "etag changed", identity: "etag", prefix: true, failure: http.StatusPreconditionFailed},
		{name: "version gone", identity: "version", prefix: true, failure: http.StatusNotFound},
		{name: "reopen denied", prefix: true, failure: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := &minioRangeBody{
				Reader: io.MultiReader(strings.NewReader("12"), iotest.ErrReader(io.ErrUnexpectedEOF)),
				closed: make(chan struct{}),
			}
			tail, wantRange := "12345", "bytes=1-5"
			if tc.prefix {
				tail, wantRange = "345", "bytes=3-5"
			}
			if tc.failure != 0 {
				code := map[int]string{http.StatusPreconditionFailed: "PreconditionFailed", http.StatusNotFound: "NoSuchVersion", http.StatusForbidden: "AccessDenied"}[tc.failure]
				tail = fmt.Sprintf("<Error><Code>%s</Code><Message>reopen failed</Message></Error>", code)
			}
			second := &minioRangeBody{Reader: strings.NewReader(tail), closed: make(chan struct{})}
			requests := make(chan *http.Request, 2)
			var gets atomic.Int32
			sdk := newMinioRangeTestSDK(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				select {
				case requests <- req.Clone(context.Background()):
				default:
				}
				if gets.Add(1) == 1 {
					return minioRangeTestResponse(req, first, 5), nil
				}
				response := minioRangeTestResponse(req, second, len(tail))
				if tc.failure != 0 {
					response.StatusCode = tc.failure
					response.Header.Set("Content-Type", "application/xml")
				}
				return response, nil
			}))
			reader := openMinioRangeTestReader(t, sdk, tc.identity)
			var prefix []byte
			if tc.prefix {
				prefix = make([]byte, 2)
				_, err := io.ReadFull(reader, prefix)
				require.NoError(t, err)
				require.Equal(t, "12", string(prefix))
			}
			got, err := io.ReadAll(reader)
			if tc.failure != 0 {
				require.Error(t, err)
				if tc.identity != "" {
					require.ErrorIs(t, err, ErrObjectChanged)
				} else {
					require.Equal(t, tc.failure, minio.ToErrorResponse(err).StatusCode)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, "12345", string(prefix)+string(got))
			}
			require.NoError(t, reader.Close())
			first.waitClosed(t)
			second.waitClosed(t)
			require.Equal(t, int64(2), first.bytes.Load())
			require.Equal(t, int64(len(tail)), second.bytes.Load())
			require.Equal(t, int32(2), gets.Load())
			for _, rang := range []string{"bytes=1-5", wantRange} {
				req := <-requests
				require.Equal(t, rang, req.Header.Get("Range"))
				if tc.identity == "etag" {
					require.Equal(t, `"etag"`, req.Header.Get("If-Match"))
				}
				if tc.identity == "version" {
					require.Equal(t, "version", req.URL.Query().Get("versionId"))
				}
			}
		})
	}
}

func TestMinioSDKBoundedReadRelease(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%v", canceled), func(t *testing.T) {
			ended, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "5")
				w.Header().Set("Content-Range", "bytes 1-5/16")
				w.Header().Set("ETag", `"etag"`)
				w.Header().Set("Last-Modified", time.Unix(1, 0).UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, "12")
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					close(ended)
				case <-release:
				}
			}))
			t.Cleanup(server.Close)
			t.Cleanup(func() { close(release) })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			t.Cleanup(cancel)
			sdk, err := NewMinioSDK(ctx, ObjectStorageArguments{
				Endpoint: server.URL, Region: "us-east-1", Bucket: "bucket",
				NoDefaultCredentials: true, NoBucketValidation: true,
			}, nil)
			require.NoError(t, err)
			min, max := int64(1), int64(6)
			reader, err := sdk.Read(ctx, "object", &min, &max)
			require.NoError(t, err)
			t.Cleanup(func() { cancel(); _ = reader.Close() })
			prefix := make([]byte, 2)
			_, err = io.ReadFull(reader, prefix)
			require.NoError(t, err)
			require.Equal(t, "12", string(prefix))
			if canceled {
				cancel()
				_, err = reader.Read(make([]byte, 1))
				require.ErrorIs(t, err, context.Canceled)
			}
			require.NoError(t, reader.Close())
			if !canceled {
				require.NoError(t, ctx.Err(), "Close must not wait for the unfinished response or deadline")
			}
			select {
			case <-ended:
			case <-time.After(5 * time.Second):
				t.Fatal("server did not observe response release")
			}
			if !canceled {
				require.NoError(t, ctx.Err())
			}
		})
	}
}

func TestMinioSDKInvalidReadRange(t *testing.T) {
	var calls atomic.Int32
	sdk := newMinioRangeTestSDK(t, roundTripperFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, fmt.Errorf("invalid ranges must not reach HTTP")
	}))
	for _, tc := range []struct {
		name     string
		min, max int64
	}{
		{name: "empty at zero"},
		{name: "empty", min: 2, max: 2},
		{name: "reversed", min: 2, max: 1},
		{name: "negative start", min: -1, max: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := sdk.Read(t.Context(), "object", &tc.min, &tc.max)
			require.Error(t, err)
			require.Nil(t, reader)
			if tc.min >= 0 {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange))
			}
		})
	}
	require.Zero(t, calls.Load())
}

func TestMinioSDKReadOpenFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		min, max *int64
		canceled bool
	}{
		{name: "bounded missing", max: ptrTo[int64](1)},
		{name: "unbounded missing"},
		{name: "seek missing", min: ptrTo[int64](1)},
		{name: "canceled before open", max: ptrTo[int64](1), canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const message = "<Error><Code>NoSuchKey</Code><Message>missing object</Message></Error>"
			body := &minioRangeBody{Reader: strings.NewReader(message), closed: make(chan struct{})}
			var calls atomic.Int32
			sdk := newMinioRangeTestSDK(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				response := minioRangeTestResponse(req, body, len(message))
				response.StatusCode = http.StatusNotFound
				response.Header.Set("Content-Type", "application/xml")
				return response, nil
			}))
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			if tc.canceled {
				cancel()
			}
			reader, err := sdk.Read(ctx, "object", tc.min, tc.max)
			if reader != nil {
				t.Cleanup(func() { _ = reader.Close() })
			}
			require.Error(t, err)
			require.Nil(t, reader)
			if tc.canceled {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, calls.Load())
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
				require.Equal(t, int32(1), calls.Load())
				body.waitClosed(t)
			}
		})
	}
}

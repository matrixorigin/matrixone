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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
	cos "github.com/tencentyun/cos-go-sdk-v5"
)

// NewQCloudSDKWithTransportForTest is test-only so an external test package can
// exercise a real LOAD consumer without a fileservice -> external import cycle.
func NewQCloudSDKWithTransportForTest(transport http.RoundTripper) *QCloudSDK {
	client := cos.NewClient(&cos.BaseURL{BucketURL: &url.URL{Scheme: "https", Host: "cos.invalid"}},
		&http.Client{Transport: transport})
	client.Conf.EnableCRC = false
	client.Conf.RetryOpt.Count = 0
	return &QCloudSDK{name: "cos-close-test", client: client}
}

// NewQCloudFileServiceWithTransportForTest supplies the real cache-free S3FS
// stream wrappers as well as the COS adapter to the external consumer test.
func NewQCloudFileServiceWithTransportForTest(transport http.RoundTripper) *S3FS {
	return &S3FS{name: "etl", storage: NewQCloudSDKWithTransportForTest(transport), ioMerger: NewIOMerger()}
}

type qcloudCloseBody struct {
	io.Reader
	reads, bytes, closes int
	closeErr             error
}

func (b *qcloudCloseBody) Read(p []byte) (int, error) {
	b.reads++
	n, err := b.Reader.Read(p)
	b.bytes += n
	return n, err
}

func (b *qcloudCloseBody) Close() error {
	b.closes++
	return b.closeErr
}

func newQCloudCloseBody(t *testing.T, reader io.Reader) *qcloudCloseBody {
	t.Helper()
	body := &qcloudCloseBody{Reader: reader}
	t.Cleanup(func() {
		if body.closes == 0 {
			_ = body.Close()
		}
	})
	return body
}

func qcloudCloseResponse(req *http.Request, body io.ReadCloser, size int) *http.Response {
	return &http.Response{
		StatusCode: http.StatusPartialContent,
		Header: http.Header{
			"Content-Length": {strconv.Itoa(size)},
		},
		ContentLength: int64(size), Body: body, Request: req,
	}
}

func TestQCloudSDKCloseDoesNotReadResponse(t *testing.T) {
	closeErr := errors.New("response close failed")
	for _, tc := range []struct {
		name, identity string
		max, take      int
		empty          bool
		closeErr       error
	}{
		{name: "unbounded unread"},
		{name: "unbounded partial", take: 2},
		{name: "bounded partial", max: 8, take: 2},
		{name: "unbounded full", take: -1},
		{name: "bounded full", max: 8, take: -1},
		{name: "empty", empty: true, take: -1},
		{name: "close error", take: 2, closeErr: closeErr},
		{name: "etag partial", identity: "etag", take: 2},
		{name: "version partial", identity: "version", max: 8, take: 2},
		{name: "conditional close error", identity: "etag", take: 2, closeErr: closeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := "0123456789abcdef"
			min := int64(1)
			if tc.empty {
				data, min = "", 0
			}
			payload := data[min:]
			var max *int64
			wantRange := fmt.Sprintf("bytes=%d-", min)
			if tc.max > 0 {
				max = ptrTo(int64(tc.max))
				payload = data[min:tc.max]
				wantRange += strconv.Itoa(tc.max - 1)
			}
			body := newQCloudCloseBody(t, strings.NewReader(payload))
			body.closeErr = tc.closeErr
			sdk := NewQCloudSDKWithTransportForTest(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, wantRange, req.Header.Get("Range"))
				if tc.identity == "etag" {
					require.Equal(t, `"etag"`, req.Header.Get("If-Match"))
				} else {
					require.Empty(t, req.Header.Get("If-Match"))
				}
				if tc.identity == "version" {
					require.Equal(t, "version", req.URL.Query().Get("versionId"))
				} else {
					require.Empty(t, req.URL.Query().Get("versionId"))
				}
				return qcloudCloseResponse(req, body, len(payload)), nil
			}))
			var reader io.ReadCloser
			var err error
			if tc.identity == "" {
				reader, err = sdk.Read(t.Context(), "object", &min, max)
			} else {
				identity := ObjectIdentity{ETag: `"etag"`, Size: int64(len(data))}
				if tc.identity == "version" {
					identity.VersionID = "version"
				}
				reader, err = sdk.ReadObjectWithIdentity(t.Context(), "object", &min, max, identity)
			}
			require.NoError(t, err)
			t.Cleanup(func() {
				if body.closes == 0 {
					_ = reader.Close()
				}
			})
			if tc.take < 0 {
				got, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, payload, string(got))
			} else {
				got := make([]byte, tc.take)
				_, err := io.ReadFull(reader, got)
				require.NoError(t, err)
				require.Equal(t, payload[:tc.take], string(got))
			}
			reads, consumed := body.reads, body.bytes
			if tc.closeErr == nil {
				require.NoError(t, reader.Close())
			} else {
				require.ErrorIs(t, reader.Close(), tc.closeErr)
			}
			require.Equal(t, reads, body.reads, "Close must not call response Read, even at EOF")
			require.Equal(t, consumed, body.bytes)
			require.Equal(t, 1, body.closes)
		})
	}
}

func TestQCloudSDKReadErrorClose(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%v", canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			readErr := errors.New("permanent response read error")
			if canceled {
				readErr = context.Canceled
			}
			var source io.Reader = iotest.ErrReader(readErr)
			if canceled {
				source = qcloudCloseContextReader{ctx: ctx, Reader: strings.NewReader("unread tail")}
			}
			body := newQCloudCloseBody(t, source)
			sdk := NewQCloudSDKWithTransportForTest(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return qcloudCloseResponse(req, body, 16), nil
			}))
			reader, err := sdk.Read(ctx, "object", nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				if body.closes == 0 {
					_ = reader.Close()
				}
			})
			if canceled {
				cancel() // The body has already been opened successfully.
			}
			_, err = reader.Read(make([]byte, 1))
			require.ErrorIs(t, err, readErr)
			require.Equal(t, 1, body.reads)
			require.NoError(t, reader.Close())
			require.Equal(t, 1, body.reads, "error cleanup must not read again")
			require.Equal(t, 1, body.closes)
		})
	}
}

type qcloudCloseContextReader struct {
	ctx context.Context
	io.Reader
}

func (r qcloudCloseContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

// Return a retryable error once, then allow more bytes to expose an accidental
// drain of the abandoned response. Bytes before the error are delivered in a
// separate successful Read, matching retryableReader's existing contract.
type qcloudCloseRetryError struct{ failed bool }

func (r *qcloudCloseRetryError) Read([]byte) (int, error) {
	if !r.failed {
		r.failed = true
		return 0, io.ErrUnexpectedEOF
	}
	return 0, io.EOF
}

func TestQCloudSDKRetryCloseDoesNotReadResponse(t *testing.T) {
	for _, reopenFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopenFails=%v", reopenFails), func(t *testing.T) {
			first := newQCloudCloseBody(t, io.MultiReader(strings.NewReader("bc"), &qcloudCloseRetryError{}, strings.NewReader("defgh")))
			second := newQCloudCloseBody(t, strings.NewReader("defgh"))
			var ranges []string
			sdk := NewQCloudSDKWithTransportForTest(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				ranges = append(ranges, req.Header.Get("Range"))
				if len(ranges) == 1 {
					return qcloudCloseResponse(req, first, 7), nil
				}
				if reopenFails {
					return nil, errors.New("permanent reopen failure")
				}
				return qcloudCloseResponse(req, second, 5), nil
			}))
			min := int64(1)
			reader, err := sdk.Read(t.Context(), "object", &min, nil)
			require.NoError(t, err)
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = reader.Close()
				}
			})
			prefix := make([]byte, 2)
			_, err = io.ReadFull(reader, prefix)
			require.NoError(t, err)
			tail := make([]byte, 3)
			_, err = io.ReadFull(reader, tail)
			if reopenFails {
				require.ErrorContains(t, err, "permanent reopen failure")
				require.Zero(t, first.closes, "failed reopen leaves current reader owned by caller")
			} else {
				require.NoError(t, err)
				require.Equal(t, "bcdef", string(prefix)+string(tail))
				require.Equal(t, 1, first.closes)
				require.Zero(t, second.closes)
			}
			require.Equal(t, []string{"bytes=1-", "bytes=3-"}, ranges)
			require.NoError(t, reader.Close())
			closed = true
			require.Equal(t, 2, first.reads)
			require.Equal(t, 2, first.bytes)
			require.Equal(t, 1, first.closes)
			if !reopenFails {
				require.Equal(t, 1, second.reads)
				require.Equal(t, 3, second.bytes)
				require.Equal(t, 1, second.closes)
			}
		})
	}
}

func TestQCloudSDKCloseReleasesUnfinishedHTTPResponse(t *testing.T) {
	const prefix = "prefix"
	prefixSent, requestEnded, releaseTail := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Leave a known large response unfinished. Close must not need its EOF.
		w.Header().Set("Content-Length", strconv.Itoa(1<<20))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, prefix)
		w.(http.Flusher).Flush()
		close(prefixSent)
		select {
		case <-r.Context().Done():
			close(requestEnded)
		case <-releaseTail: // Only failure cleanup can release the handler.
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(releaseTail) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second) // Hang guard, not a timing oracle.
	t.Cleanup(cancel)
	sdk := newTestCOSClient(t, server)
	reader, err := sdk.Read(ctx, "object", nil, nil)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		cancel()
		if !closed {
			_ = reader.Close()
		}
	})
	got := make([]byte, len(prefix))
	_, err = io.ReadFull(reader, got)
	require.NoError(t, err)
	require.Equal(t, prefix, string(got))
	select {
	case <-prefixSent:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	err = reader.Close()
	closed = true
	require.NoError(t, err)
	require.NoError(t, ctx.Err(), "Close must complete without the deadline canceling the request")
	select {
	case <-requestEnded:
	case <-ctx.Done():
		t.Fatal("server did not observe release of the unfinished response")
	}
	require.NoError(t, ctx.Err(), "server release must not depend on deadline cancellation")
}

// Copyright 2024 Matrix Origin
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

package fileservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"github.com/tencentyun/cos-go-sdk-v5"
)

func TestQCloudSDK(t *testing.T) {

	t.Run("object storage", func(t *testing.T) {
		for _, args := range objectStorageArgumentsForTest("test", t) {
			if !strings.Contains(args.Endpoint, "myqcloud") {
				continue
			}

			t.Run(fmt.Sprintf("%s %s", args.Name, args.Bucket), func(t *testing.T) {

				testObjectStorageWithContext(t, "qcloud", func(t *testing.T, ctx context.Context) *QCloudSDK {
					args.KeyPrefix = fmt.Sprintf("%v", rand.Int64())
					ret, err := NewQCloudSDK(
						ctx,
						args,
						nil,
					)
					if err != nil {
						t.Fatal(err)
					}
					return ret
				})

			})
		}
	})

	t.Run("file service", func(t *testing.T) {
		for _, args := range objectStorageArgumentsForTest("test", t) {
			if !strings.Contains(args.Endpoint, "myqcloud") {
				continue
			}

			t.Run(fmt.Sprintf("%s %s", args.Name, args.Bucket), func(t *testing.T) {

				t.Run("file service", func(t *testing.T) {
					testFileServiceWithContext(t, 0, func(ctx context.Context, name string) FileService {
						args.Name = name
						args.KeyPrefix = fmt.Sprintf("%v", rand.Int64())
						ret, err := NewS3FS(
							ctx,
							args,
							DisabledCacheConfig,
							nil,
							true,
							true,
						)
						if err != nil {
							t.Fatal(err)
						}
						return ret
					})
				})

			})
		}
	})

}

func TestNewQCloudSDKNoBucketValidation(t *testing.T) {
	sdk, err := NewQCloudSDK(context.Background(), ObjectStorageArguments{
		Name:               "qcloud-new",
		Bucket:             "bucket",
		Region:             "ap-guangzhou",
		KeyID:              "id",
		KeySecret:          "secret",
		SessionToken:       "token",
		NoBucketValidation: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sdk.name != "qcloud-new" || sdk.client == nil {
		t.Fatalf("unexpected sdk: %#v", sdk)
	}
	if sdk.client.Conf.RetryOpt.Count != 0 {
		t.Fatalf("expected SDK retry disabled, got %d", sdk.client.Conf.RetryOpt.Count)
	}

	t.Setenv("AWS_ACCESS_KEY_ID", "env-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")
	sdk, err = NewQCloudSDK(context.Background(), ObjectStorageArguments{
		Bucket:             "bucket",
		Region:             "ap-guangzhou",
		NoBucketValidation: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sdk.client == nil {
		t.Fatalf("expected client")
	}
}

func TestQCloudSDKCopyObject(t *testing.T) {
	var copySource string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		copySource = r.Header.Get("x-cos-copy-source")
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<CopyObjectResult><ETag>"etag"</ETag><LastModified>2026-01-01T00:00:00Z</LastModified></CopyObjectResult>`)
	}))
	defer server.Close()

	domain := newObjectStorageCopyCredentialDomain("id", "secret")
	src := newTestCOSClient(t, server)
	src.copySourceHost = "source.example.com"
	src.copyCredentialDomain = domain
	dst := newTestCOSClient(t, server)
	dst.copyCredentialDomain = domain

	copied, err := dst.CopyObject(context.Background(), src, "source/key", "destination/key")
	require.NoError(t, err)
	require.True(t, copied)
	require.Equal(t, "source.example.com/source/key", copySource)

	copied, err = dst.CopyObject(context.Background(), dummyObjectStorage{}, "source", "destination")
	require.NoError(t, err)
	require.False(t, copied)
}

func TestQCloudSDKConditionalObjectIdentityReads(t *testing.T) {
	const lastModRaw = "Wed, 02 Sep 2026 03:04:05 GMT"
	var requests []*http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Clone(context.Background()))
		if r.URL.Query().Get("versionId") == "stale" || r.Header.Get("If-Match") == `"stale"` {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>object changed</Message></Error>`)
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "6")
			w.Header().Set("ETag", `"etag-v1"`)
			w.Header().Set("x-cos-version-id", "version-v1")
			w.Header().Set("Last-Modified", lastModRaw)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Length", "3")
		w.Header().Set("Content-Range", "bytes 1-3/6")
		w.Header().Set("ETag", `"etag-v1"`)
		w.Header().Set("Last-Modified", lastModRaw)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "bcd")
	}))
	defer server.Close()

	sdk := newTestCOSClient(t, server)
	identity, err := sdk.StatObjectIdentity(context.Background(), "object")
	require.NoError(t, err)
	wantLastModified, err := time.Parse(http.TimeFormat, lastModRaw)
	require.NoError(t, err)
	require.Equal(t, ObjectIdentity{
		VersionID: "version-v1", ETag: `"etag-v1"`, Size: 6, LastModified: wantLastModified,
	}, identity)

	min, max := int64(1), int64(4)
	reader, err := sdk.ReadObjectWithIdentity(context.Background(), "object", &min, &max, identity)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, "bcd", string(data))
	versionRequest := requests[len(requests)-1]
	require.Equal(t, "version-v1", versionRequest.URL.Query().Get("versionId"))
	require.Empty(t, versionRequest.Header.Get("If-Match"))
	require.Equal(t, "bytes=1-3", versionRequest.Header.Get("Range"))

	etagIdentity := identity
	etagIdentity.VersionID = ""
	reader, err = sdk.ReadObjectWithIdentity(context.Background(), "object", &min, &max, etagIdentity)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	etagRequest := requests[len(requests)-1]
	require.Empty(t, etagRequest.URL.Query().Get("versionId"))
	require.Equal(t, `"etag-v1"`, etagRequest.Header.Get("If-Match"))

	stale := identity
	stale.VersionID = "stale"
	_, err = sdk.ReadObjectWithIdentity(context.Background(), "object", &min, &max, stale)
	require.ErrorIs(t, err, ErrObjectChanged)
	stale.VersionID = ""
	stale.ETag = `"stale"`
	_, err = sdk.ReadObjectWithIdentity(context.Background(), "object", &min, &max, stale)
	require.ErrorIs(t, err, ErrObjectChanged)
}

func TestQCloudSDKWriteRetriesSeekablePut(t *testing.T) {
	data := bytes.Repeat([]byte("x"), int(smallObjectThreshold))
	size := int64(len(data))
	transport := &qcloudRetryPutTransport{}
	reader := &trackingSeekReader{Reader: bytes.NewReader(data)}

	baseURL, err := url.Parse("http://cos.local")
	if err != nil {
		t.Fatal(err)
	}
	client := cos.NewClient(
		&cos.BaseURL{BucketURL: baseURL},
		&http.Client{Transport: transport},
	)
	client.Conf.EnableCRC = false
	client.Conf.RetryOpt.Count = 0

	sdk := &QCloudSDK{
		name:   "qcloud-retry-test",
		client: client,
	}
	if err := sdk.Write(context.Background(), "object", reader, &size, nil); err != nil {
		t.Fatal(err)
	}

	if transport.calls != 2 {
		t.Fatalf("expected 2 put attempts, got %d", transport.calls)
	}
	for i, body := range transport.bodies {
		if !bytes.Equal(body, data) {
			t.Fatalf("attempt %d body mismatch: got %d bytes, want %d", i+1, len(body), len(data))
		}
	}
	if reader.seekStartCount == 0 {
		t.Fatalf("expected QCloudSDK.Write to seek back before retry")
	}
}

func TestQCloudCOSHTTPStatusRetryable(t *testing.T) {
	for _, status := range []int{
		http.StatusInternalServerError,
		http.StatusNotImplemented,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		http.StatusTooManyRequests,
	} {
		err := newTestCOSError(status)
		if !IsRetryableError(err) {
			t.Fatalf("expected COS status %d to be retryable", status)
		}
	}

	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusConflict,
	} {
		err := newTestCOSError(status)
		if IsRetryableError(err) {
			t.Fatalf("expected COS status %d to be non-retryable", status)
		}
	}
}

func TestQCloudCOSRetryErrorRetryable(t *testing.T) {
	err := &cos.RetryError{
		Errs: []error{
			newTestCOSError(http.StatusInternalServerError),
		},
	}
	if !IsRetryableError(err) {
		t.Fatalf("expected COS RetryError to be retryable")
	}
}

func TestQCloudCOSRetryErrorNonRetryable(t *testing.T) {
	err := &cos.RetryError{
		Errs: []error{
			newTestCOSError(http.StatusBadRequest),
		},
	}
	if IsRetryableError(err) {
		t.Fatalf("expected COS RetryError with non-retryable inner error to be non-retryable")
	}
}

func TestWrappedNetTimeoutRetryable(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", timeoutError{})
	if !IsRetryableError(err) {
		t.Fatalf("expected wrapped net timeout to be retryable")
	}
}

func TestServerClosedIdleConnectionRetryable(t *testing.T) {
	err := fmt.Errorf(`Put "https://bucket.cos.ap-guangzhou.myqcloud.com/object?partNumber=716&uploadId=id": http: server closed idle connection`)
	if !IsRetryableError(err) {
		t.Fatalf("expected server closed idle connection to be retryable")
	}
}

func TestQCloudSDKBasicObjectOperations(t *testing.T) {
	const body = "hello object"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		switch {
		case r.Method == http.MethodGet && key == "" && !strings.Contains(r.URL.RawQuery, "marker=page2"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<ListBucketResult><Name>bucket</Name><Prefix>dir/</Prefix><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>dir/file1</Key><Size>11</Size></Contents><CommonPrefixes><Prefix>dir/sub/</Prefix></CommonPrefixes><NextMarker>page2</NextMarker></ListBucketResult>`)
		case r.Method == http.MethodGet && key == "" && strings.Contains(r.URL.RawQuery, "marker=page2"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<ListBucketResult><Name>bucket</Name><Prefix>dir/</Prefix><Marker>page2</Marker><IsTruncated>false</IsTruncated><Contents><Key>dir/file2</Key><Size>7</Size></Contents></ListBucketResult>`)
		case r.Method == http.MethodHead && key == "missing":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>`)
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && key == "missing":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>`)
		case r.Method == http.MethodGet:
			data := []byte(body)
			if rangeHeader := r.Header.Get("Range"); rangeHeader != "" && rangeHeader != "bytes=0-" {
				data = data[1:5]
			}
			_, _ = w.Write(data)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "delete"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.ReadAll(r.Body)
			_, _ = io.WriteString(w, `<DeleteResult></DeleteResult>`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	sdk := newTestCOSClient(t, server)
	sdk.listMaxKeys = 2

	entries := make([]DirEntry, 0, 3)
	for entry, err := range sdk.List(context.Background(), "dir/") {
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		entries = append(entries, *entry)
	}
	if fmt.Sprint(entries) != fmt.Sprint([]DirEntry{
		{Name: "dir/file1", Size: 11},
		{IsDir: true, Name: "dir/sub/"},
		{Name: "dir/file2", Size: 7},
	}) {
		t.Fatalf("unexpected entries: %#v", entries)
	}

	size, err := sdk.Stat(context.Background(), "dir/file1")
	if err != nil || size != 12 {
		t.Fatalf("stat got size %d err %v", size, err)
	}
	_, err = sdk.Stat(context.Background(), "missing")
	if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		t.Fatalf("expected file not found, got %v", err)
	}

	exists, err := sdk.Exists(context.Background(), "dir/file1")
	if err != nil || !exists {
		t.Fatalf("exists got %v, %v", exists, err)
	}
	exists, err = sdk.Exists(context.Background(), "missing")
	if err != nil || exists {
		t.Fatalf("missing exists got %v, %v", exists, err)
	}

	reader, err := sdk.Read(context.Background(), "dir/file1", nil, nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if string(data) != body {
		t.Fatalf("body = %q", data)
	}

	min, max := int64(1), int64(5)
	reader, err = sdk.Read(context.Background(), "dir/file1", &min, &max)
	if err != nil {
		t.Fatalf("range read: %v", err)
	}
	data, err = io.ReadAll(reader)
	if err != nil {
		t.Fatalf("range read body: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("range close: %v", err)
	}
	if string(data) != "ello" {
		t.Fatalf("range body = %q", data)
	}

	reader, err = sdk.Read(context.Background(), "missing", nil, nil)
	if reader != nil || !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		t.Fatalf("expected nil reader and file not found, got %v, %v", reader, err)
	}

	if !sdk.SupportsParallelMultipart() {
		t.Fatalf("expected parallel multipart support")
	}
	if err := sdk.Delete(context.Background()); err != nil {
		t.Fatalf("empty delete: %v", err)
	}
	if err := sdk.Delete(context.Background(), "dir/file1"); err != nil {
		t.Fatalf("single delete: %v", err)
	}
	if err := sdk.Delete(context.Background(), "dir/file1", "dir/file2"); err != nil {
		t.Fatalf("multi delete: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	seen := false
	for entry, err := range sdk.List(ctx, "dir/") {
		if entry != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled list got %v, %v", entry, err)
		}
		seen = true
	}
	if !seen {
		t.Fatalf("expected canceled list result")
	}
	if err := sdk.Delete(ctx, "dir/file1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled delete, got %v", err)
	}
}

func TestQCloudSDKReadOnlyOperationsPreserveInFlightCancellation(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *QCloudSDK) error
	}{
		{
			name: "bucket head",
			run: func(ctx context.Context, sdk *QCloudSDK) error {
				_, err := doQCloudReadWithRetry(ctx, "cos bucket head", func() (*cos.Response, error) {
					return sdk.client.Bucket.Head(ctx, &cos.BucketHeadOptions{})
				})
				return err
			},
		},
		{
			name: "list",
			run: func(ctx context.Context, sdk *QCloudSDK) error {
				_, err := sdk.listObjects(ctx, "", "")
				return err
			},
		},
		{
			name: "stat",
			run: func(ctx context.Context, sdk *QCloudSDK) error {
				_, err := sdk.statObject(ctx, "object")
				return err
			},
		},
		{
			name: "read",
			run: func(ctx context.Context, sdk *QCloudSDK) error {
				reader, err := sdk.Read(ctx, "object", nil, nil)
				if reader != nil {
					_ = reader.Close()
				}
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestStarted := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				select {
				case requestStarted <- struct{}{}:
				default:
				}
				<-request.Context().Done()
			}))
			t.Cleanup(server.Close)

			sdk := newTestCOSClient(t, server)
			// Match production: MatrixOne owns retries, while the COS SDK still wraps
			// the one attempted request in cos.RetryError.
			sdk.client.Conf.RetryOpt.Count = 0

			ctx, cancel := context.WithCancel(context.Background())
			cancelDone := make(chan struct{})
			go func() {
				defer close(cancelDone)
				select {
				case <-requestStarted:
					cancel()
				case <-ctx.Done():
				}
			}()
			t.Cleanup(func() {
				cancel()
				<-cancelDone
			})

			err := test.run(ctx, sdk)
			require.ErrorIs(t, err, context.Canceled)
			var retryErr *cos.RetryError
			require.False(t, errors.As(err, &retryErr),
				"QCloudSDK must not leak the opaque COS cancellation wrapper")
		})
	}
}

func TestNormalizeQCloudContextErrorIsConservative(t *testing.T) {
	t.Run("active context", func(t *testing.T) {
		retryErr := &cos.RetryError{Errs: []error{context.Canceled}}
		require.Same(t, retryErr, normalizeQCloudContextError(context.Background(), retryErr))
	})

	t.Run("unrelated error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		retryErr := &cos.RetryError{Errs: []error{errors.New("read failed")}}
		require.Same(t, retryErr, normalizeQCloudContextError(ctx, retryErr))
	})

	t.Run("mixed errors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		retryErr := &cos.RetryError{Errs: []error{context.Canceled, errors.New("read failed")}}
		require.Same(t, retryErr, normalizeQCloudContextError(ctx, retryErr))
	})

	t.Run("matching deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		retryErr := &cos.RetryError{Errs: []error{fmt.Errorf("request failed: %w", context.DeadlineExceeded)}}
		require.ErrorIs(t, normalizeQCloudContextError(ctx, retryErr), context.DeadlineExceeded)
	})
}

func TestQCloudSDKListStopsRetryingAtContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	sdk := newTestCOSClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	for _, err := range sdk.List(ctx, "") {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context deadline exceeded, got %v", err)
		}
		return
	}
	t.Fatal("expected list to return a deadline error")
}

func TestQCloudSDKAbortMultipartStopsAtContextDeadline(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	sdk := newTestCOSClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := sdk.abortMultipartUpload(ctx, "object", "upload-id")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
	require.Positive(t, requests.Load())
}

type timeoutError struct{}

func (timeoutError) Error() string {
	return "timeout"
}

func (timeoutError) Timeout() bool {
	return true
}

func (timeoutError) Temporary() bool {
	return true
}

func newTestCOSError(status int) error {
	req := &http.Request{
		Method: http.MethodPut,
		URL: &url.URL{
			Scheme: "http",
			Host:   "cos.local",
			Path:   "/object",
		},
	}
	return &cos.ErrorResponse{
		Response: &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Request:    req,
		},
	}
}

type qcloudRetryPutTransport struct {
	calls  int
	bodies [][]byte
}

func (t *qcloudRetryPutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls++
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	t.bodies = append(t.bodies, body)
	if t.calls == 1 {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Status:     "502 Bad Gateway",
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Request:    req,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Request:    req,
	}, nil
}

type trackingSeekReader struct {
	*bytes.Reader
	seekStartCount int
}

func (r *trackingSeekReader) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekStart {
		r.seekStartCount++
	}
	return r.Reader.Seek(offset, whence)
}

func TestQCloudSDKWriteNonSeekableFallbackPreservesSizeHint(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		server, state := newMockCOSServer(t, 0)
		defer server.Close()
		state.uploadID = "cos-size-mismatch-empty"

		sdk := newTestCOSClient(t, server)
		size := int64(smallObjectThreshold)
		err := sdk.Write(context.Background(), "object", nonSeekReader{r: bytes.NewReader(nil)}, &size, nil)
		if !moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch) {
			t.Fatalf("expected size mismatch, got %v", err)
		}
		if state.putCount != 0 || len(state.parts) != 0 {
			t.Fatalf("expected no successful upload for empty short reader")
		}
	})

	t.Run("short", func(t *testing.T) {
		server, state := newMockCOSServer(t, 0)
		defer server.Close()
		state.uploadID = "cos-size-mismatch-short"

		sdk := newTestCOSClient(t, server)
		size := int64(smallObjectThreshold)
		err := sdk.Write(context.Background(), "object", nonSeekReader{r: bytes.NewReader([]byte("short"))}, &size, nil)
		if !moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch) {
			t.Fatalf("expected size mismatch, got %v", err)
		}
		if state.putCount != 0 || len(state.parts) != 0 || state.completed.Load() {
			t.Fatalf("expected no committed object for short reader")
		}
	})
}

type nonSeekReader struct {
	r io.Reader
}

func (r nonSeekReader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

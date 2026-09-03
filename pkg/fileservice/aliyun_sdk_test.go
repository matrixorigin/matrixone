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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOSSCredential(t *testing.T) {
	creds := &ossCredential{
		upstream: new(credentials.CredentialModel),
	}
	id := creds.GetAccessKeyID()
	assert.Equal(t, "", id)
	secret := creds.GetAccessKeySecret()
	assert.Equal(t, "", secret)
	token := creds.GetSecurityToken()
	assert.Equal(t, "", token)
}

func TestAliyunSDKConditionalObjectIdentityReads(t *testing.T) {
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
			w.Header().Set("x-oss-version-id", "version-v1")
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

	client, err := oss.New(server.URL, "id", "secret",
		oss.ForcePathStyle(true), oss.HTTPClient(server.Client()))
	require.NoError(t, err)
	bucket, err := client.Bucket("bucket")
	require.NoError(t, err)
	sdk := &AliyunSDK{bucket: bucket}

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

func TestAliyunSDKCopyObjectPropagatesCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	client, err := oss.New(
		server.URL,
		"id",
		"secret",
		oss.ForcePathStyle(true),
		oss.HTTPClient(server.Client()),
	)
	require.NoError(t, err)
	srcBucket, err := client.Bucket("source-bucket")
	require.NoError(t, err)
	dstBucket, err := client.Bucket("destination-bucket")
	require.NoError(t, err)
	domain := newObjectStorageCopyCredentialDomain("id", "secret")
	src := &AliyunSDK{endpoint: server.URL, bucket: srcBucket, copyCredentialDomain: domain}
	dst := &AliyunSDK{endpoint: server.URL, bucket: dstBucket, copyCredentialDomain: domain}

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		copied bool
		err    error
	}
	done := make(chan result, 1)
	go func() {
		copied, copyErr := dst.CopyObject(ctx, src, "source", "destination")
		done <- result{copied: copied, err: copyErr}
	}()
	<-started
	cancel()

	select {
	case result := <-done:
		require.True(t, result.copied)
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Aliyun copy did not stop after cancellation")
	}
}

func TestAliyunSDKListStopsRetryingAtContextDeadline(t *testing.T) {
	client, err := oss.New(
		"http://oss.test",
		"id",
		"secret",
		oss.ForcePathStyle(true),
		oss.HTTPClient(&http.Client{Transport: aliyunRetryTimeoutTransport{}}),
	)
	require.NoError(t, err)
	bucket, err := client.Bucket("bucket")
	require.NoError(t, err)
	sdk := &AliyunSDK{bucket: bucket}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for _, err := range sdk.List(ctx, "") {
		require.ErrorIs(t, err, context.DeadlineExceeded)
		return
	}
	t.Fatal("expected list to return a deadline error")
}

type aliyunRetryTimeoutTransport struct{}

func (aliyunRetryTimeoutTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, aliyunRetryTimeoutError{}
}

type aliyunRetryTimeoutError struct{}

func (aliyunRetryTimeoutError) Error() string   { return "timeout" }
func (aliyunRetryTimeoutError) Timeout() bool   { return true }
func (aliyunRetryTimeoutError) Temporary() bool { return true }

func TestAliyunPutObjectPhysicalAccounting(t *testing.T) {
	var fail bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if fail {
			http.Error(w, "rejected", http.StatusBadRequest)
			return
		}
		w.Header().Set("ETag", "etag")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := oss.New(server.URL, "id", "secret",
		oss.UseCname(true), oss.HTTPClient(server.Client()))
	require.NoError(t, err)
	bucket, err := client.Bucket("bucket")
	require.NoError(t, err)
	sdk := &AliyunSDK{bucket: bucket}

	data := []byte("accepted")
	size := int64(len(data))
	counter := new(perfcounter.CounterSet)
	ctx := perfcounter.WithCounterSet(context.Background(), counter)
	require.NoError(t, sdk.putObject(ctx, "object", bytes.NewReader(data), &size, nil))
	require.Equal(t, int64(1), counter.FileService.S3.Put.Load())
	require.Equal(t, size, counter.FileService.S3WriteSize.Load())

	unknownSize := new(perfcounter.CounterSet)
	ctx = perfcounter.WithCounterSet(context.Background(), unknownSize)
	expire := time.Now().Add(time.Hour)
	require.NoError(t, sdk.putObject(ctx, "unknown", bytes.NewReader(data), nil, &expire))
	require.Equal(t, int64(1), unknownSize.FileService.S3.Put.Load())
	require.Equal(t, size, unknownSize.FileService.S3WriteSize.Load())

	fail = true
	failed := new(perfcounter.CounterSet)
	ctx = perfcounter.WithCounterSet(context.Background(), failed)
	require.Error(t, sdk.putObject(ctx, "object", bytes.NewReader(data), &size, nil))
	require.Equal(t, int64(1), failed.FileService.S3.Put.Load())
	require.Zero(t, failed.FileService.S3WriteSize.Load())
}

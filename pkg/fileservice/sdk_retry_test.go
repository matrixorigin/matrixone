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

package fileservice

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/stretchr/testify/require"
)

// failingHeads is an object storage server whose HEAD requests fail with the status and
// body of fail for the first fails of them, and succeed after. Before the first fails,
// it waits until a foreground request waits for room of g, and that request, once it
// runs, sends the HEADs made so far to ranWith.
type failingHeads struct {
	t        *testing.T
	g        *governor
	fails    int32
	status   int
	body     string
	attempts atomic.Int32
	ranWith  chan int32
}

func newFailingHeads(t *testing.T, fails int32, status int, body string) (*failingHeads, *httptest.Server) {
	f := &failingHeads{t: t, fails: fails, status: status, body: body, ranWith: make(chan int32, 1)}
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	return f, server
}

func (f *failingHeads) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Query().Has("location") {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<LocationConstraint>us-east-1</LocationConstraint>`)
		return
	}
	if r.Method != http.MethodHead {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	n := f.attempts.Add(1)
	if n == 1 {
		go func() {
			if err := waitRoom(f.g, context.Background(), foregroundRequest); err != nil {
				f.ranWith <- -1
				return
			}
			attempts := f.attempts.Load()
			f.g.done(foregroundRequest)
			f.ranWith <- attempts
		}()
		deadline := time.Now().Add(5 * time.Second)
		for _, waiting := f.g.counts(); waiting[foregroundRequest] == 0 && time.Now().Before(deadline); _, waiting = f.g.counts() {
			time.Sleep(time.Millisecond)
		}
	}
	if n <= f.fails {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, f.body)
		return
	}
	w.Header().Set("Content-Length", "6")
	w.Header().Set("ETag", `"etag"`)
	w.Header().Set("Last-Modified", time.Unix(1, 0).UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

// statHoldingAllOtherRoom stats object on raw through a governor whose room, but for
// one, is held, and returns its result and the HEADs made when the request that waited
// for room behind it ran.
func (f *failingHeads) statHoldingAllOtherRoom(raw ObjectStorage) (int64, int32, error) {
	f.g = newGovernor(raw, minRequestRoom)
	defer occupy(f.t, f.g, foregroundRequest, minRequestRoom-1)()
	res, err := f.g.Do(context.Background(), statRequest{key: "object"})
	ranWith := <-f.ranWith
	require.Zero(f.t, heldRoom(f.g))
	return res.n, ranWith, err
}

// requireRetriedWithoutRoom requires that the stat retried the failed HEAD once, and that
// the request waiting for the room the stat held ran before the retry.
func (f *failingHeads) requireRetriedWithoutRoom(size int64, ranWith int32, err error) {
	require.NoError(f.t, err)
	require.Equal(f.t, int64(6), size)
	require.Equal(f.t, int32(2), f.attempts.Load(), "the failed HEAD is made again once")
	require.Equal(f.t, int32(1), ranWith, "the request waiting for room runs during the retry's wait")
}

const serviceUnavailableXML = `<Error><Code>SlowDown</Code><Message>slow down</Message></Error>`

// A HEAD that fails once with a status the AWS SDK retries is made again by the file
// service's retry, which waits with no room held.
func TestAwsSDKv2RetriesWithoutRoom(t *testing.T) {
	f, server := newFailingHeads(t, 1, http.StatusServiceUnavailable, serviceUnavailableXML)
	sdk, err := NewAwsSDKv2(context.Background(), ObjectStorageArguments{
		Name: "aws", Bucket: "bucket", Endpoint: server.URL, Region: "us-east-1",
		KeyID: "id", KeySecret: "secret", NoBucketValidation: true,
	}, nil)
	require.NoError(t, err)
	f.requireRetriedWithoutRoom(f.statHoldingAllOtherRoom(sdk))
}

// A HEAD that fails once with a status the MinIO SDK retries is made again by the file
// service's retry, which waits with no room held.
func TestMinioSDKRetriesWithoutRoom(t *testing.T) {
	f, server := newFailingHeads(t, 1, http.StatusServiceUnavailable, serviceUnavailableXML)
	sdk, err := NewMinioSDK(context.Background(), ObjectStorageArguments{
		Endpoint: server.URL, Region: "us-east-1", Bucket: "bucket",
		KeyID: "id", KeySecret: "secret", NoBucketValidation: true,
	}, nil)
	require.NoError(t, err)
	f.requireRetriedWithoutRoom(f.statHoldingAllOtherRoom(sdk))
}

// A HEAD that fails with a status the file service does not retry is made once by the
// MinIO SDK.
func TestMinioSDKMakesOneAttempt(t *testing.T) {
	f, server := newFailingHeads(t, 1, http.StatusNotImplemented, `<Error><Code>NotImplemented</Code></Error>`)
	sdk, err := NewMinioSDK(context.Background(), ObjectStorageArguments{
		Endpoint: server.URL, Region: "us-east-1", Bucket: "bucket",
		KeyID: "id", KeySecret: "secret", NoBucketValidation: true,
	}, nil)
	require.NoError(t, err)
	_, ranWith, err := f.statHoldingAllOtherRoom(sdk)
	require.Error(t, err)
	require.Equal(t, int32(1), f.attempts.Load())
	require.Equal(t, int32(1), ranWith)
}

// A HEAD that fails once with a status the file service retries for COS is made again by
// the file service's retry, which waits with no room held.
func TestQCloudSDKRetriesWithoutRoom(t *testing.T) {
	f, server := newFailingHeads(t, 1, http.StatusServiceUnavailable, serviceUnavailableXML)
	sdk, err := NewQCloudSDK(context.Background(), ObjectStorageArguments{
		Name: "cos", Bucket: "bucket", Region: "ap-guangzhou",
		KeyID: "id", KeySecret: "secret", NoBucketValidation: true,
	}, nil)
	require.NoError(t, err)
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	sdk.client.BaseURL.BucketURL = u
	f.requireRetriedWithoutRoom(f.statHoldingAllOtherRoom(sdk))
}

// The Aliyun SDK makes one attempt per call: a HEAD that fails with a status the file
// service does not retry is made once.
func TestAliyunSDKMakesOneAttempt(t *testing.T) {
	f, server := newFailingHeads(t, 1, http.StatusServiceUnavailable, serviceUnavailableXML)
	client, err := oss.New(server.URL, "id", "secret", oss.ForcePathStyle(true), oss.HTTPClient(server.Client()))
	require.NoError(t, err)
	bucket, err := client.Bucket("bucket")
	require.NoError(t, err)
	_, ranWith, err := f.statHoldingAllOtherRoom(&AliyunSDK{bucket: bucket})
	require.Error(t, err)
	require.Equal(t, int32(1), f.attempts.Load())
	require.Equal(t, int32(1), ranWith)
}

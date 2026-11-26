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
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newConnCountingServer returns an object storage stub that answers every HEAD with a
// 3-byte object, and the count of its open connections.
func newConnCountingServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	var open atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "3")
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Last-Modified", "Wed, 02 Sep 2026 03:04:05 GMT")
		w.WriteHeader(http.StatusOK)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			open.Add(1)
		case http.StateClosed, http.StateHijacked:
			open.Add(-1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	return server, &open
}

func newStubS3FS(t *testing.T, endpoint string, minio bool) *S3FS {
	fs, err := NewS3FS(context.Background(), ObjectStorageArguments{
		Name:               "s3",
		Endpoint:           endpoint,
		Region:             "us-east-1",
		Bucket:             "bucket",
		KeyID:              "id",
		KeySecret:          "secret",
		IsMinio:            minio,
		NoBucketValidation: true,
	}, DisabledCacheConfig, nil, true, true)
	require.NoError(t, err)
	return fs
}

// A closed S3FS leaves no idle connection open, well before the idle timeout; a second
// Close does nothing. Control: an S3FS left open keeps its idle connection.
func TestS3FSCloseReleasesIdleConnections(t *testing.T) {
	for _, minio := range []bool{false, true} {
		name := "aws"
		if minio {
			name = "minio"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			server, open := newConnCountingServer(t)

			control := newStubS3FS(t, server.URL, minio)
			defer control.Close(ctx)
			_, err := control.StatFile(ctx, "control")
			require.NoError(t, err)
			require.Eventually(t, func() bool { return open.Load() == 1 }, time.Second, time.Millisecond)

			fs := newStubS3FS(t, server.URL, minio)
			_, err = fs.StatFile(ctx, "object")
			require.NoError(t, err)
			require.Eventually(t, func() bool { return open.Load() == 2 }, time.Second, time.Millisecond)

			fs.Close(ctx)
			require.Eventually(t, func() bool { return open.Load() == 1 }, time.Second, time.Millisecond,
				"the closed file service leaves an idle connection open")
			fs.Close(ctx)

			time.Sleep(100 * time.Millisecond)
			require.Equal(t, int64(1), open.Load(), "control: the open file service keeps its idle connection")
		})
	}
}

type closeCountingETLFS struct {
	ETLFileService
	name   string
	closed atomic.Int64
}

func (c *closeCountingETLFS) Name() string { return c.name }

func (c *closeCountingETLFS) Close(context.Context) { c.closed.Add(1) }

// CloseOwnedETL closes a file service GetForETL created for a local path or a path with
// service arguments, and leaves the named service GetForETL returns open.
func TestCloseOwnedETL(t *testing.T) {
	ctx := WithSQLFileAccess(context.Background(), "sys", "moadmin")
	shared := &closeCountingETLFS{name: "etl"}
	services, err := NewFileServices("etl", shared)
	require.NoError(t, err)

	got, _, err := GetForETL(ctx, services, "etl:/a.csv")
	require.NoError(t, err)
	require.Same(t, shared, got)
	CloseOwnedETL(ctx, "etl:/a.csv", got)
	require.Zero(t, shared.closed.Load(), "the named service is shared")

	for _, path := range []string{
		"/tmp/a.csv",
		"s3-opts,endpoint=http://127.0.0.1:1,region=r,bucket=b,key=k,secret=s:/a.csv",
	} {
		owned := &closeCountingETLFS{name: "owned"}
		CloseOwnedETL(ctx, path, owned)
		require.Equal(t, int64(1), owned.closed.Load(), path)
	}

	CloseOwnedETL(ctx, "/tmp/a.csv", nil)
}

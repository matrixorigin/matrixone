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

package plan

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// StatFile of an S3 source closes the file service made for it, which leaves no
// connection open; OwnsETLWithType tells such a source from a named service.
func TestStatFileClosesOwnedFileService(t *testing.T) {
	var open, requests atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", "3")
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
	defer server.Close()

	param := &tree.ExternParam{}
	param.Ctx = fileservice.WithSQLFileAccess(context.Background(), "sys", "moadmin")
	param.ScanType = tree.S3
	param.Filepath = "/a.csv"
	param.S3Param = &tree.S3Parameter{
		Endpoint: server.URL, Region: "us-east-1", APIKey: "id", APISecret: "secret", Bucket: "bucket",
	}
	require.True(t, OwnsETLWithType(param, param.Filepath))
	require.NoError(t, StatFile(param))
	require.Equal(t, int64(3), param.FileSize)
	require.Positive(t, requests.Load())
	require.Eventually(t, func() bool { return open.Load() == 0 }, time.Second, time.Millisecond,
		"the file service of StatFile leaves its connection open")

	named := &tree.ExternParam{}
	named.ScanType = tree.INFILE
	require.False(t, OwnsETLWithType(named, "etl:/a.csv"))
	require.True(t, OwnsETLWithType(named, "/a.csv"))
}

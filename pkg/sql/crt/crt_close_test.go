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
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Closing the reader of an S3 load source closes the file service made for it, which
// leaves no connection open.
func TestGetIOReadCloserClosesOwnedFileService(t *testing.T) {
	var open atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, "abc")
		}
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

	proc := testutil.NewProcess(t)
	param := &tree.ExternParam{}
	param.ScanType = tree.S3
	param.S3Param = &tree.S3Parameter{
		Endpoint: server.URL, Region: "us-east-1", APIKey: "id", APISecret: "secret", Bucket: "bucket",
	}
	r, err := GetIOReadCloser(proc, param, "/a.csv", nil, math.MaxInt64)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "abc", string(data))
	require.Equal(t, int64(1), open.Load())

	require.NoError(t, r.Close())
	require.Eventually(t, func() bool { return open.Load() == 0 }, time.Second, time.Millisecond,
		"the file service of the reader leaves its connection open")
}

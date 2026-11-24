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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A configured connection limit below minMaxConnsPerHost is raised to it, so requests
// that hold their connections, as downloads do, leave connections to other requests.
func TestHTTPClientConnsPerHostFloor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		conns int
	}{{"configured below the floor", 2}, {"default", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			gate := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/held") {
					<-gate
				}
				_, _ = io.WriteString(w, "body")
			}))
			defer server.Close()
			defer close(gate)
			client := newHTTPClient(ObjectStorageArguments{MaxConnsPerHost: tc.conns})
			defer client.CloseIdleConnections()
			get := func(ctx context.Context, path string) error {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
				if err != nil {
					return err
				}
				resp, err := client.Do(req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				_, err = io.ReadAll(resp.Body)
				return err
			}
			for i := 0; i < 2; i++ {
				go func() { _ = get(context.Background(), "/held") }()
			}
			time.Sleep(100 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			require.NoError(t, get(ctx, "/other"), "a request waits for no connection held by others")
		})
	}
}

func TestConnsPerHost(t *testing.T) {
	require.Equal(t, 0, connsPerHost(0))
	require.Equal(t, minMaxConnsPerHost, connsPerHost(1))
	require.Equal(t, minMaxConnsPerHost, connsPerHost(2))
	require.Equal(t, 256, connsPerHost(256))
}

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

package foreigntvf

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/stretchr/testify/require"
)

// Compare actual HTTP requests with the pinned constructor, rather than
// deriving an expected request from the new header/config translation.
func TestESQLClientWireParity(t *testing.T) {
	require.Equal(t, elasticsearch.Version, esqlClientVersion, "sync the pinned header version when upgrading go-elasticsearch")
	type request struct {
		method, uri, body string
		headers           http.Header
		length            int64
	}
	for _, tc := range []struct {
		name, compatibility string
		config              esqlConfig
	}{
		{name: "default"},
		{name: "basic", config: esqlConfig{Username: "user", Password: "secret"}},
		{name: "incomplete basic", config: esqlConfig{Username: "user"}},
		{name: "service token", config: esqlConfig{Username: "user", Password: "secret", ServiceToken: "token"}},
		{name: "API key", config: esqlConfig{Username: "user", Password: "secret", ServiceToken: "token", APIKey: "key"}},
		{name: "URL userinfo and prefix", config: esqlConfig{Addresses: []string{"http://urluser:urlpassword@<server>/prefix///"}, Username: "other", Password: "other", ServiceToken: "token", APIKey: "key"}},
		{name: "compatibility", compatibility: "true"},
		{name: "compatibility numeric", compatibility: "1"},
		{name: "compatibility disabled", compatibility: "false"},
		{name: "compatibility invalid", compatibility: "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan request, 4)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				select {
				case requests <- request{r.Method, r.URL.RequestURI(), string(body), r.Header.Clone(), r.ContentLength}:
				default:
					t.Error("unexpected extra request")
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				_, _ = io.WriteString(w, "v\n1\n")
			}))
			t.Cleanup(srv.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			t.Setenv("ELASTIC_CLIENT_APIVERSIONING", tc.compatibility)
			c := tc.config
			if len(c.Addresses) != 0 {
				c.Addresses = []string{strings.ReplaceAll(c.Addresses[0], "<server>", strings.TrimPrefix(srv.URL, "http://"))}
			}
			if len(c.Addresses) == 0 {
				c.Addresses = []string{srv.URL}
			}
			makeTransport := func() *http.Transport { tr := &http.Transport{}; t.Cleanup(tr.CloseIdleConnections); return tr }
			original, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: c.Addresses, Username: c.Username, Password: c.Password, APIKey: c.APIKey, ServiceToken: c.ServiceToken, Transport: makeTransport()})
			require.NoError(t, err)
			optimized, err := c.newClient(makeTransport())
			require.NoError(t, err)
			// Construction captures the environment, rather than changing an
			// admitted connection's behavior on a later process-env change.
			t.Setenv("ELASTIC_CLIENT_APIVERSIONING", "false")
			for _, client := range []esapi.Transport{original, optimized} {
				for _, req := range []esapi.Request{
					esapi.InfoRequest{},
					esapi.EsqlQueryRequest{Body: bytes.NewBufferString(`{"query":"FROM idx | LIMIT 1"}`), Format: "csv"},
				} {
					func() {
						res, err := req.Do(ctx, client)
						require.NoError(t, err)
						defer func() { require.NoError(t, res.Body.Close()) }()
						_, err = io.Copy(io.Discard, res.Body)
						require.NoError(t, err)
					}()
				}
			}
			wantInfo, wantQuery, gotInfo, gotQuery := <-requests, <-requests, <-requests, <-requests
			require.Equal(t, wantInfo, gotInfo)
			require.Equal(t, wantQuery, gotQuery)
		})
	}
}

func TestESQLClientEndpointParity(t *testing.T) {
	cloud := "label:" + base64.StdEncoding.EncodeToString([]byte("example.com:9243$es-id$kibana-id"))
	for _, c := range []esqlConfig{
		{CloudID: cloud},
		{CloudID: "missing-colon"},
		{CloudID: "label:bad-base64"},
		{CloudID: "label:" + base64.StdEncoding.EncodeToString([]byte("missing-separator"))},
		{Addresses: []string{"http://endpoint"}, CloudID: cloud},
		{Addresses: []string{"http://%invalid"}},
	} {
		t.Run(c.CloudID+strings.Join(c.Addresses, ","), func(t *testing.T) {
			original, originalErr := elasticsearch.NewClient(elasticsearch.Config{Addresses: c.Addresses, CloudID: c.CloudID, Transport: &http.Transport{}})
			optimized, err := c.newClient(&http.Transport{})
			if originalErr != nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, original.Transport.(*elastictransport.Client).URLs(), optimized.URLs())
		})
	}
}

func TestESQLClientProductCheck(t *testing.T) {
	for _, product := range []string{"", "other", "Elasticsearch"} {
		t.Run(product, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.Header().Set("X-Elastic-Product", product)
				}
				_, _ = io.WriteString(w, "v\n1\n")
			}))
			t.Cleanup(srv.Close)
			conn, err := connectESQL(t.Context(), esConfigJSON(srv.URL))
			if product != "Elasticsearch" {
				require.Error(t, err)
				require.Nil(t, conn)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			// A validated connection keeps the SDK's cached product-check result.
			stream, err := conn.Query(t.Context(), "FROM idx")
			require.NoError(t, err)
			require.NoError(t, stream.Close())
		})
	}
}

func TestESQLClientCancellation(t *testing.T) {
	for _, phase := range []string{"info", "query", "stream"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if phase == "stream" && r.Method == http.MethodPost {
					w.Header().Set("Content-Length", "1000")
					_, _ = io.WriteString(w, "v\n1\n")
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				if (phase == "info" && r.Method == http.MethodGet) || (phase == "query" && r.Method == http.MethodPost) {
					close(entered)
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				_, _ = io.WriteString(w, "{}")
			}))
			t.Cleanup(srv.Close)
			deadline, stop := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(stop)
			ctx, cancel := context.WithCancel(deadline)
			t.Cleanup(cancel)
			done := make(chan error, 1)
			if phase == "info" {
				go func() {
					conn, err := connectESQL(ctx, esConfigJSON(srv.URL))
					if conn != nil {
						_ = conn.Close()
					}
					done <- err
				}()
			} else {
				conn, err := connectESQL(t.Context(), esConfigJSON(srv.URL))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, conn.Close()) })
				go func() {
					stream, err := conn.Query(ctx, "FROM idx")
					if phase == "stream" && err == nil {
						// Observe real streaming bytes while the server holds EOF.
						_, err = io.ReadFull(stream, make([]byte, 4))
						if err == nil {
							close(entered)
							_, err = io.ReadAll(stream)
						}
					}
					if stream != nil {
						_ = stream.Close()
					}
					done <- err
				}()
			}
			joined := false
			t.Cleanup(func() {
				cancel()
				if !joined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("cancelled request did not join")
					}
				}
			})
			select {
			case <-entered:
			case <-deadline.Done():
				t.Fatal(deadline.Err())
			}
			cancel()
			select {
			case err := <-done:
				joined = true
				require.ErrorContains(t, err, "context canceled")
			case <-deadline.Done():
				t.Fatal(deadline.Err())
			}
		})
	}
}

// An upgrade is not a usable Info handshake. Reject it before cache admission,
// rather than admitting a connection whose first real product check is pending.
func TestESQLClientRejectsProtocolUpgrade(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Connection", "upgrade")
		w.Header().Set("Upgrade", "test")
		w.WriteHeader(http.StatusSwitchingProtocols)
		w.(http.Flusher).Flush()
	}))
	t.Cleanup(srv.Close)
	conn, err := connectESQL(t.Context(), esConfigJSON(srv.URL))
	require.Nil(t, conn)
	require.ErrorContains(t, err, "101 Switching Protocols")
}

func TestESQLClientConcurrentRetry(t *testing.T) {
	var mu sync.Mutex
	attempts := make(map[string]int)
	var firstRequests atomic.Int32
	bothEntered := make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(bothEntered) }) })
	srv := fakeES(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			http.Error(w, "invalid replay body", http.StatusBadRequest)
			return
		}
		query := body["query"]
		mu.Lock()
		attempts[query]++
		attempt := attempts[query]
		mu.Unlock()
		if attempt == 1 {
			if firstRequests.Add(1) == 2 {
				release.Do(func() { close(bothEntered) })
			}
			select {
			case <-bothEntered:
			case <-r.Context().Done():
				return
			}
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "v\n"+query+"\n")
	})
	conn, err := connectESQL(t.Context(), esConfigJSON(srv.URL))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	type result struct {
		query, csv string
		err        error
	}
	done := make(chan result, 2)
	for _, query := range []string{"first", "second"} {
		go func() {
			stream, err := conn.Query(ctx, query)
			if err != nil {
				done <- result{query: query, err: err}
				return
			}
			csv, err := io.ReadAll(stream)
			closeErr := stream.Close()
			if err == nil {
				err = closeErr
			}
			done <- result{query: query, csv: string(csv), err: err}
		}()
	}
	// Receive both terminal results before assertions can end the test.
	results := []result{<-done, <-done}
	for _, r := range results {
		require.NoError(t, r.err)
		require.Equal(t, "v\n"+r.query+"\n", r.csv)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[string]int{"first": 2, "second": 2}, attempts)
}

// Neither constructor dials. Both use the same private transport and normal
// credentials/defaults; this measures endpoint registration, not network time.
func BenchmarkESQLClientConstruction(b *testing.B) {
	tr := &http.Transport{}
	b.Cleanup(tr.CloseIdleConnections)
	c := esqlConfig{Addresses: []string{"http://localhost:9200"}}
	for _, tc := range []struct {
		name      string
		construct func() (esapi.Transport, error)
	}{
		{"all endpoints", func() (esapi.Transport, error) {
			return elasticsearch.NewClient(elasticsearch.Config{Addresses: c.Addresses, Transport: tr})
		}},
		{"ESQL endpoints", func() (esapi.Transport, error) { return c.newClient(tr) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				client, err := tc.construct()
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(client)
			}
		})
	}
}

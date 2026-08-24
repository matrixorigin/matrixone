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
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeES serves the minimal Elasticsearch surface the connector touches: the
// product-check/info handshake and the ES|QL _query endpoint.
func fakeES(t *testing.T, query func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// go-elasticsearch validates this header before trusting responses.
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.URL.Path == "/" || r.URL.Path == "":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"name":"fake","version":{"number":"8.15.0","build_flavor":"default"},"tagline":"You Know, for Search"}`)
		case strings.HasPrefix(r.URL.Path, "/_query"):
			query(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func esConfigJSON(url string) string {
	b, _ := json.Marshal(map[string]any{"addresses": []string{url}})
	return string(b)
}

func TestConnectESQLAndQuery(t *testing.T) {
	ctx := context.Background()
	var gotQuery string
	srv := fakeES(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		gotQuery = body["query"]
		require.Equal(t, "csv", r.URL.Query().Get("format"))
		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprint(w, "a,b\r\n1,x\r\n2,y\r\n")
	})

	conn, err := connectESQL(ctx, esConfigJSON(srv.URL))
	require.NoError(t, err)
	require.Equal(t, KindESQL, conn.Kind())

	stream, err := conn.Query(ctx, "FROM idx | LIMIT 2")
	require.NoError(t, err)
	data, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, "a,b\r\n1,x\r\n2,y\r\n", string(data))
	require.Equal(t, "FROM idx | LIMIT 2", gotQuery)

	// Close releases idle sockets and is idempotent.
	require.NoError(t, conn.Close())
	require.NoError(t, conn.Close())
}

func TestConnectESQLErrors(t *testing.T) {
	ctx := context.Background()

	// malformed config JSON
	_, err := connectESQL(ctx, "not json")
	require.ErrorContains(t, err, "invalid elasticsearch config")

	// unreachable server: fail fast at connect (Info handshake)
	_, err = connectESQL(ctx, esConfigJSON("http://127.0.0.1:1"))
	require.ErrorContains(t, err, "cannot reach elasticsearch")
}

func TestEsqlQueryErrorStatus(t *testing.T) {
	ctx := context.Background()
	srv := fakeES(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"reason":"unknown index [nope]"}}`)
	})
	conn, err := connectESQL(ctx, esConfigJSON(srv.URL))
	require.NoError(t, err)
	_, err = conn.Query(ctx, "FROM nope")
	require.ErrorContains(t, err, "query error")
	require.ErrorContains(t, err, "unknown index")
}

// TestEsqlQueryTruncatedResponse proves a mid-stream connection drop surfaces
// as a hard "response truncated" error instead of silent end-of-data: the
// server advertises a Content-Length larger than what it sends.
func TestEsqlQueryTruncatedResponse(t *testing.T) {
	ctx := context.Background()
	srv := fakeES(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Length", "1000")
		fmt.Fprint(w, "a,b\r\n1,x\r\n") // then the handler returns: connection closes short
	})
	conn, err := connectESQL(ctx, esConfigJSON(srv.URL))
	require.NoError(t, err)
	stream, err := conn.Query(ctx, "FROM idx")
	require.NoError(t, err)
	_, err = io.ReadAll(stream)
	require.Error(t, err)
	require.ErrorContains(t, err, "response truncated")
	_ = stream.Close()
}

func TestValidateESQLConfig(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, validateESQLConfig(ctx, `{"addresses":["http://h:9200"]}`))
	require.NoError(t, validateESQLConfig(ctx, `{"cloudid":"x:y"}`))
	require.Error(t, validateESQLConfig(ctx, `nope`))
	require.Error(t, validateESQLConfig(ctx, `{}`)) // neither addresses nor cloudid
	// field names stay case-insensitive, as with the previous decoder
	require.NoError(t, validateESQLConfig(ctx, `{"Addresses":["http://h:9200"],"Username":"u"}`))
	// trailing garbage after the object is rejected
	require.Error(t, validateESQLConfig(ctx, `{"addresses":["http://h"]}{}`))
}

// TestESQLConfigRejectsLifecycleKnobs proves the session JSON can only set
// the whitelisted endpoint/credential fields. The library's
// elasticsearch.Config also carries lifecycle and process-global knobs a SQL
// session must not reach: CACert (library-side handling clones the transport
// and panics on a nil TLSClientConfig), DiscoverNodesInterval (installs a
// self-rescheduling timer nothing can stop), EnableDebugLogger (writes an
// unsynchronized package-global). All unknown fields fail closed on both the
// validate and connect paths.
func TestESQLConfigRejectsLifecycleKnobs(t *testing.T) {
	ctx := context.Background()
	for _, cfg := range []string{
		`{"addresses":["http://h:9200"],"EnableDebugLogger":true}`,
		`{"addresses":["http://h:9200"],"DiscoverNodesInterval":20000000}`,
		`{"addresses":["http://h:9200"],"DiscoverNodesOnStart":true}`,
		`{"addresses":["http://h:9200"],"RetryOnStatus":[502]}`,
		`{"addresses":["http://h:9200"],"EnableMetrics":true}`,
		`{"addresses":["http://h:9200"],"bogus":1}`,
	} {
		require.ErrorContains(t, validateESQLConfig(ctx, cfg), "invalid elasticsearch config", cfg)
		_, err := connectESQL(ctx, cfg)
		require.ErrorContains(t, err, "invalid elasticsearch config", cfg)
	}
}

// TestConnectESQLCACert covers the MO-owned private-CA path: the PEM applies
// to MO's own transport (never the library's CACert field, whose clone would
// break Close ownership — and panic). Success and handshake-failure paths
// both leave zero open server connections after Close/failure.
func TestConnectESQLCACert(t *testing.T) {
	ctx := context.Background()

	newTLS := func(status int) (*httptest.Server, *atomic.Int64, string) {
		var open atomic.Int64
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			if status != http.StatusOK {
				w.WriteHeader(status)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"name":"fake","version":{"number":"8.15.0","build_flavor":"default"},"tagline":"You Know, for Search"}`)
		}))
		srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
			switch s {
			case http.StateNew:
				open.Add(1)
			case http.StateClosed, http.StateHijacked:
				open.Add(-1)
			}
		}
		srv.StartTLS()
		t.Cleanup(srv.Close)
		caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
		return srv, &open, caPEM
	}
	cfgWithCA := func(url, caPEM string) string {
		b, _ := json.Marshal(map[string]any{"addresses": []string{url}, "cacert": caPEM})
		return string(b)
	}

	// a valid private CA connects (no panic, no library clone), and Close
	// drains the actual socket-owning transport
	srv, open, caPEM := newTLS(http.StatusOK)
	conn, err := connectESQL(ctx, cfgWithCA(srv.URL, caPEM))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool { return open.Load() == 0 },
		5*time.Second, 20*time.Millisecond, "Close must drain the transport the client actually uses")

	// without the CA the handshake is rejected (proves verification is on)
	_, err = connectESQL(ctx, esConfigJSON(srv.URL))
	require.ErrorContains(t, err, "cannot reach elasticsearch")

	// a 401 behind the private CA takes the failure defer: no socket leaks
	srv401, open401, caPEM401 := newTLS(http.StatusUnauthorized)
	_, err = connectESQL(ctx, cfgWithCA(srv401.URL, caPEM401))
	require.ErrorContains(t, err, "elasticsearch returned")
	require.Eventually(t, func() bool { return open401.Load() == 0 },
		5*time.Second, 20*time.Millisecond, "failed CA connects must not leave open connections")

	// non-PEM cacert content is a clean error
	_, err = connectESQL(ctx, cfgWithCA("https://h:9200", "not pem"))
	require.ErrorContains(t, err, "no valid PEM certificate")
}

// TestConnectESQLRejectsEmptyConfig proves an empty/endpoint-less config
// cannot inherit process defaults (ELASTICSEARCH_URL / localhost:9200): the
// connect path itself validates, not just the DDL path.
func TestConnectESQLRejectsEmptyConfig(t *testing.T) {
	t.Setenv("ELASTICSEARCH_URL", "http://operator-secret-host:9200")
	ctx := context.Background()
	for _, cfg := range []string{`{}`, `{"username":"u","password":"p"}`} {
		_, err := connectESQL(ctx, cfg)
		require.ErrorContains(t, err, "needs addresses or a cloudid", cfg)
	}
	// and through the public connect-or-reuse path as well
	cache := newFakeConnCache()
	_, _, err := ResolveOrConnect(ctx, cache, KindESQL, `{}`)
	require.ErrorContains(t, err, "needs addresses or a cloudid")
}

// TestConnectESQLFailedHandshakeClosesTransport proves a failed connect leaves
// no unowned keep-alive socket: the private transport is drained on every
// failure path. A failed connect is never admitted to the session cache, so
// nothing else could ever release those sockets (regression: 8 failed
// connects against a 401 endpoint left 8 server connections in StateIdle for
// IdleConnTimeout).
func TestConnectESQLFailedHandshakeClosesTransport(t *testing.T) {
	ctx := context.Background()
	var open atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusUnauthorized)
	}))
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			open.Add(1)
		case http.StateClosed, http.StateHijacked:
			open.Add(-1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	for i := 0; i < 8; i++ {
		_, err := connectESQL(ctx, esConfigJSON(srv.URL))
		require.ErrorContains(t, err, "elasticsearch returned")
	}
	require.Eventually(t, func() bool { return open.Load() == 0 },
		5*time.Second, 20*time.Millisecond,
		"failed connects must not leave open keep-alive connections")
}

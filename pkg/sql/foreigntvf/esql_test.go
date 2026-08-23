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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

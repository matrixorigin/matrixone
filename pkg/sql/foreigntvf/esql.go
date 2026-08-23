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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// EsqlConn is a connection to an Elasticsearch cluster used to run ES|QL
// queries. The go-elasticsearch client is safe for concurrent use and pools
// HTTP connections in its private transport.
type EsqlConn struct {
	es *elasticsearch.Client
	// transport is the client's private http.Transport, kept so Close can
	// release its idle keep-alive sockets (the client itself has no Close).
	transport *http.Transport
}

var _ Conn = (*EsqlConn)(nil)

// validateESQLConfig checks the elasticsearch.Config JSON shape without
// creating a client or dialing.
func validateESQLConfig(ctx context.Context, configJSON string) error {
	var cfg elasticsearch.Config
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return moerr.NewInvalidInputf(ctx, "esql: invalid elasticsearch config: %v", err)
	}
	if len(cfg.Addresses) == 0 && cfg.CloudID == "" {
		return moerr.NewInvalidInput(ctx, "esql: elasticsearch config needs addresses or a cloudid")
	}
	return nil
}

// connectESQL parses configJSON as an elasticsearch.Config, builds a client,
// and verifies connectivity so connect() fails fast with a clear error.
func connectESQL(ctx context.Context, configJSON string) (Conn, error) {
	// Every runtime path must pass the same endpoint validation the DDL path
	// uses: go-elasticsearch treats an empty config as "read ELASTICSEARCH_URL
	// or default to localhost:9200" (including URL userinfo as credentials),
	// so an unvalidated '{}' would inherit operator-configured process
	// defaults and bypass the sys-only env: gate.
	if err := validateESQLConfig(ctx, configJSON); err != nil {
		return nil, err
	}
	var cfg elasticsearch.Config
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "esql_tvf: invalid elasticsearch config: %v", err)
	}
	// MO's fileservice replaces http.DefaultTransport with a custom
	// RoundTripper, so the ES client cannot clone it. Always provide an
	// explicit transport (Transport is not settable from the JSON config).
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	cfg.Transport = transport
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "esql_tvf: cannot create elasticsearch client: %v", err)
	}
	res, err := es.Info(es.Info.WithContext(ctx))
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "esql_tvf: cannot reach elasticsearch: %v", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, moerr.NewInternalErrorf(ctx, "esql_tvf: elasticsearch returned %s", res.Status())
	}
	return &EsqlConn{es: es, transport: transport}, nil
}

func (c *EsqlConn) Kind() Kind { return KindESQL }

// Close releases the idle keep-alive sockets of the connection's private
// transport. In-flight requests are unaffected (their sockets close when the
// response body is closed).
func (c *EsqlConn) Close() error {
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	return nil
}

// Query sends esql to the ES|QL _query API requesting CSV output and returns
// the response body (a CSV stream with a header row). ES renders NULL as an
// empty field; the external reader treats an empty numeric field as NULL.
func (c *EsqlConn) Query(ctx context.Context, esql string) (io.ReadCloser, error) {
	body, err := json.Marshal(map[string]string{"query": esql})
	if err != nil {
		return nil, err
	}
	res, err := c.es.EsqlQuery(
		bytes.NewReader(body),
		c.es.EsqlQuery.WithContext(ctx),
		c.es.EsqlQuery.WithFormat("csv"),
	)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "esql_tvf: query failed: %v", err)
	}
	if res.IsError() {
		// Bound the error-body read: a misconfigured endpoint could stream an
		// unbounded body on its error path.
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 8192))
		res.Body.Close()
		return nil, moerr.NewInvalidInputf(ctx, "esql_tvf: query error %s: %s", res.Status(), string(msg))
	}
	// The CSV parser must treat io.ErrUnexpectedEOF as its own normal
	// final-partial-block signal, but net/http also reports a PREMATURELY
	// CLOSED response body (node restart, LB idle timeout) as
	// io.ErrUnexpectedEOF — which would silently truncate the result. Remap
	// the transport-level one to a real error before the parser can see it.
	return &truncationGuard{ctx: ctx, body: res.Body}, nil
}

// truncationGuard converts a transport-level io.ErrUnexpectedEOF from the ES
// response body into a hard error so a dropped connection can never be
// mistaken for end-of-data.
type truncationGuard struct {
	ctx  context.Context
	body io.ReadCloser
}

func (g *truncationGuard) Read(p []byte) (int, error) {
	n, err := g.body.Read(p)
	if err == io.ErrUnexpectedEOF {
		return n, moerr.NewInternalError(g.ctx, "esql: response truncated (connection to elasticsearch closed mid-stream)")
	}
	return n, err
}

func (g *truncationGuard) Close() error { return g.body.Close() }

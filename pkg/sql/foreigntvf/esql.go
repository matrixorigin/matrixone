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
// HTTP connections internally; there is no explicit close, so Close is a no-op.
type EsqlConn struct {
	es *elasticsearch.Client
}

var _ Conn = (*EsqlConn)(nil)

// connectESQL parses configJSON as an elasticsearch.Config, builds a client,
// and verifies connectivity so connect() fails fast with a clear error.
func connectESQL(ctx context.Context, configJSON string) (Conn, error) {
	var cfg elasticsearch.Config
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "esql_tvf: invalid elasticsearch config: %v", err)
	}
	// MO's fileservice replaces http.DefaultTransport with a custom
	// RoundTripper, so the ES client cannot clone it. Always provide an
	// explicit transport (Transport is not settable from the JSON config).
	cfg.Transport = &http.Transport{
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
	return &EsqlConn{es: es}, nil
}

func (c *EsqlConn) Kind() Kind { return KindESQL }

func (c *EsqlConn) Close() error { return nil }

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
		msg, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return nil, moerr.NewInvalidInputf(ctx, "esql_tvf: query error %s: %s", res.Status(), string(msg))
	}
	return res.Body, nil
}

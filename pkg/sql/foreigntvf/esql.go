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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
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

// esqlConfig is the whitelist of elasticsearch config options a SQL session
// may set: endpoint and credential fields only. It is deliberately NOT the
// library's elasticsearch.Config — that struct also carries lifecycle and
// process-global knobs MO cannot own from a session (CACert makes
// elastictransport clone the transport and panic on a nil TLSClientConfig,
// DiscoverNodesInterval installs a self-rescheduling timer with no Stop, and
// EnableDebugLogger writes an unsynchronized package-global). Unknown fields
// are rejected fail-closed, never silently ignored.
type esqlConfig struct {
	Addresses              []string `json:"addresses"`
	Username               string   `json:"username"`
	Password               string   `json:"password"`
	CloudID                string   `json:"cloudid"`
	APIKey                 string   `json:"apikey"`
	ServiceToken           string   `json:"servicetoken"`
	CertificateFingerprint string   `json:"certificatefingerprint"`
	// CACert is a PEM bundle for a private CA. MO applies it to its own
	// transport's TLS config; it is never passed to the library (whose CACert
	// path clones the transport, breaking Close ownership).
	CACert string `json:"cacert"`
}

// parseESQLConfig strictly decodes the session-supplied JSON into the
// whitelisted config: unknown fields (in particular every library lifecycle
// or global knob) are an error.
func parseESQLConfig(ctx context.Context, configJSON string) (esqlConfig, error) {
	var c esqlConfig
	dec := json.NewDecoder(strings.NewReader(configJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return esqlConfig{}, moerr.NewInvalidInputf(ctx,
			"esql: invalid elasticsearch config (allowed fields: addresses, username, password, cloudid, apikey, servicetoken, certificatefingerprint, cacert): %v", err)
	}
	if dec.More() {
		return esqlConfig{}, moerr.NewInvalidInput(ctx, "esql: invalid elasticsearch config: trailing data after JSON object")
	}
	if len(c.Addresses) == 0 && c.CloudID == "" {
		return esqlConfig{}, moerr.NewInvalidInput(ctx, "esql: elasticsearch config needs addresses or a cloudid")
	}
	return c, nil
}

// validateESQLConfig checks the config JSON shape without creating a client
// or dialing.
func validateESQLConfig(ctx context.Context, configJSON string) error {
	_, err := parseESQLConfig(ctx, configJSON)
	return err
}

// connectESQL parses configJSON as an elasticsearch.Config, builds a client,
// and verifies connectivity so connect() fails fast with a clear error.
func connectESQL(ctx context.Context, configJSON string) (Conn, error) {
	// Every runtime path must pass the same strict parse the DDL path uses:
	// only whitelisted endpoint/credential fields, and an endpoint is
	// mandatory — go-elasticsearch treats an empty config as "read
	// ELASTICSEARCH_URL or default to localhost:9200" (including URL userinfo
	// as credentials), so an unvalidated '{}' would inherit
	// operator-configured process defaults — query processing must never
	// read the CN environment.
	c, err := parseESQLConfig(ctx, configJSON)
	if err != nil {
		return nil, err
	}
	// MO's fileservice replaces http.DefaultTransport with a custom
	// RoundTripper, so the ES client cannot clone it. Always provide an
	// explicit transport; with no library CACert set, elastictransport uses
	// it as-is, so EsqlConn.Close and the failure defer below drain the
	// transport that actually owns the sockets.
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
	if c.CACert != "" {
		// Applied to the MO-owned transport, NOT via the library's CACert
		// field: that path clones the transport (and panics on a nil
		// TLSClientConfig), leaving Close draining an object the client no
		// longer uses.
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(c.CACert)) {
			return nil, moerr.NewInvalidInput(ctx, "esql: cacert contains no valid PEM certificate")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	cfg := elasticsearch.Config{
		Addresses:              c.Addresses,
		Username:               c.Username,
		Password:               c.Password,
		CloudID:                c.CloudID,
		APIKey:                 c.APIKey,
		ServiceToken:           c.ServiceToken,
		CertificateFingerprint: c.CertificateFingerprint,
		Transport:              transport,
	}
	// Until ownership transfers to the returned EsqlConn, every failure path
	// must close the private transport's keep-alive sockets itself — a failed
	// connect is never admitted to the session cache, so nothing else would
	// ever release them. Registered before the body-close defer so (LIFO) the
	// response body is closed first, returning its socket to the idle pool
	// this then drains.
	owned := false
	defer func() {
		if !owned {
			transport.CloseIdleConnections()
		}
	}()
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
	owned = true
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

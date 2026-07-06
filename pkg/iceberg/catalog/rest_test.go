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

package catalog

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestRESTClientGetConfigNegotiatesCapabilitiesAndCaches(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/config" || r.URL.Query().Get("warehouse") != "warehouse_a" {
			t.Fatalf("unexpected config request: %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"defaults": {"prefix": "warehouse_a"},
			"overrides": {"scan-planning-mode": "server"},
			"endpoints": [
				"GET /v1/{prefix}/namespaces",
				"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
				"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign",
				"POST /v1/{prefix}/namespaces/{namespace}/tables",
				"POST /v1/{prefix}/transactions/commit"
			]
		}`))
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	req := api.GetConfigRequest{CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL)}, Warehouse: "warehouse_a"}
	resp, err := client.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("get config: %v", err)
	}
	if resp.Prefix != "warehouse_a" || !resp.Capabilities.CredentialVending || !resp.Capabilities.RemoteSigning || !resp.Capabilities.ServerSidePlanning || !resp.Capabilities.Commit || !resp.Capabilities.CreateTable {
		t.Fatalf("unexpected config response: %+v", resp)
	}
	cached, err := client.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("get cached config: %v", err)
	}
	if !cached.Cached || calls.Load() != 1 {
		t.Fatalf("expected cached response, cached=%v calls=%d", cached.Cached, calls.Load())
	}
	cached.Defaults["prefix"] = "mutated"
	cachedAgain, err := client.GetConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("get cached config again: %v", err)
	}
	if cachedAgain.Defaults["prefix"] != "warehouse_a" {
		t.Fatalf("cached config map should be cloned, got %+v", cachedAgain.Defaults)
	}
}

func TestRESTClientDecodesConfigPrefixBeforeFollowupRequests(t *testing.T) {
	var credentialsPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/config":
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"main%7Cs3%3A%2F%2Fwarehouse"},"overrides":{}}`))
		case strings.HasSuffix(r.URL.Path, "/credentials"):
			credentialsPath = r.URL.EscapedPath()
			_, _ = w.Write([]byte(`{"storage-credentials":[]}`))
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	resp, err := client.GetConfig(context.Background(), api.GetConfigRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL)},
		Warehouse:      "s3://warehouse",
		NoCache:        true,
	})
	if err != nil {
		t.Fatalf("get config: %v", err)
	}
	if resp.Prefix != "main|s3://warehouse" {
		t.Fatalf("expected decoded prefix, got %q", resp.Prefix)
	}
	_, err = client.LoadCredentials(context.Background(), api.LoadCredentialsRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: resp.Prefix},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
	})
	if err != nil {
		t.Fatalf("load credentials: %v", err)
	}
	want := "/v1/main%7Cs3:%2F%2Fwarehouse/namespaces/sales/tables/orders/credentials"
	if credentialsPath != want {
		t.Fatalf("expected single-escaped prefix path %q, got %q", want, credentialsPath)
	}
}

func TestNegotiateCapabilitiesDetectsCreateTableOnlyForNamespaceTablesEndpoint(t *testing.T) {
	caps := negotiateCapabilities(nil, nil, []string{
		"POST /v1/{prefix}/namespaces/{namespace}/tables",
		"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	})
	if !caps.CreateTable || !caps.Commit {
		t.Fatalf("expected create-table and commit capabilities: %+v", caps)
	}
	commitOnly := negotiateCapabilities(nil, nil, []string{
		"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	})
	if commitOnly.CreateTable || !commitOnly.Commit {
		t.Fatalf("table commit endpoint should not imply create-table: %+v", commitOnly)
	}
	fromConfig := negotiateCapabilities(map[string]string{"table-create-enabled": "true"}, nil, nil)
	if !fromConfig.CreateTable {
		t.Fatalf("expected create-table from config flag: %+v", fromConfig)
	}
}

func TestRESTClientRejectsPlainHTTPByDefault(t *testing.T) {
	client := NewRESTClient()
	_, err := client.GetConfig(context.Background(), api.GetConfigRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog("http://catalog.example.com")},
	})
	if err == nil || !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("expected plain HTTP rejection, got %v", err)
	}
}

func TestRESTClientDefaultTimeout(t *testing.T) {
	client := NewRESTClient()
	req := client.normalizeRequest(api.CatalogRequest{})
	if req.Timeout != defaultRESTTimeout {
		t.Fatalf("expected default timeout %s, got %s", defaultRESTTimeout, req.Timeout)
	}

	client = NewRESTClient(WithDefaultTimeout(2 * time.Second))
	req = client.normalizeRequest(api.CatalogRequest{})
	if req.Timeout != 2*time.Second {
		t.Fatalf("expected custom default timeout, got %s", req.Timeout)
	}

	client = NewRESTClient(WithDefaultTimeout(0))
	req = client.normalizeRequest(api.CatalogRequest{})
	if req.Timeout != defaultRESTTimeout {
		t.Fatalf("expected non-positive custom timeout to fall back to %s, got %s", defaultRESTTimeout, req.Timeout)
	}
	req = client.normalizeRequest(api.CatalogRequest{Timeout: -time.Second})
	if req.Timeout != defaultRESTTimeout {
		t.Fatalf("expected negative request timeout to fall back to %s, got %s", defaultRESTTimeout, req.Timeout)
	}

	req = client.normalizeRequest(api.CatalogRequest{Timeout: time.Second})
	if req.Timeout != time.Second {
		t.Fatalf("request timeout should override default, got %s", req.Timeout)
	}
}

func TestRESTClientResidencyValidatorRunsBeforeTokenAndHTTP(t *testing.T) {
	var serverCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalls.Add(1)
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer server.Close()

	var tokenCalls atomic.Int32
	client := NewRESTClient(
		WithHTTPClient(server.Client()),
		WithAllowPlainHTTP(true),
		WithTokenProvider(countingTokenProvider{calls: &tokenCalls}),
		WithResidencyValidator(func(ctx context.Context, req api.CatalogRequest) error {
			if req.Catalog.AccountID != 7 || req.Catalog.CatalogID != 42 {
				t.Fatalf("unexpected catalog in residency validator: %+v", req.Catalog)
			}
			return api.NewError(api.ErrResidencyDenied, "catalog endpoint is not allowed", map[string]string{
				"catalog_uri": req.Catalog.URI,
			})
		}),
	)
	catalog := testCatalog(server.URL)
	catalog.AccountID = 7
	catalog.CatalogID = 42
	catalog.TokenSecretRef = "secret://catalog"
	_, err := client.GetConfig(context.Background(), api.GetConfigRequest{
		CatalogRequest: api.CatalogRequest{Catalog: catalog},
	})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrResidencyDenied)) {
		t.Fatalf("expected residency denial, got %v", err)
	}
	if tokenCalls.Load() != 0 {
		t.Fatalf("residency denial should happen before token resolution, token calls=%d", tokenCalls.Load())
	}
	if serverCalls.Load() != 0 {
		t.Fatalf("residency denial should happen before HTTP request, server calls=%d", serverCalls.Load())
	}
}

func TestRESTClientListPagination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/warehouse_a/namespaces":
			if r.URL.Query().Get("pageToken") == "" {
				_, _ = w.Write([]byte(`{"namespaces":[["sales"]],"next-page-token":"p2"}`))
				return
			}
			_, _ = w.Write([]byte(`{"namespaces":[["finance"]]}`))
		case "/v1/warehouse_a/namespaces/sales/tables":
			if r.URL.Query().Get("pageToken") == "" {
				_, _ = w.Write([]byte(`{"identifiers":[{"namespace":["sales"],"name":"orders"}],"next-page-token":"p2"}`))
				return
			}
			_, _ = w.Write([]byte(`{"identifiers":[{"namespace":["sales"],"name":"customers"}]}`))
		default:
			t.Fatalf("unexpected list path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	req := api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"}
	namespaces, err := client.ListNamespaces(context.Background(), api.ListNamespacesRequest{CatalogRequest: req, PageSize: 1})
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	if len(namespaces.Namespaces) != 2 || namespaces.Namespaces[1][0] != "finance" {
		t.Fatalf("unexpected namespaces: %+v", namespaces)
	}
	tables, err := client.ListTables(context.Background(), api.ListTablesRequest{CatalogRequest: req, Namespace: api.Namespace{"sales"}, PageSize: 1})
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(tables.Identifiers) != 2 || tables.Identifiers[1].Name != "customers" {
		t.Fatalf("unexpected tables: %+v", tables)
	}
}

func TestRESTClientLoadTableParsesTokenCredentialsAndHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/warehouse_a/namespaces/sales/tables/orders" {
			t.Fatalf("unexpected load table path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer catalog-token" {
			t.Fatalf("missing catalog bearer token: %s", r.Header.Get("Authorization"))
		}
		if r.Header.Get(accessDelegationHeader) != "vended-credentials,remote-signing" {
			t.Fatalf("missing access delegation header: %s", r.Header.Get(accessDelegationHeader))
		}
		w.Header().Set("ETag", "etag-1")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"metadata-location":"s3://warehouse/sales/orders/metadata/v1.json",
			"metadata":{"format-version":2,"table-uuid":"uuid-1"},
			"config":{
				"token":"table-token",
				"s3.access-key-id":"AKIA",
				"s3.secret-access-key":"secret",
				"s3.remote-signing-enabled":"true"
			},
			"storage-credentials":[
				{"prefix":"s3://warehouse/sales/orders","config":{"s3.session-token":"session"}}
			]
		}`))
	}))
	defer server.Close()

	client := NewRESTClient(
		WithHTTPClient(server.Client()),
		WithAllowPlainHTTP(true),
		WithTokenProvider(StaticTokenProvider{Tokens: map[string]string{"secret://catalog": "catalog-token"}}),
	)
	catalog := testCatalog(server.URL)
	catalog.TokenSecretRef = "secret://catalog"
	resp, err := client.LoadTable(context.Background(), api.LoadTableRequest{
		CatalogRequest: api.CatalogRequest{Catalog: catalog, Prefix: "warehouse_a"},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		AccessDelegation: []string{
			"vended-credentials",
			"remote-signing",
		},
	})
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	if resp.MetadataLocation == "" || resp.TableToken != "table-token" || resp.ETag != "etag-1" {
		t.Fatalf("unexpected load table response: %+v", resp)
	}
	if resp.MetadataLocationRed == "" || strings.Contains(resp.MetadataLocationRed, "warehouse") || strings.Contains(resp.MetadataLocationRed, "orders") {
		t.Fatalf("metadata location redaction leaked path: %q", resp.MetadataLocationRed)
	}
	if len(resp.StorageCredentials) != 2 {
		t.Fatalf("expected vended and config credentials: %+v", resp.StorageCredentials)
	}
	if !resp.Capabilities.RemoteSigning || !resp.Capabilities.CredentialVending {
		t.Fatalf("expected credential capabilities: %+v", resp.Capabilities)
	}
}

func TestRESTClientLoadCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/warehouse_a/namespaces/sales/tables/orders/credentials" || r.URL.Query().Get("planId") != "plan-1" {
			t.Fatalf("unexpected credentials request: %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"storage-credentials":[{"prefix":"s3://warehouse","config":{"s3.session-token":"session"}}]}`))
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	resp, err := client.LoadCredentials(context.Background(), api.LoadCredentialsRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		PlanID:         "plan-1",
	})
	if err != nil {
		t.Fatalf("load credentials: %v", err)
	}
	if len(resp.StorageCredentials) != 1 || resp.StorageCredentials[0].Config["s3.session-token"] != "session" {
		t.Fatalf("unexpected credentials: %+v", resp.StorageCredentials)
	}
}

func TestRESTClientRemoteSignerSignsS3Request(t *testing.T) {
	var seen remoteSignRequestWire
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/iceberg/sign" || r.Method != http.MethodPost {
			t.Fatalf("unexpected signer request: %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Fatalf("decode signer request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"uri":"http://127.0.0.1:9000/warehouse/sales/orders/data.parquet",
			"headers":{
				"Authorization":["AWS4-HMAC-SHA256 signed"],
				"Host":["127.0.0.1:9000"],
				"X-Amz-Date":["20260626T095748Z"]
			}
		}`))
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	signer := client.NewRemoteSigner(api.CatalogRequest{}, map[string]string{
		"s3.signer.uri":             server.URL + "/iceberg",
		"s3.signer.endpoint":        "sign",
		"s3.endpoint":               "http://127.0.0.1:9000",
		"s3.path-style-access":      "true",
		"client.region":             "us-east-1",
		"s3.remote-signing-enabled": "true",
	})
	signed, err := signer.Sign(context.Background(), http.MethodGet, "s3://warehouse/sales/orders/data.parquet")
	if err != nil {
		t.Fatalf("sign request: %v", err)
	}
	if seen.Method != http.MethodGet || seen.Region != "us-east-1" || seen.URI != "http://127.0.0.1:9000/warehouse/sales/orders/data.parquet" {
		t.Fatalf("unexpected signer body: %+v", seen)
	}
	if signed.URL != "http://127.0.0.1:9000/warehouse/sales/orders/data.parquet" ||
		signed.Headers["Authorization"] != "AWS4-HMAC-SHA256 signed" ||
		signed.Headers["Host"] != "127.0.0.1:9000" ||
		signed.ExpiresAt.IsZero() {
		t.Fatalf("unexpected signed response: %+v", signed)
	}
}

func TestRESTClientCreateTable(t *testing.T) {
	var gotBody createTableRequestWire
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/warehouse_a/namespaces/sales/tables" {
			t.Fatalf("unexpected create table path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read create table body: %v", err)
		}
		bodyText := string(body)
		if strings.Contains(bodyText, `"Kind"`) || !strings.Contains(bodyText, `"type":"long"`) {
			t.Fatalf("create table body used non-Iceberg schema encoding: %s", bodyText)
		}
		if !strings.Contains(bodyText, `"schema-id":0`) || !strings.Contains(bodyText, `"spec-id":0`) {
			t.Fatalf("create table body omitted required zero ids: %s", bodyText)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode create table body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata-location":"s3://warehouse/sales/orders/metadata/v1.json","config":{"table-uuid":"uuid-1"}}`))
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	resp, err := client.CreateTable(context.Background(), api.CreateTableRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Location:       "s3://warehouse/sales/orders",
		Schema: api.Schema{SchemaID: 0, Fields: []api.SchemaField{{
			ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong},
		}}},
		PartitionSpec: api.PartitionSpec{SpecID: 0},
		Properties:    map[string]string{"owner": "matrixone"},
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	if gotBody.Name != "orders" || gotBody.Schema.Fields[0].Type != "long" || gotBody.Location == "" {
		t.Fatalf("unexpected create table request: %+v", gotBody)
	}
	if resp.TableUUID != "uuid-1" || resp.MetadataLocationHash == "" {
		t.Fatalf("unexpected create table response: %+v", resp)
	}
}

func TestRESTClientCommitTable(t *testing.T) {
	var gotBody commitTableRequestWire
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/warehouse_a/namespaces/sales/tables/orders" {
			t.Fatalf("unexpected commit path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Idempotency-Key") != "idem-1" {
			t.Fatalf("missing idempotency key: %s", r.Header.Get("Idempotency-Key"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read commit body: %v", err)
		}
		bodyText := string(body)
		if !strings.Contains(bodyText, `"snapshot-id"`) || !strings.Contains(bodyText, `"file-path"`) {
			t.Fatalf("commit body used non-Iceberg field names: %s", bodyText)
		}
		if !strings.Contains(bodyText, `"current-schema-id":0`) || !strings.Contains(bodyText, `"default-spec-id":0`) || !strings.Contains(bodyText, `"default-sort-order-id":0`) {
			t.Fatalf("commit body missed REST requirement ids: %s", bodyText)
		}
		if strings.Contains(bodyText, `"schema-id":`) || strings.Contains(bodyText, `"spec-id":`) || strings.Contains(bodyText, `"sort-order-id":`) {
			t.Fatalf("commit body used snapshot field names for requirements: %s", bodyText)
		}
		if strings.Contains(bodyText, `"SnapshotID"`) || strings.Contains(bodyText, `"FilePath"`) {
			t.Fatalf("commit body leaked Go field names: %s", bodyText)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode commit body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Iceberg-Commit-ID", "commit-1")
		_, _ = w.Write([]byte(`{"metadata-location":"s3://warehouse/sales/orders/metadata/v2.json","snapshot-id":200}`))
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	resp, err := client.CommitTable(context.Background(), api.CommitRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		IdempotencyKey: "idem-1",
		Requirements: []api.CommitRequirement{{
			Type:       "assert-ref-snapshot-id",
			Ref:        "main",
			SnapshotID: 100,
		}, {
			Type:      "assert-table-uuid",
			TableUUID: "uuid-1",
		}, {
			Type:     "assert-current-schema-id",
			SchemaID: 0,
		}, {
			Type:   "assert-default-spec-id",
			SpecID: 0,
		}, {
			Type:        "assert-default-sort-order-id",
			SortOrderID: 0,
		}},
		Updates: []api.CommitUpdate{{
			Type:     "append",
			FilePath: "s3://warehouse/sales/orders/metadata/manifest.avro",
		}},
	})
	if err != nil {
		t.Fatalf("commit table: %v", err)
	}
	if resp.SnapshotID != 200 || resp.CommitID != "commit-1" || resp.MetadataLocationHash == "" {
		t.Fatalf("unexpected commit response: %+v", resp)
	}
	if gotBody.Identifier.Name != "orders" || len(gotBody.Identifier.Namespace) != 1 || gotBody.Identifier.Namespace[0] != "sales" {
		t.Fatalf("unexpected commit identifier: %+v", gotBody.Identifier)
	}
	if len(gotBody.Requirements) != 5 || gotBody.Requirements[0].Type != "assert-ref-snapshot-id" {
		t.Fatalf("unexpected requirements: %+v", gotBody.Requirements)
	}
	if gotBody.Requirements[2].CurrentSchemaID == nil || *gotBody.Requirements[2].CurrentSchemaID != 0 {
		t.Fatalf("current schema id 0 must be preserved: %+v", gotBody.Requirements[2])
	}
	if gotBody.Requirements[3].DefaultSpecID == nil || *gotBody.Requirements[3].DefaultSpecID != 0 {
		t.Fatalf("default spec id 0 must be preserved: %+v", gotBody.Requirements[3])
	}
	if gotBody.Requirements[4].DefaultSortOrderID == nil || *gotBody.Requirements[4].DefaultSortOrderID != 0 {
		t.Fatalf("default sort order id 0 must be preserved: %+v", gotBody.Requirements[4])
	}
	if len(gotBody.Updates) != 1 || gotBody.Updates[0].FilePath == "" {
		t.Fatalf("unexpected updates: %+v", gotBody.Updates)
	}
}

func TestRESTClientCommitTableSerializesSnapshotUpdates(t *testing.T) {
	var gotBody commitTableRequestWire
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read commit body: %v", err)
		}
		bodyText := string(body)
		if strings.Contains(bodyText, `"payload"`) || strings.Contains(bodyText, `"set-manifest-list"`) || strings.Contains(bodyText, `"add-manifest"`) {
			t.Fatalf("commit body leaked internal update fields: %s", bodyText)
		}
		if !strings.Contains(bodyText, `"snapshot"`) || !strings.Contains(bodyText, `"parent-snapshot-id"`) || !strings.Contains(bodyText, `"sequence-number"`) || !strings.Contains(bodyText, `"schema-id"`) {
			t.Fatalf("commit body missed snapshot fields: %s", bodyText)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode commit body: %v", err)
		}
		_, _ = w.Write([]byte(`{"metadata-location":"s3://warehouse/sales/orders/metadata/v2.json","snapshot-id":200}`))
	}))
	defer server.Close()

	snapshot := api.NewCommitSnapshot(
		200,
		100,
		9,
		0,
		123456,
		"s3://warehouse/sales/orders/metadata/snap-200.avro",
		map[string]string{"operation": "append"},
	)
	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
	_, err := client.CommitTable(context.Background(), api.CommitRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
		Updates: []api.CommitUpdate{
			api.NewAddSnapshotUpdate(snapshot),
			api.NewSetSnapshotRefUpdate("main", "branch", 200),
		},
	})
	if err != nil {
		t.Fatalf("commit table: %v", err)
	}
	if len(gotBody.Updates) != 2 {
		t.Fatalf("unexpected updates: %+v", gotBody.Updates)
	}
	if gotBody.Updates[0].Action != "add-snapshot" || gotBody.Updates[0].Snapshot == nil {
		t.Fatalf("unexpected add-snapshot update: %+v", gotBody.Updates[0])
	}
	if gotBody.Updates[0].Snapshot.SchemaID == nil || *gotBody.Updates[0].Snapshot.SchemaID != 0 {
		t.Fatalf("schema-id 0 must be preserved: %+v", gotBody.Updates[0].Snapshot)
	}
	if gotBody.Updates[1].Action != "set-snapshot-ref" || gotBody.Updates[1].RefName != "main" || gotBody.Updates[1].RefType != "branch" || gotBody.Updates[1].SnapshotID != 200 {
		t.Fatalf("unexpected set-snapshot-ref update: %+v", gotBody.Updates[1])
	}
}

func TestRESTClientCommitConflictMaps409(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"message":"snapshot changed","type":"CommitFailedException","code":409}}`))
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true), WithRetryPolicy(api.RetryPolicy{MaxAttempts: 3}))
	_, err := client.CommitTable(context.Background(), api.CommitRequest{
		CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"},
		Namespace:      api.Namespace{"sales"},
		Table:          "orders",
	})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrCommitConflict)) {
		t.Fatalf("expected commit conflict, got %v", err)
	}
}

func TestRESTClientMaxBodyBytes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true), WithMaxBodyBytes(8))
	_, err := client.GetConfig(context.Background(), api.GetConfigRequest{CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL)}})
	if err == nil || !strings.Contains(err.Error(), "response body is too large") {
		t.Fatalf("expected body size error, got %v", err)
	}
}

func TestRESTClientErrorMappingAndRetry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if call == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"message":"busy","type":"ServiceUnavailableException","code":503}}`))
			return
		}
		_, _ = w.Write([]byte(`{"namespaces":[["sales"]]}`))
	}))
	defer server.Close()

	client := NewRESTClient(
		WithHTTPClient(server.Client()),
		WithAllowPlainHTTP(true),
		WithRetryPolicy(api.RetryPolicy{MaxAttempts: 2}),
		WithRetrySleep(func(context.Context, time.Duration) error { return nil }),
	)
	resp, err := client.ListNamespaces(context.Background(), api.ListNamespacesRequest{CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL)}})
	if err != nil {
		t.Fatalf("expected retry success: %v", err)
	}
	if len(resp.Namespaces) != 1 || calls.Load() != 2 {
		t.Fatalf("unexpected retry result: %+v calls=%d", resp, calls.Load())
	}
}

func TestRESTClientRetriesGetConfigAndLoadTable(t *testing.T) {
	t.Run("get config", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			call := calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if call == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":{"message":"busy","type":"ServiceUnavailableException","code":503}}`))
				return
			}
			if r.URL.Path != "/v1/config" {
				t.Fatalf("unexpected config retry path: %s", r.URL.Path)
			}
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse_a"},"overrides":{}}`))
		}))
		defer server.Close()

		client := NewRESTClient(
			WithHTTPClient(server.Client()),
			WithAllowPlainHTTP(true),
			WithRetryPolicy(api.RetryPolicy{MaxAttempts: 2}),
			WithRetrySleep(func(context.Context, time.Duration) error { return nil }),
		)
		resp, err := client.GetConfig(context.Background(), api.GetConfigRequest{
			CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL)},
			NoCache:        true,
		})
		if err != nil {
			t.Fatalf("expected get config retry success: %v", err)
		}
		if resp.Prefix != "warehouse_a" || calls.Load() != 2 {
			t.Fatalf("unexpected get config retry result: %+v calls=%d", resp, calls.Load())
		}
	})

	t.Run("load table", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			call := calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if call == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":{"message":"busy","type":"ServiceUnavailableException","code":503}}`))
				return
			}
			if r.URL.Path != "/v1/warehouse_a/namespaces/sales/tables/orders" {
				t.Fatalf("unexpected load retry path: %s", r.URL.Path)
			}
			_, _ = w.Write([]byte(`{
				"metadata-location":"s3://warehouse/sales/orders/metadata/v2.json",
				"metadata":{"format-version":2,"table-uuid":"uuid-2"}
			}`))
		}))
		defer server.Close()

		client := NewRESTClient(
			WithHTTPClient(server.Client()),
			WithAllowPlainHTTP(true),
			WithRetryPolicy(api.RetryPolicy{MaxAttempts: 2}),
			WithRetrySleep(func(context.Context, time.Duration) error { return nil }),
		)
		resp, err := client.LoadTable(context.Background(), api.LoadTableRequest{
			CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL), Prefix: "warehouse_a"},
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
		})
		if err != nil {
			t.Fatalf("expected load table retry success: %v", err)
		}
		if resp.MetadataLocation == "" || calls.Load() != 2 {
			t.Fatalf("unexpected load retry result: %+v calls=%d", resp, calls.Load())
		}
	})
}

func TestRESTClientAuthDoesNotRetryWithoutRefresh(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"bad token","type":"UnauthorizedException","code":401}}`))
	}))
	defer server.Close()

	client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true), WithRetryPolicy(api.RetryPolicy{MaxAttempts: 3}))
	_, err := client.GetConfig(context.Background(), api.GetConfigRequest{CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL)}})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrAuthUnauthorized)) {
		t.Fatalf("expected unauthorized error, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("401 should not retry without refresh, calls=%d", calls.Load())
	}
}

func TestRESTClientAuthRefreshRetriesOnce(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"expired","type":"UnauthorizedException","code":401}}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer refreshed-token" {
			t.Fatalf("expected refreshed bearer token, got %s", r.Header.Get("Authorization"))
		}
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer server.Close()

	client := NewRESTClient(
		WithHTTPClient(server.Client()),
		WithAllowPlainHTTP(true),
		WithTokenProvider(&refreshTokenProvider{}),
		WithRetryPolicy(api.RetryPolicy{MaxAttempts: 1}),
	)
	catalog := testCatalog(server.URL)
	catalog.TokenSecretRef = "secret://catalog"
	_, err := client.GetConfig(context.Background(), api.GetConfigRequest{CatalogRequest: api.CatalogRequest{Catalog: catalog}})
	if err != nil {
		t.Fatalf("expected refresh success: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected one refresh retry, calls=%d", calls.Load())
	}
}

func TestRESTClientMaps419And404(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   api.ErrorCode
	}{
		{status: 419, code: api.ErrAuthTimeout},
		{status: http.StatusNotFound, code: api.ErrTableNotFound},
		{status: http.StatusForbidden, code: api.ErrAuthForbidden},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(`{"error":{"message":"mapped","type":"MappedException","code":` + strconv.Itoa(tc.status) + `}}`))
		}))
		client := NewRESTClient(WithHTTPClient(server.Client()), WithAllowPlainHTTP(true))
		_, err := client.LoadTable(context.Background(), api.LoadTableRequest{
			CatalogRequest: api.CatalogRequest{Catalog: testCatalog(server.URL)},
			Namespace:      api.Namespace{"sales"},
			Table:          "orders",
		})
		server.Close()
		if err == nil || !strings.Contains(err.Error(), string(tc.code)) {
			t.Fatalf("status %d expected %s, got %v", tc.status, tc.code, err)
		}
	}
}

func testCatalog(uri string) model.Catalog {
	return model.Catalog{
		AccountID: 1,
		CatalogID: 2,
		Name:      "test",
		Type:      "rest",
		URI:       uri,
	}
}

type refreshTokenProvider struct{}

func (p *refreshTokenProvider) ResolveToken(ctx context.Context, catalog model.Catalog) (string, error) {
	return "expired-token", nil
}

func (p *refreshTokenProvider) RefreshToken(ctx context.Context, req api.CatalogRequest, previousToken string) (string, bool, error) {
	return "refreshed-token", true, nil
}

type countingTokenProvider struct {
	calls *atomic.Int32
}

func (p countingTokenProvider) ResolveToken(ctx context.Context, catalog model.Catalog) (string, error) {
	p.calls.Add(1)
	return "token", nil
}

func (p countingTokenProvider) RefreshToken(ctx context.Context, req api.CatalogRequest, previousToken string) (string, bool, error) {
	return "", false, nil
}

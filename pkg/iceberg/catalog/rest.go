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
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

const (
	AdapterNativeREST = "native-rest"

	accessDelegationHeader  = "X-Iceberg-Access-Delegation"
	externalPrincipalHeader = "X-Iceberg-Principal"
	requestIDHeader         = "X-Request-ID"
	defaultNamespaceSep     = "\x1f"
	defaultRESTTimeout      = 30 * time.Second
	defaultMaxBodyBytes     = 32 << 20
)

type TokenProvider interface {
	ResolveToken(ctx context.Context, catalog model.Catalog) (string, error)
	RefreshToken(ctx context.Context, req api.CatalogRequest, previousToken string) (string, bool, error)
}

type StaticTokenProvider struct {
	Tokens map[string]string
}

func (p StaticTokenProvider) ResolveToken(ctx context.Context, catalog model.Catalog) (string, error) {
	if len(p.Tokens) == 0 {
		return "", api.NewError(api.ErrAuthUnauthorized, "Iceberg REST static token provider has no tokens", map[string]string{"catalog": catalog.Name})
	}
	token := p.Tokens[catalog.TokenSecretRef]
	if strings.TrimSpace(token) == "" {
		return "", api.NewError(api.ErrAuthUnauthorized, "Iceberg REST secret ref is not available", map[string]string{
			"catalog":          catalog.Name,
			"token_secret_ref": catalog.TokenSecretRef,
		})
	}
	return token, nil
}

func (p StaticTokenProvider) RefreshToken(ctx context.Context, req api.CatalogRequest, previousToken string) (string, bool, error) {
	return "", false, nil
}

type RESTClientOption func(*RESTClient)

type CatalogResidencyValidator func(ctx context.Context, req api.CatalogRequest) error

type RESTClient struct {
	httpClient         *http.Client
	tokenProvider      TokenProvider
	residencyValidator CatalogResidencyValidator
	cacheTTL           time.Duration
	sleep              func(context.Context, time.Duration) error
	configCacheMu      sync.Mutex
	configCache        map[string]configCacheEntry
	defaultRetry       api.RetryPolicy
	defaultTimeout     time.Duration
	allowPlainHTTP     bool
	userAgent          string
	namespaceSep       string
	maxBodyBytes       int64
}

type configCacheEntry struct {
	expiresAt time.Time
	response  api.ConfigResponse
}

func NewRESTClient(opts ...RESTClientOption) *RESTClient {
	c := &RESTClient{
		httpClient:     http.DefaultClient,
		cacheTTL:       5 * time.Minute,
		configCache:    make(map[string]configCacheEntry),
		defaultRetry:   api.RetryPolicy{MaxAttempts: 3, BaseBackoff: 10 * time.Millisecond, MaxBackoff: 200 * time.Millisecond},
		defaultTimeout: defaultRESTTimeout,
		userAgent:      "matrixone-iceberg-connector",
		namespaceSep:   defaultNamespaceSep,
		maxBodyBytes:   defaultMaxBodyBytes,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}
	if c.configCache == nil {
		c.configCache = make(map[string]configCacheEntry)
	}
	if c.sleep == nil {
		c.sleep = sleepContext
	}
	if c.namespaceSep == "" {
		c.namespaceSep = defaultNamespaceSep
	}
	if c.defaultTimeout <= 0 {
		c.defaultTimeout = defaultRESTTimeout
	}
	if c.maxBodyBytes <= 0 {
		c.maxBodyBytes = defaultMaxBodyBytes
	}
	return c
}

func WithHTTPClient(httpClient *http.Client) RESTClientOption {
	return func(c *RESTClient) {
		c.httpClient = httpClient
	}
}

func WithTokenProvider(provider TokenProvider) RESTClientOption {
	return func(c *RESTClient) {
		c.tokenProvider = provider
	}
}

func WithResidencyValidator(validator CatalogResidencyValidator) RESTClientOption {
	return func(c *RESTClient) {
		c.residencyValidator = validator
	}
}

func WithConfigCacheTTL(ttl time.Duration) RESTClientOption {
	return func(c *RESTClient) {
		c.cacheTTL = ttl
	}
}

func WithRetryPolicy(policy api.RetryPolicy) RESTClientOption {
	return func(c *RESTClient) {
		c.defaultRetry = policy
	}
}

func WithRetrySleep(sleep func(context.Context, time.Duration) error) RESTClientOption {
	return func(c *RESTClient) {
		c.sleep = sleep
	}
}

func WithDefaultTimeout(timeout time.Duration) RESTClientOption {
	return func(c *RESTClient) {
		c.defaultTimeout = timeout
	}
}

func WithAllowPlainHTTP(allow bool) RESTClientOption {
	return func(c *RESTClient) {
		c.allowPlainHTTP = allow
	}
}

func WithMaxBodyBytes(maxBytes int64) RESTClientOption {
	return func(c *RESTClient) {
		c.maxBodyBytes = maxBytes
	}
}

func WithUserAgent(userAgent string) RESTClientOption {
	return func(c *RESTClient) {
		c.userAgent = userAgent
	}
}

func (c *RESTClient) AdapterName() string {
	return AdapterNativeREST
}

func (c *RESTClient) GetConfig(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	cacheKey := c.configCacheKey(req)
	if !req.NoCache {
		if cached, ok := c.getConfigCache(cacheKey); ok {
			cached.Cached = true
			return &cached, nil
		}
	}
	target, err := c.configURL(ctx, req)
	if err != nil {
		return nil, err
	}
	raw, err := c.doGet(ctx, "get_config", req.CatalogRequest, target, nil)
	if err != nil {
		return nil, err
	}
	var wire configResponseWire
	if err := decodeJSON(raw.body, &wire, "get_config"); err != nil {
		return nil, err
	}
	resp := api.ConfigResponse{
		Defaults:               cloneStringMap(wire.Defaults),
		Overrides:              cloneStringMap(wire.Overrides),
		Endpoints:              append([]string(nil), wire.Endpoints...),
		IdempotencyKeyLifetime: wire.IdempotencyKeyLifetime,
		Prefix:                 normalizeCatalogPrefix(firstNonEmpty(wire.Overrides["prefix"], wire.Defaults["prefix"])),
	}
	resp.Capabilities = negotiateCapabilities(resp.Defaults, resp.Overrides, resp.Endpoints)
	if !req.NoCache {
		c.putConfigCache(cacheKey, resp)
	}
	return &resp, nil
}

func (c *RESTClient) ListNamespaces(ctx context.Context, req api.ListNamespacesRequest) (*api.ListNamespacesResponse, error) {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	maxPages := normalizeMaxPages(req.MaxPages)
	var out api.ListNamespacesResponse
	pageToken := ""
	for page := 0; page < maxPages; page++ {
		target, err := c.namespacesURL(ctx, req, pageToken)
		if err != nil {
			return nil, err
		}
		raw, err := c.doGet(ctx, "list_namespaces", req.CatalogRequest, target, nil)
		if err != nil {
			return nil, err
		}
		var wire listNamespacesWire
		if err := decodeJSON(raw.body, &wire, "list_namespaces"); err != nil {
			return nil, err
		}
		for _, ns := range wire.Namespaces {
			out.Namespaces = append(out.Namespaces, api.Namespace(ns))
		}
		out.NextPageToken = wire.NextPageToken
		if wire.NextPageToken == "" {
			return &out, nil
		}
		pageToken = wire.NextPageToken
	}
	return nil, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg REST namespace pagination exceeded max pages", map[string]string{
		"operation": "list_namespaces",
		"max_pages": strconv.Itoa(maxPages),
	})
}

func (c *RESTClient) ListTables(ctx context.Context, req api.ListTablesRequest) (*api.ListTablesResponse, error) {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	maxPages := normalizeMaxPages(req.MaxPages)
	var out api.ListTablesResponse
	pageToken := ""
	for page := 0; page < maxPages; page++ {
		target, err := c.tablesURL(ctx, req, pageToken)
		if err != nil {
			return nil, err
		}
		raw, err := c.doGet(ctx, "list_tables", req.CatalogRequest, target, nil)
		if err != nil {
			return nil, err
		}
		var wire listTablesWire
		if err := decodeJSON(raw.body, &wire, "list_tables"); err != nil {
			return nil, err
		}
		for _, ident := range wire.Identifiers {
			out.Identifiers = append(out.Identifiers, api.TableIdentifier{
				Namespace: api.Namespace(ident.Namespace),
				Name:      ident.Name,
			})
		}
		out.NextPageToken = wire.NextPageToken
		if wire.NextPageToken == "" {
			return &out, nil
		}
		pageToken = wire.NextPageToken
	}
	return nil, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg REST table pagination exceeded max pages", map[string]string{
		"operation": "list_tables",
		"max_pages": strconv.Itoa(maxPages),
	})
}

func (c *RESTClient) LoadTable(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	target, err := c.loadTableURL(ctx, req)
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	if req.IfNoneMatch != "" {
		headers.Set("If-None-Match", req.IfNoneMatch)
	}
	if len(req.AccessDelegation) > 0 {
		headers.Set(accessDelegationHeader, strings.Join(req.AccessDelegation, ","))
	}
	raw, err := c.doGet(ctx, "load_table", req.CatalogRequest, target, headers)
	if err != nil {
		return nil, err
	}
	if raw.statusCode == http.StatusNotModified {
		return &api.LoadTableResponse{Namespace: req.Namespace, TableName: req.Table, ETag: raw.headers.Get("ETag"), NotModified: true}, nil
	}
	var wire loadTableWire
	if err := decodeJSON(raw.body, &wire, "load_table"); err != nil {
		return nil, err
	}
	resp := &api.LoadTableResponse{
		Namespace:           req.Namespace,
		TableName:           req.Table,
		MetadataLocation:    wire.MetadataLocation,
		MetadataJSON:        cloneRawMessage(wire.Metadata),
		Config:              cloneStringMap(wire.Config),
		TableToken:          tableTokenFromConfig(wire.Config),
		StorageCredentials:  storageCredentialsFromWire(wire.StorageCredentials),
		Capabilities:        negotiateCapabilities(nil, wire.Config, nil),
		ETag:                raw.headers.Get("ETag"),
		MetadataLocationRed: api.RedactPath(wire.MetadataLocation),
	}
	resp.StorageCredentials = append(resp.StorageCredentials, storageCredentialsFromConfig(wire.Config)...)
	return resp, nil
}

func (c *RESTClient) LoadCredentials(ctx context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	target, err := c.credentialsURL(ctx, req)
	if err != nil {
		return nil, err
	}
	raw, err := c.doGet(ctx, "load_credentials", req.CatalogRequest, target, nil)
	if err != nil {
		return nil, err
	}
	var wire loadCredentialsWire
	if err := decodeJSON(raw.body, &wire, "load_credentials"); err != nil {
		return nil, err
	}
	return &api.LoadCredentialsResponse{StorageCredentials: storageCredentialsFromWire(wire.StorageCredentials)}, nil
}

func (c *RESTClient) CreateTable(ctx context.Context, req api.CreateTableRequest) (*api.CreateTableResponse, error) {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	target, err := c.createTableURL(ctx, req)
	if err != nil {
		return nil, err
	}
	payload := createTableRequestWire{
		Name:          req.Table,
		Schema:        schemaWireFromAPI(req.Schema),
		PartitionSpec: partitionSpecWireFromAPI(req.PartitionSpec),
		Location:      req.Location,
		Properties:    cloneStringMap(req.Properties),
		StageCreate:   req.StageCreate,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg REST create table request could not be encoded", map[string]string{"operation": "create_table"}, err)
	}
	raw, err := c.doJSON(ctx, "create_table", req.CatalogRequest, http.MethodPost, target, nil, body)
	if err != nil {
		return nil, err
	}
	var wire loadTableWire
	if len(bytes.TrimSpace(raw.body)) > 0 {
		if err := decodeJSON(raw.body, &wire, "create_table"); err != nil {
			return nil, err
		}
	}
	return &api.CreateTableResponse{
		Namespace:            append(api.Namespace(nil), req.Namespace...),
		TableName:            req.Table,
		MetadataLocation:     wire.MetadataLocation,
		MetadataLocationHash: api.PathHash(wire.MetadataLocation),
		TableUUID:            firstNonEmpty(wire.Config["table-uuid"], wire.Config["uuid"]),
		Config:               cloneStringMap(wire.Config),
		MetadataJSON:         cloneRawMessage(wire.Metadata),
		StorageCredentials:   storageCredentialsFromWire(wire.StorageCredentials),
	}, nil
}

func (c *RESTClient) CommitTable(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	target, err := c.commitTableURL(ctx, req)
	if err != nil {
		return nil, err
	}
	payload := commitTableRequestWire{
		Identifier: tableIdentifierWire{
			Namespace: []string(req.Namespace),
			Name:      req.Table,
		},
		Requirements: commitRequirementWires(req.Requirements),
		Updates:      commitUpdateWires(req.Updates),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg REST commit request could not be encoded", map[string]string{"operation": "commit_table"}, err)
	}
	headers := make(http.Header)
	if req.IdempotencyKey != "" {
		headers.Set("Idempotency-Key", req.IdempotencyKey)
	}
	raw, err := c.doJSON(ctx, "commit_table", req.CatalogRequest, http.MethodPost, target, headers, body)
	if err != nil {
		return nil, err
	}
	var wire commitTableResponseWire
	if len(bytes.TrimSpace(raw.body)) > 0 {
		if err := decodeJSON(raw.body, &wire, "commit_table"); err != nil {
			return nil, err
		}
	}
	metadataLocation := wire.MetadataLocation
	return &api.CommitResult{
		SnapshotID:           wire.SnapshotID,
		MetadataLocation:     metadataLocation,
		MetadataLocationHash: api.PathHash(metadataLocation),
		CommitID:             firstNonEmpty(raw.headers.Get("X-Iceberg-Commit-ID"), raw.headers.Get("X-Request-ID")),
	}, nil
}

func (c *RESTClient) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	target, err := c.planScanURL(ctx, req)
	if err != nil {
		return nil, err
	}
	payload := planScanRequestWire{
		Identifier: tableIdentifierWire{
			Namespace: []string(req.Namespace),
			Name:      req.Table,
		},
		Ref:                    req.Ref,
		SnapshotID:             req.Snapshot.SnapshotID,
		HasSnapshotID:          req.Snapshot.HasSnapshotID,
		TimestampMS:            req.Snapshot.TimestampMS,
		HasTimestampMS:         req.Snapshot.HasTimestampMS,
		ProjectionIDs:          append([]int(nil), req.ProjectionIDs...),
		ResidualSQL:            req.ResidualSQL,
		PrunePredicates:        prunePredicateWires(req.PrunePredicates),
		EnableRowGroupPlanning: req.EnableRowGroupPlanning,
		EnableDeleteApply:      req.EnableDeleteApply,
		DeleteMaxMemoryBytes:   req.DeleteMaxMemoryBytes,
		EnableDeleteSpill:      req.EnableDeleteSpill,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataInvalid, "Iceberg REST plan scan request could not be encoded", map[string]string{"operation": "plan_scan"}, err)
	}
	raw, err := c.doJSON(ctx, "plan_scan", req.CatalogRequest, http.MethodPost, target, nil, body)
	if err != nil {
		return nil, err
	}
	var wire planScanResponseWire
	if err := decodeJSON(raw.body, &wire, "plan_scan"); err != nil {
		return nil, err
	}
	plan := scanPlanFromWire(wire)
	plan.DeleteMaxMemoryBytes = req.DeleteMaxMemoryBytes
	plan.EnableDeleteSpill = req.EnableDeleteSpill
	return plan, nil
}

func (c *RESTClient) ReportMetrics(ctx context.Context, req api.MetricsReportRequest) error {
	req.CatalogRequest = c.normalizeRequest(req.CatalogRequest)
	target, err := c.metricsURL(ctx, req)
	if err != nil {
		return err
	}
	payload := metricsReportRequestWire{
		Identifier: tableIdentifierWire{
			Namespace: []string(req.Namespace),
			Name:      req.Table,
		},
		Ref:                  req.Ref,
		SnapshotID:           req.SnapshotID,
		QueryID:              req.QueryID,
		StatementID:          req.StatementID,
		Kind:                 req.Kind,
		PlanningProfile:      req.PlanningProfile,
		CommitID:             req.CommitID,
		MetadataLocationHash: req.MetadataLocationHash,
		Rows:                 req.Rows,
		Files:                req.Files,
		Extra:                cloneStringMap(req.Extra),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg REST metrics report request could not be encoded", map[string]string{"operation": "report_metrics"}, err)
	}
	_, err = c.doJSON(ctx, "report_metrics", req.CatalogRequest, http.MethodPost, target, nil, body)
	return err
}

func (c *RESTClient) normalizeRequest(req api.CatalogRequest) api.CatalogRequest {
	req.Retry = mergeRetryPolicy(req.Retry, c.defaultRetry).Normalize()
	if req.Timeout <= 0 {
		req.Timeout = c.defaultTimeout
	}
	return req
}

func (c *RESTClient) doGet(ctx context.Context, operation string, req api.CatalogRequest, target string, headers http.Header) (rawHTTPResponse, error) {
	return c.doJSON(ctx, operation, req, http.MethodGet, target, headers, nil)
}

func (c *RESTClient) doJSON(ctx context.Context, operation string, req api.CatalogRequest, method, target string, headers http.Header, body []byte) (rawHTTPResponse, error) {
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, req.Timeout, api.CauseForCode(api.ErrCatalogUnavailable))
		defer cancel()
	}
	if c.residencyValidator != nil {
		if err := c.residencyValidator(ctx, req); err != nil {
			return rawHTTPResponse{}, err
		}
	}
	token, err := c.resolveToken(ctx, req)
	if err != nil {
		return rawHTTPResponse{}, err
	}
	var lastErr error
	refreshTried := false
	retry := req.Retry.Normalize()
	for attempt := 1; attempt <= retry.MaxAttempts; attempt++ {
		raw, err := c.roundTrip(ctx, operation, req, method, target, headers, body, token)
		if err == nil && ((raw.statusCode >= 200 && raw.statusCode < 300) || raw.statusCode == http.StatusNotModified) {
			return raw, nil
		}
		if err == nil && raw.statusCode == http.StatusUnauthorized && !refreshTried && c.tokenProvider != nil {
			refreshTried = true
			if refreshed, ok, refreshErr := c.tokenProvider.RefreshToken(ctx, req, token); refreshErr != nil {
				return rawHTTPResponse{}, refreshErr
			} else if ok && strings.TrimSpace(refreshed) != "" && refreshed != token {
				token = refreshed
				attempt--
				continue
			}
		}
		if err == nil {
			lastErr = mapHTTPError(operation, raw.statusCode, raw.body)
			if !retryableStatus(raw.statusCode) || raw.statusCode == http.StatusUnauthorized || raw.statusCode == http.StatusForbidden || attempt == retry.MaxAttempts {
				return rawHTTPResponse{}, lastErr
			}
		} else {
			lastErr = err
			if !retryableNetworkError(err) || attempt == retry.MaxAttempts {
				return rawHTTPResponse{}, err
			}
		}
		if err := c.sleep(ctx, retryDelay(retry, attempt)); err != nil {
			return rawHTTPResponse{}, api.WrapError(api.ErrCatalogUnavailable, "Iceberg REST retry sleep interrupted", map[string]string{"operation": operation}, err)
		}
	}
	if lastErr != nil {
		return rawHTTPResponse{}, lastErr
	}
	return rawHTTPResponse{}, api.NewError(api.ErrCatalogUnavailable, "Iceberg REST request failed", map[string]string{"operation": operation})
}

func (c *RESTClient) roundTrip(ctx context.Context, operation string, req api.CatalogRequest, method, target string, headers http.Header, body []byte, token string) (rawHTTPResponse, error) {
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return rawHTTPResponse{}, api.WrapError(api.ErrConfigInvalid, "Iceberg REST request URL is invalid", map[string]string{"operation": operation, "uri": target}, err)
	}
	httpReq.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	if c.userAgent != "" {
		httpReq.Header.Set("User-Agent", c.userAgent)
	}
	if req.RequestID != "" {
		httpReq.Header.Set(requestIDHeader, req.RequestID)
	}
	if req.ExternalPrincipal != "" {
		httpReq.Header.Set(externalPrincipalHeader, req.ExternalPrincipal)
	}
	if token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+token)
	}
	for key, values := range headers {
		for _, value := range values {
			httpReq.Header.Add(key, value)
		}
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return rawHTTPResponse{}, api.WrapError(api.ErrCatalogUnavailable, "Iceberg REST request failed", map[string]string{"operation": operation, "uri": target}, err)
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, c.maxBodyBytes+1))
	if readErr != nil {
		return rawHTTPResponse{}, api.WrapError(api.ErrCatalogUnavailable, "Iceberg REST response read failed", map[string]string{"operation": operation}, readErr)
	}
	if int64(len(data)) > c.maxBodyBytes {
		return rawHTTPResponse{}, api.NewError(api.ErrCatalogUnavailable, "Iceberg REST response body is too large", map[string]string{"operation": operation})
	}
	return rawHTTPResponse{statusCode: resp.StatusCode, headers: resp.Header.Clone(), body: data}, nil
}

func (c *RESTClient) resolveToken(ctx context.Context, req api.CatalogRequest) (string, error) {
	if strings.TrimSpace(req.TableToken) != "" {
		return strings.TrimSpace(req.TableToken), nil
	}
	if strings.TrimSpace(req.Catalog.TokenSecretRef) == "" {
		return "", nil
	}
	if c.tokenProvider == nil {
		return "", api.NewError(api.ErrAuthUnauthorized, "Iceberg REST catalog token provider is not configured", map[string]string{
			"catalog":          req.Catalog.Name,
			"token_secret_ref": req.Catalog.TokenSecretRef,
		})
	}
	token, err := c.tokenProvider.ResolveToken(ctx, req.Catalog)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(token) == "" {
		return "", api.NewError(api.ErrAuthUnauthorized, "Iceberg REST catalog token provider returned empty token", map[string]string{"catalog": req.Catalog.Name})
	}
	return strings.TrimSpace(token), nil
}

func (c *RESTClient) configURL(ctx context.Context, req api.GetConfigRequest) (string, error) {
	target, err := restURL(req.Catalog.URI, []string{"config"}, c.allowPlainHTTP)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", api.WrapError(api.ErrConfigInvalid, "Iceberg REST config URL is invalid", map[string]string{"uri": target}, err)
	}
	q := u.Query()
	warehouse := firstNonEmpty(req.Warehouse, req.Catalog.Warehouse)
	if warehouse != "" {
		q.Set("warehouse", warehouse)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *RESTClient) namespacesURL(ctx context.Context, req api.ListNamespacesRequest, pageToken string) (string, error) {
	segments := c.pathPrefix(req.CatalogRequest)
	segments = append(segments, "namespaces")
	target, err := restURL(req.Catalog.URI, segments, c.allowPlainHTTP)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", api.WrapError(api.ErrConfigInvalid, "Iceberg REST namespaces URL is invalid", map[string]string{"uri": target}, err)
	}
	q := u.Query()
	if len(req.Parent) > 0 {
		q.Set("parent", namespacePath(req.Parent, c.namespaceSep))
	}
	if req.PageSize > 0 {
		q.Set("pageSize", strconv.Itoa(req.PageSize))
	}
	if pageToken != "" {
		q.Set("pageToken", pageToken)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *RESTClient) tablesURL(ctx context.Context, req api.ListTablesRequest, pageToken string) (string, error) {
	segments := c.pathPrefix(req.CatalogRequest)
	segments = append(segments, "namespaces", namespacePath(req.Namespace, c.namespaceSep), "tables")
	target, err := restURL(req.Catalog.URI, segments, c.allowPlainHTTP)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", api.WrapError(api.ErrConfigInvalid, "Iceberg REST tables URL is invalid", map[string]string{"uri": target}, err)
	}
	q := u.Query()
	if req.PageSize > 0 {
		q.Set("pageSize", strconv.Itoa(req.PageSize))
	}
	if pageToken != "" {
		q.Set("pageToken", pageToken)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *RESTClient) loadTableURL(ctx context.Context, req api.LoadTableRequest) (string, error) {
	segments := c.pathPrefix(req.CatalogRequest)
	segments = append(segments, "namespaces", namespacePath(req.Namespace, c.namespaceSep), "tables", req.Table)
	target, err := restURL(req.Catalog.URI, segments, c.allowPlainHTTP)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", api.WrapError(api.ErrConfigInvalid, "Iceberg REST load table URL is invalid", map[string]string{"uri": target}, err)
	}
	q := u.Query()
	if req.Snapshots != "" {
		q.Set("snapshots", req.Snapshots)
	}
	if len(req.ReferencedBy) > 0 {
		q.Set("referenced-by", strings.Join(req.ReferencedBy, ","))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *RESTClient) credentialsURL(ctx context.Context, req api.LoadCredentialsRequest) (string, error) {
	segments := c.pathPrefix(req.CatalogRequest)
	segments = append(segments, "namespaces", namespacePath(req.Namespace, c.namespaceSep), "tables", req.Table, "credentials")
	target, err := restURL(req.Catalog.URI, segments, c.allowPlainHTTP)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", api.WrapError(api.ErrConfigInvalid, "Iceberg REST credentials URL is invalid", map[string]string{"uri": target}, err)
	}
	q := u.Query()
	if req.PlanID != "" {
		q.Set("planId", req.PlanID)
	}
	if len(req.ReferencedBy) > 0 {
		q.Set("referenced-by", strings.Join(req.ReferencedBy, ","))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *RESTClient) createTableURL(ctx context.Context, req api.CreateTableRequest) (string, error) {
	segments := c.pathPrefix(req.CatalogRequest)
	segments = append(segments, "namespaces", namespacePath(req.Namespace, c.namespaceSep), "tables")
	return restURL(req.Catalog.URI, segments, c.allowPlainHTTP)
}

func (c *RESTClient) commitTableURL(ctx context.Context, req api.CommitRequest) (string, error) {
	segments := c.pathPrefix(req.CatalogRequest)
	segments = append(segments, "namespaces", namespacePath(req.Namespace, c.namespaceSep), "tables", req.Table)
	return restURL(req.Catalog.URI, segments, c.allowPlainHTTP)
}

func (c *RESTClient) planScanURL(ctx context.Context, req api.ScanPlanRequest) (string, error) {
	segments := c.pathPrefix(req.CatalogRequest)
	segments = append(segments, "namespaces", namespacePath(req.Namespace, c.namespaceSep), "tables", req.Table, "plan-scan")
	return restURL(req.Catalog.URI, segments, c.allowPlainHTTP)
}

func (c *RESTClient) metricsURL(ctx context.Context, req api.MetricsReportRequest) (string, error) {
	segments := c.pathPrefix(req.CatalogRequest)
	segments = append(segments, "namespaces", namespacePath(req.Namespace, c.namespaceSep), "tables", req.Table, "metrics")
	return restURL(req.Catalog.URI, segments, c.allowPlainHTTP)
}

func (c *RESTClient) pathPrefix(req api.CatalogRequest) []string {
	prefix := normalizeCatalogPrefix(firstNonEmpty(req.Prefix, prefixFromWarehouse(req.Catalog.Warehouse)))
	if prefix == "" {
		return nil
	}
	return []string{prefix}
}

func (c *RESTClient) configCacheKey(req api.GetConfigRequest) string {
	return strings.Join([]string{
		strconv.FormatUint(uint64(req.Catalog.AccountID), 10),
		strconv.FormatUint(req.Catalog.CatalogID, 10),
		req.Catalog.URI,
		firstNonEmpty(req.Warehouse, req.Catalog.Warehouse),
		req.ExternalPrincipal,
	}, "\x00")
}

func (c *RESTClient) getConfigCache(key string) (api.ConfigResponse, bool) {
	if c.cacheTTL <= 0 {
		return api.ConfigResponse{}, false
	}
	c.configCacheMu.Lock()
	defer c.configCacheMu.Unlock()
	entry, ok := c.configCache[key]
	if !ok || time.Now().After(entry.expiresAt) {
		if ok {
			delete(c.configCache, key)
		}
		return api.ConfigResponse{}, false
	}
	return cloneConfigResponse(entry.response), true
}

func (c *RESTClient) putConfigCache(key string, resp api.ConfigResponse) {
	if c.cacheTTL <= 0 {
		return
	}
	c.configCacheMu.Lock()
	defer c.configCacheMu.Unlock()
	c.configCache[key] = configCacheEntry{expiresAt: time.Now().Add(c.cacheTTL), response: cloneConfigResponse(resp)}
}

func restURL(rawBase string, segments []string, allowPlainHTTP bool) (string, error) {
	rawBase = strings.TrimSpace(rawBase)
	if rawBase == "" {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg REST catalog URI is required", nil)
	}
	base, err := url.Parse(rawBase)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", api.WrapError(api.ErrConfigInvalid, "Iceberg REST catalog URI is invalid", map[string]string{"uri": rawBase}, err)
	}
	switch strings.ToLower(base.Scheme) {
	case "https":
	case "http":
		if !allowPlainHTTP {
			return "", api.NewError(api.ErrConfigInvalid, "Iceberg REST catalog URI must use https unless plain HTTP is explicitly enabled", map[string]string{"uri": rawBase})
		}
	default:
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg REST catalog URI must use https", map[string]string{"uri": rawBase})
	}
	pathParts := splitPath(base.Path)
	if len(pathParts) == 0 || pathParts[len(pathParts)-1] != "v1" {
		pathParts = append(pathParts, "v1")
	}
	for _, segment := range segments {
		if strings.TrimSpace(segment) == "" {
			continue
		}
		pathParts = append(pathParts, segment)
	}
	escaped := make([]string, 0, len(pathParts))
	for _, part := range pathParts {
		escaped = append(escaped, url.PathEscape(part))
	}
	base.Path = "/" + strings.Join(pathParts, "/")
	base.RawPath = "/" + strings.Join(escaped, "/")
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
}

func splitPath(p string) []string {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func namespacePath(ns api.Namespace, sep string) string {
	return strings.Join([]string(ns), sep)
}

func prefixFromWarehouse(warehouse string) string {
	warehouse = strings.TrimSpace(warehouse)
	if warehouse == "" || strings.Contains(warehouse, "://") {
		return ""
	}
	return warehouse
}

func normalizeCatalogPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	if decoded, err := url.PathUnescape(prefix); err == nil {
		prefix = strings.TrimSpace(decoded)
	}
	return prefix
}

func decodeJSON(data []byte, out any, operation string) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg REST response body is empty", map[string]string{"operation": operation})
	}
	if err := json.Unmarshal(data, out); err != nil {
		return api.WrapError(api.ErrMetadataInvalid, "Iceberg REST response JSON is invalid", map[string]string{"operation": operation}, err)
	}
	return nil
}

func mapHTTPError(operation string, status int, body []byte) error {
	fields := map[string]string{
		"operation": operation,
		"status":    strconv.Itoa(status),
	}
	if model := parseErrorModel(body); model.Message != "" {
		fields["catalog_error_type"] = model.Type
		fields["catalog_error_code"] = strconv.Itoa(model.Code)
		return api.NewError(errorCodeForStatus(status, model.Type), model.Message, fields)
	}
	return api.NewError(errorCodeForStatus(status, ""), http.StatusText(status), fields)
}

func errorCodeForStatus(status int, catalogType string) api.ErrorCode {
	switch status {
	case http.StatusUnauthorized:
		return api.ErrAuthUnauthorized
	case http.StatusForbidden:
		return api.ErrAuthForbidden
	case http.StatusNotFound:
		return api.ErrTableNotFound
	case http.StatusConflict:
		return api.ErrCommitConflict
	case 419:
		return api.ErrAuthTimeout
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return api.ErrCatalogUnavailable
	case http.StatusNotAcceptable, http.StatusNotImplemented:
		return api.ErrUnsupportedFeature
	case http.StatusBadRequest:
		return api.ErrConfigInvalid
	default:
		if status >= 500 {
			return api.ErrCatalogUnavailable
		}
		if strings.Contains(strings.ToLower(catalogType), "unsupported") {
			return api.ErrUnsupportedFeature
		}
		return api.ErrCatalogUnavailable
	}
}

func parseErrorModel(body []byte) errorModelWire {
	var nested struct {
		Error errorModelWire `json:"error"`
	}
	if err := json.Unmarshal(body, &nested); err == nil && nested.Error.Message != "" {
		return nested.Error
	}
	var flat errorModelWire
	if err := json.Unmarshal(body, &flat); err == nil {
		return flat
	}
	return errorModelWire{}
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable || status >= 500
}

func retryableNetworkError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if ok := stderrors.As(err, &netErr); ok && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	var icebergErr *api.IcebergError
	return stderrors.As(err, &icebergErr) && icebergErr.Code == api.ErrCatalogUnavailable
}

func retryDelay(policy api.RetryPolicy, attempt int) time.Duration {
	delay := policy.BaseBackoff
	if delay <= 0 {
		return 0
	}
	for i := 1; i < attempt; i++ {
		delay *= 2
		if policy.MaxBackoff > 0 && delay > policy.MaxBackoff {
			return policy.MaxBackoff
		}
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func mergeRetryPolicy(req, fallback api.RetryPolicy) api.RetryPolicy {
	if req.MaxAttempts == 0 {
		req.MaxAttempts = fallback.MaxAttempts
	}
	if req.BaseBackoff == 0 {
		req.BaseBackoff = fallback.BaseBackoff
	}
	if req.MaxBackoff == 0 {
		req.MaxBackoff = fallback.MaxBackoff
	}
	return req
}

func normalizeMaxPages(maxPages int) int {
	if maxPages <= 0 {
		return 1000
	}
	return maxPages
}

func negotiateCapabilities(defaults, overrides map[string]string, endpoints []string) api.CatalogCapabilities {
	cfg := mergeConfig(defaults, overrides)
	caps := api.CatalogCapabilities{}
	for _, endpoint := range endpoints {
		e := strings.ToLower(endpoint)
		if strings.Contains(e, "/credentials") {
			caps.CredentialVending = true
		}
		if strings.Contains(e, "/sign") {
			caps.RemoteSigning = true
		}
		if strings.Contains(e, "plan") && strings.Contains(e, "scan") {
			caps.ServerSidePlanning = true
		}
		if strings.Contains(e, "refs") {
			caps.BranchTag = true
		}
		if strings.HasPrefix(e, "post ") && (strings.Contains(e, "/transactions/commit") || strings.Contains(e, "/tables/{table}")) {
			caps.Commit = true
		}
		if isCreateTableEndpoint(e) {
			caps.CreateTable = true
		}
		if strings.Contains(e, "/metrics") {
			caps.MetricsReport = true
		}
	}
	if strings.EqualFold(cfg["create-table-enabled"], "true") || strings.EqualFold(cfg["table-create-enabled"], "true") {
		caps.CreateTable = true
	}
	if strings.EqualFold(cfg["s3.remote-signing-enabled"], "true") {
		caps.RemoteSigning = true
	}
	if cfg["scan-planning-mode"] == "server" {
		caps.ServerSidePlanning = true
	}
	if strings.EqualFold(cfg["metrics-report-enabled"], "true") || strings.EqualFold(cfg["catalog-metrics-report-enabled"], "true") {
		caps.MetricsReport = true
	}
	if hasStorageCredentialConfig(cfg) {
		caps.CredentialVending = true
	}
	return caps
}

func isCreateTableEndpoint(endpoint string) bool {
	e := strings.TrimSpace(strings.ToLower(endpoint))
	if !strings.HasPrefix(e, "post ") {
		return false
	}
	path := strings.TrimSpace(strings.TrimPrefix(e, "post "))
	path = strings.TrimSuffix(path, "/")
	return strings.HasSuffix(path, "/tables") && !strings.Contains(path, "{table}")
}

func mergeConfig(defaults, overrides map[string]string) map[string]string {
	out := cloneStringMap(defaults)
	if out == nil {
		out = make(map[string]string)
	}
	for k, v := range overrides {
		out[k] = v
	}
	return out
}

func tableTokenFromConfig(config map[string]string) string {
	if token := strings.TrimSpace(config["token"]); token != "" {
		return token
	}
	for key, value := range config {
		if strings.HasPrefix(strings.ToLower(key), "urn:ietf:params:oauth:token-type:") && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func storageCredentialsFromWire(in []storageCredentialWire) []api.StorageCredential {
	if len(in) == 0 {
		return nil
	}
	out := make([]api.StorageCredential, 0, len(in))
	for _, cred := range in {
		out = append(out, api.StorageCredential{Prefix: cred.Prefix, Config: cloneStringMap(cred.Config)})
	}
	return out
}

func storageCredentialsFromConfig(config map[string]string) []api.StorageCredential {
	if !hasStorageCredentialConfig(config) {
		return nil
	}
	return []api.StorageCredential{{Config: selectStorageCredentialConfig(config)}}
}

func hasStorageCredentialConfig(config map[string]string) bool {
	return len(selectStorageCredentialConfig(config)) > 0
}

func selectStorageCredentialConfig(config map[string]string) map[string]string {
	out := make(map[string]string)
	for key, value := range config {
		k := strings.ToLower(key)
		if strings.HasPrefix(k, "s3.") || strings.HasPrefix(k, "gcs.") || strings.HasPrefix(k, "adls.") || strings.HasPrefix(k, "client.region") {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneConfigResponse(in api.ConfigResponse) api.ConfigResponse {
	out := in
	out.Defaults = cloneStringMap(in.Defaults)
	out.Overrides = cloneStringMap(in.Overrides)
	out.Endpoints = append([]string(nil), in.Endpoints...)
	return out
}

func cloneRawMessage(in json.RawMessage) json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make(json.RawMessage, len(in))
	copy(out, in)
	return out
}

func commitRequirementWires(in []api.CommitRequirement) []commitRequirementWire {
	if len(in) == 0 {
		return nil
	}
	out := make([]commitRequirementWire, 0, len(in))
	for _, req := range in {
		wire := commitRequirementWire{
			Type:       req.Type,
			Ref:        req.Ref,
			SnapshotID: req.SnapshotID,
			TableUUID:  req.TableUUID,
		}
		switch req.Type {
		case "assert-ref-not-exists":
			wire.Type = "assert-ref-snapshot-id"
			wire.SnapshotID = 0
		case "assert-current-schema-id":
			wire.CurrentSchemaID = intPtr(req.SchemaID)
		case "assert-default-spec-id":
			wire.DefaultSpecID = intPtr(req.SpecID)
		case "assert-default-sort-order-id":
			wire.DefaultSortOrderID = intPtr(req.SortOrderID)
		}
		out = append(out, wire)
	}
	return out
}

func intPtr(v int) *int {
	return &v
}

func commitUpdateWires(in []api.CommitUpdate) []commitUpdateWire {
	if len(in) == 0 {
		return nil
	}
	out := make([]commitUpdateWire, 0, len(in))
	for _, update := range in {
		wire := commitUpdateWire{Action: update.Type}
		switch update.Type {
		case "add-snapshot":
			if update.Snapshot != nil {
				snapshot := *update.Snapshot
				snapshot.Summary = cloneStringMap(snapshot.Summary)
				wire.Snapshot = &snapshot
			} else {
				wire.Payload = cloneStringMap(update.Payload)
			}
		case "set-snapshot-ref":
			wire.RefName = update.Ref
			wire.RefType = update.RefType
			wire.SnapshotID = update.SnapshotID
			wire.MinSnapshotsToKeep = update.MinSnapshotsToKeep
			wire.MaxSnapshotAgeMS = update.MaxSnapshotAgeMS
			wire.MaxRefAgeMS = update.MaxRefAgeMS
		case "remove-snapshot":
			if raw := firstNonEmpty(update.Payload["snapshot-id"], update.Payload["snapshot_id"]); raw != "" {
				if snapshotID, err := strconv.ParseInt(raw, 10, 64); err == nil {
					wire.SnapshotID = snapshotID
				}
			}
		default:
			wire.Payload = cloneStringMap(update.Payload)
			wire.FilePath = update.FilePath
		}
		out = append(out, wire)
	}
	return out
}

func schemaWireFromAPI(schema api.Schema) schemaWire {
	fields := make([]schemaFieldWire, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		fields = append(fields, schemaFieldWire{
			ID:       field.ID,
			Name:     field.Name,
			Required: field.Required,
			Type:     field.Type.String(),
			Doc:      field.Doc,
		})
	}
	return schemaWire{
		SchemaID:           schema.SchemaID,
		Fields:             fields,
		IdentifierFieldIDs: append([]int(nil), schema.IdentifierFieldIDs...),
	}
}

func partitionSpecWireFromAPI(spec api.PartitionSpec) partitionSpecWire {
	fields := make([]partitionFieldWire, 0, len(spec.Fields))
	for _, field := range spec.Fields {
		fields = append(fields, partitionFieldWire{
			SourceID:  field.SourceID,
			FieldID:   field.FieldID,
			Name:      field.Name,
			Transform: field.Transform,
		})
	}
	return partitionSpecWire{SpecID: spec.SpecID, Fields: fields}
}

func prunePredicateWires(in []api.PrunePredicate) []prunePredicateWire {
	if len(in) == 0 {
		return nil
	}
	out := make([]prunePredicateWire, 0, len(in))
	for _, predicate := range in {
		out = append(out, prunePredicateWire{
			FieldID: predicate.FieldID,
			Op:      predicate.Op,
			Literal: predicate.Literal,
		})
	}
	return out
}

func scanPlanFromWire(wire planScanResponseWire) *api.IcebergScanPlan {
	return &api.IcebergScanPlan{
		Snapshot:                  wire.Snapshot,
		DataTasks:                 append([]api.DataFileTask(nil), wire.DataTasks...),
		DeleteTasks:               append([]api.DeleteFileTask(nil), wire.DeleteTasks...),
		ColumnMapping:             append([]api.IcebergColumnMapping(nil), wire.ColumnMapping...),
		ResidualFilter:            wire.ResidualFilter,
		Profile:                   wire.Profile,
		ObjectIORef:               wire.ObjectIORef,
		ServerPredicateEquivalent: wire.PredicateEquivalent,
	}
}

type rawHTTPResponse struct {
	statusCode int
	headers    http.Header
	body       []byte
}

type configResponseWire struct {
	Defaults               map[string]string `json:"defaults"`
	Overrides              map[string]string `json:"overrides"`
	Endpoints              []string          `json:"endpoints"`
	IdempotencyKeyLifetime string            `json:"idempotency-key-lifetime"`
}

type listNamespacesWire struct {
	NextPageToken string     `json:"next-page-token"`
	Namespaces    [][]string `json:"namespaces"`
}

type listTablesWire struct {
	NextPageToken string                `json:"next-page-token"`
	Identifiers   []tableIdentifierWire `json:"identifiers"`
}

type tableIdentifierWire struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

type loadTableWire struct {
	MetadataLocation   string                  `json:"metadata-location"`
	Metadata           json.RawMessage         `json:"metadata"`
	Config             map[string]string       `json:"config"`
	StorageCredentials []storageCredentialWire `json:"storage-credentials"`
}

type loadCredentialsWire struct {
	StorageCredentials []storageCredentialWire `json:"storage-credentials"`
}

type createTableRequestWire struct {
	Name          string            `json:"name"`
	Schema        schemaWire        `json:"schema"`
	PartitionSpec partitionSpecWire `json:"partition-spec,omitempty"`
	Location      string            `json:"location,omitempty"`
	Properties    map[string]string `json:"properties,omitempty"`
	StageCreate   bool              `json:"stage-create"`
}

type schemaWire struct {
	SchemaID           int               `json:"schema-id"`
	Fields             []schemaFieldWire `json:"fields"`
	IdentifierFieldIDs []int             `json:"identifier-field-ids,omitempty"`
}

type schemaFieldWire struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
	Doc      string `json:"doc,omitempty"`
}

type partitionSpecWire struct {
	SpecID int                  `json:"spec-id"`
	Fields []partitionFieldWire `json:"fields,omitempty"`
}

type partitionFieldWire struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id,omitempty"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

type commitTableRequestWire struct {
	Identifier   tableIdentifierWire     `json:"identifier"`
	Requirements []commitRequirementWire `json:"requirements"`
	Updates      []commitUpdateWire      `json:"updates"`
}

type commitTableResponseWire struct {
	MetadataLocation string `json:"metadata-location"`
	SnapshotID       int64  `json:"snapshot-id"`
}

type planScanRequestWire struct {
	Identifier             tableIdentifierWire  `json:"identifier"`
	Ref                    string               `json:"ref,omitempty"`
	SnapshotID             int64                `json:"snapshot-id,omitempty"`
	HasSnapshotID          bool                 `json:"has-snapshot-id,omitempty"`
	TimestampMS            int64                `json:"timestamp-ms,omitempty"`
	HasTimestampMS         bool                 `json:"has-timestamp-ms,omitempty"`
	ProjectionIDs          []int                `json:"projection-ids,omitempty"`
	ResidualSQL            string               `json:"residual-sql,omitempty"`
	PrunePredicates        []prunePredicateWire `json:"prune-predicates,omitempty"`
	EnableRowGroupPlanning bool                 `json:"enable-row-group-planning,omitempty"`
	EnableDeleteApply      bool                 `json:"enable-delete-apply,omitempty"`
	DeleteMaxMemoryBytes   int64                `json:"delete-max-memory-bytes,omitempty"`
	EnableDeleteSpill      bool                 `json:"enable-delete-spill,omitempty"`
}

type prunePredicateWire struct {
	FieldID int              `json:"field-id"`
	Op      api.PruneOp      `json:"op"`
	Literal api.PruneLiteral `json:"literal"`
}

type planScanResponseWire struct {
	Snapshot            api.SnapshotPlan           `json:"snapshot"`
	DataTasks           []api.DataFileTask         `json:"data-tasks"`
	DeleteTasks         []api.DeleteFileTask       `json:"delete-tasks"`
	ColumnMapping       []api.IcebergColumnMapping `json:"column-mapping"`
	ResidualFilter      api.ResidualFilter         `json:"residual-filter"`
	Profile             api.PlanningProfile        `json:"profile"`
	ObjectIORef         string                     `json:"object-io-ref"`
	PredicateEquivalent bool                       `json:"predicate-equivalent,omitempty"`
}

type metricsReportRequestWire struct {
	Identifier           tableIdentifierWire   `json:"identifier"`
	Ref                  string                `json:"ref,omitempty"`
	SnapshotID           int64                 `json:"snapshot-id,omitempty"`
	QueryID              string                `json:"query-id,omitempty"`
	StatementID          string                `json:"statement-id,omitempty"`
	Kind                 api.MetricsReportKind `json:"kind"`
	PlanningProfile      api.PlanningProfile   `json:"planning-profile,omitempty"`
	CommitID             string                `json:"commit-id,omitempty"`
	MetadataLocationHash string                `json:"metadata-location-hash,omitempty"`
	Rows                 int64                 `json:"rows,omitempty"`
	Files                int                   `json:"files,omitempty"`
	Extra                map[string]string     `json:"extra,omitempty"`
}

type commitRequirementWire struct {
	Type               string `json:"type"`
	Ref                string `json:"ref,omitempty"`
	SnapshotID         int64  `json:"snapshot-id,omitempty"`
	TableUUID          string `json:"uuid,omitempty"`
	CurrentSchemaID    *int   `json:"current-schema-id,omitempty"`
	DefaultSpecID      *int   `json:"default-spec-id,omitempty"`
	DefaultSortOrderID *int   `json:"default-sort-order-id,omitempty"`
}

type commitUpdateWire struct {
	Action             string            `json:"action"`
	Payload            map[string]string `json:"payload,omitempty"`
	FilePath           string            `json:"file-path,omitempty"`
	Snapshot           *api.Snapshot     `json:"snapshot,omitempty"`
	RefName            string            `json:"ref-name,omitempty"`
	RefType            string            `json:"type,omitempty"`
	SnapshotID         int64             `json:"snapshot-id,omitempty"`
	MinSnapshotsToKeep int               `json:"min-snapshots-to-keep,omitempty"`
	MaxSnapshotAgeMS   int64             `json:"max-snapshot-age-ms,omitempty"`
	MaxRefAgeMS        int64             `json:"max-ref-age-ms,omitempty"`
}

type storageCredentialWire struct {
	Prefix string            `json:"prefix"`
	Config map[string]string `json:"config"`
}

type errorModelWire struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

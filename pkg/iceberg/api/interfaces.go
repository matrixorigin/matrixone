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

package api

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type CatalogClient interface {
	GetConfig(ctx context.Context, req GetConfigRequest) (*ConfigResponse, error)
	ListNamespaces(ctx context.Context, req ListNamespacesRequest) (*ListNamespacesResponse, error)
	ListTables(ctx context.Context, req ListTablesRequest) (*ListTablesResponse, error)
	LoadTable(ctx context.Context, req LoadTableRequest) (*LoadTableResponse, error)
	LoadCredentials(ctx context.Context, req LoadCredentialsRequest) (*LoadCredentialsResponse, error)
	CreateTable(ctx context.Context, req CreateTableRequest) (*CreateTableResponse, error)
	CommitTable(ctx context.Context, req CommitRequest) (*CommitResult, error)
}

type CatalogFacade interface {
	CatalogClient
	AdapterName() string
}

type CatalogRequest struct {
	Catalog           model.Catalog
	ExternalPrincipal string
	RequestID         string
	Timeout           time.Duration
	Retry             RetryPolicy
	Prefix            string
	TableToken        string
}

type RetryPolicy struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func (p RetryPolicy) Normalize() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.BaseBackoff < 0 {
		p.BaseBackoff = 0
	}
	if p.MaxBackoff < 0 {
		p.MaxBackoff = 0
	}
	if p.MaxBackoff > 0 && p.BaseBackoff > p.MaxBackoff {
		p.BaseBackoff = p.MaxBackoff
	}
	return p
}

type GetConfigRequest struct {
	CatalogRequest
	Warehouse string
	NoCache   bool
}

type ConfigResponse struct {
	Defaults               map[string]string
	Overrides              map[string]string
	Endpoints              []string
	IdempotencyKeyLifetime string
	Capabilities           CatalogCapabilities
	Prefix                 string
	Cached                 bool
}

// IsNessieCatalog reports whether either configuration map advertises Nessie.
func (r *ConfigResponse) IsNessieCatalog() bool {
	if r == nil {
		return false
	}
	for _, cfg := range []map[string]string{r.Overrides, r.Defaults} {
		for key, value := range cfg {
			if strings.EqualFold(strings.TrimSpace(key), "nessie.is-nessie-catalog") &&
				strings.EqualFold(strings.TrimSpace(value), "true") {
				return true
			}
		}
	}
	return false
}

type CatalogCapabilities struct {
	CredentialVending  bool
	RemoteSigning      bool
	ServerSidePlanning bool
	BranchTag          bool
	Commit             bool
	CreateTable        bool
	MetricsReport      bool
}

type ListNamespacesRequest struct {
	CatalogRequest
	Parent   Namespace
	PageSize int
	MaxPages int
}

type ListNamespacesResponse struct {
	Namespaces    []Namespace
	NextPageToken string
}

type ListTablesRequest struct {
	CatalogRequest
	Namespace Namespace
	PageSize  int
	MaxPages  int
}

type ListTablesResponse struct {
	Identifiers   []TableIdentifier
	NextPageToken string
}

type LoadTableRequest struct {
	CatalogRequest
	Namespace        Namespace
	Table            string
	Snapshots        string
	IfNoneMatch      string
	AccessDelegation []string
	ReferencedBy     []string
}

type LoadTableResponse struct {
	Namespace           Namespace
	TableName           string
	MetadataLocation    string
	MetadataJSON        json.RawMessage
	Config              map[string]string
	TableToken          string
	StorageCredentials  []StorageCredential
	Capabilities        CatalogCapabilities
	ETag                string
	NotModified         bool
	MetadataLocationRed string
}

type CreateTableRequest struct {
	CatalogRequest
	Namespace     Namespace
	Table         string
	Schema        Schema
	PartitionSpec PartitionSpec
	Location      string
	Properties    map[string]string
	StageCreate   bool
}

type CreateTableResponse struct {
	Namespace            Namespace
	TableName            string
	MetadataLocation     string
	MetadataLocationHash string
	TableUUID            string
	Config               map[string]string
	MetadataJSON         json.RawMessage
	StorageCredentials   []StorageCredential
}

type LoadCredentialsRequest struct {
	CatalogRequest
	Namespace    Namespace
	Table        string
	PlanID       string
	ReferencedBy []string
}

type LoadCredentialsResponse struct {
	StorageCredentials []StorageCredential
}

type Namespace []string

type TableIdentifier struct {
	Namespace Namespace
	Name      string
}

type StorageCredential struct {
	Prefix    string
	Config    map[string]string
	ExpiresAt time.Time
}

type MetadataReader interface {
	Read(ctx context.Context, location string) (TableMetadata, error)
}

type ObjectReader interface {
	Read(ctx context.Context, location string, offset, length int64) ([]byte, error)
}

// ResolveCatalogRequestPrefix retrieves the catalog prefix when it is not already set.
func ResolveCatalogRequestPrefix(ctx context.Context, client CatalogClient, req CatalogRequest) (CatalogRequest, error) {
	if client == nil || strings.TrimSpace(req.Prefix) != "" {
		return req, nil
	}
	resp, err := client.GetConfig(ctx, GetConfigRequest{
		CatalogRequest: req,
		Warehouse:      req.Catalog.Warehouse,
	})
	if err != nil {
		return req, err
	}
	if resp != nil && strings.TrimSpace(resp.Prefix) != "" {
		req.Prefix = strings.TrimSpace(resp.Prefix)
	}
	return req, nil
}

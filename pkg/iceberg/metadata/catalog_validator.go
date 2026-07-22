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

package metadata

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
)

type validatingCatalogClient struct {
	upstream  api.CatalogClient
	validator CatalogRequestValidator
}

func (c validatingCatalogClient) GetConfig(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
	if err := c.validate(ctx, req.CatalogRequest); err != nil {
		return nil, err
	}
	return c.upstream.GetConfig(ctx, req)
}

func (c validatingCatalogClient) ListNamespaces(ctx context.Context, req api.ListNamespacesRequest) (*api.ListNamespacesResponse, error) {
	if err := c.validate(ctx, req.CatalogRequest); err != nil {
		return nil, err
	}
	return c.upstream.ListNamespaces(ctx, req)
}

func (c validatingCatalogClient) ListTables(ctx context.Context, req api.ListTablesRequest) (*api.ListTablesResponse, error) {
	if err := c.validate(ctx, req.CatalogRequest); err != nil {
		return nil, err
	}
	return c.upstream.ListTables(ctx, req)
}

func (c validatingCatalogClient) LoadTable(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
	if err := c.validate(ctx, req.CatalogRequest); err != nil {
		return nil, err
	}
	return c.upstream.LoadTable(ctx, req)
}

func (c validatingCatalogClient) LoadCredentials(ctx context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
	if err := c.validate(ctx, req.CatalogRequest); err != nil {
		return nil, err
	}
	return c.upstream.LoadCredentials(ctx, req)
}

func (c validatingCatalogClient) CreateTable(ctx context.Context, req api.CreateTableRequest) (*api.CreateTableResponse, error) {
	if err := c.validate(ctx, req.CatalogRequest); err != nil {
		return nil, err
	}
	return c.upstream.CreateTable(ctx, req)
}

func (c validatingCatalogClient) CommitTable(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
	if err := c.validate(ctx, req.CatalogRequest); err != nil {
		return nil, err
	}
	return c.upstream.CommitTable(ctx, req)
}

func (c validatingCatalogClient) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	if err := c.validate(ctx, req.CatalogRequest); err != nil {
		return nil, err
	}
	planner, ok := c.upstream.(api.ScanPlanner)
	if !ok {
		return nil, api.NewError(api.ErrServerPlanningRequired, "Iceberg server-side planning is required but catalog client cannot plan scans", nil)
	}
	return planner.PlanScan(ctx, req)
}

func (c validatingCatalogClient) NewRemoteSigner(req api.CatalogRequest, config map[string]string) icebergio.RemoteSigner {
	factory, ok := c.upstream.(interface {
		NewRemoteSigner(api.CatalogRequest, map[string]string) icebergio.RemoteSigner
	})
	if !ok {
		return nil
	}
	return factory.NewRemoteSigner(req, config)
}

func (c validatingCatalogClient) validate(ctx context.Context, req api.CatalogRequest) error {
	if c.validator == nil {
		return nil
	}
	return c.validator(ctx, req)
}

var _ api.CatalogClient = validatingCatalogClient{}
var _ api.ScanPlanner = validatingCatalogClient{}

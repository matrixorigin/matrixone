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

package maintenance

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

type CatalogClientFactory interface {
	NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error)
}

type CatalogRunnerOptions struct {
	OrphanRecorder    write.OrphanRecorder
	CommitVerifier    CommitResultVerifier
	Now               func() time.Time
	OrphanTTL         time.Duration
	DefaultRetainLast int
	PlanningMaxMemory int64
}

func NewCatalogExpireSnapshotsRunner(client api.CatalogClient, catalog api.CatalogRequest, opts CatalogRunnerOptions) Runner {
	return CommitRunner{
		Planner: ExpireSnapshotsPlanner{
			Catalog:           catalog,
			Loader:            CatalogMetadataLoader{Client: client, Catalog: catalog},
			Now:               opts.Now,
			DefaultRetainLast: opts.DefaultRetainLast,
		},
		Committer:      client,
		Verifier:       opts.CommitVerifier,
		OrphanRecorder: opts.OrphanRecorder,
		Now:            opts.Now,
		OrphanTTL:      opts.OrphanTTL,
	}
}

func NewCatalogRewriteManifestsRunner(client api.CatalogClient, catalog api.CatalogRequest, opts CatalogRunnerOptions) Runner {
	return CommitRunner{
		Planner: RewriteManifestsPlanner{
			Catalog: catalog,
			Loader:  CatalogMetadataLoader{Client: client, Catalog: catalog},
		},
		Committer:      client,
		Verifier:       opts.CommitVerifier,
		OrphanRecorder: opts.OrphanRecorder,
		Now:            opts.Now,
		OrphanTTL:      opts.OrphanTTL,
	}
}

func NewNativeRewriteManifestsRunner(client api.CatalogClient, catalog api.CatalogRequest, objectReader api.ObjectReader, objectWriter ObjectWriter, opts CatalogRunnerOptions) Runner {
	return CommitRunner{
		Planner: NativeRewriteManifestsPlanner{
			Catalog: catalog,
			Loader:  CatalogMetadataLoader{Client: client, Catalog: catalog},
			Now:     opts.Now,
			Materializer: RewriteManifestsMaterializer{
				ObjectReader:   objectReader,
				MaxMemoryBytes: opts.PlanningMaxMemory,
			},
		},
		ObjectWriter:   objectWriter,
		Committer:      client,
		Verifier:       opts.CommitVerifier,
		OrphanRecorder: opts.OrphanRecorder,
		Now:            opts.Now,
		OrphanTTL:      opts.OrphanTTL,
	}
}

func NewNativeRewriteDataFilesRunner(client api.CatalogClient, catalog api.CatalogRequest, objectReader api.ObjectReader, objectWriter ObjectWriter, compactor RewriteDataFilesCompactor, opts CatalogRunnerOptions) Runner {
	return CommitRunner{
		Planner: NativeRewriteDataFilesPlanner{
			Catalog: catalog,
			Loader:  CatalogMetadataLoader{Client: client, Catalog: catalog},
			Now:     opts.Now,
			Selector: RewriteDataFilesSelector{
				ObjectReader:   objectReader,
				MaxMemoryBytes: opts.PlanningMaxMemory,
			},
			Compactor: compactor,
		},
		ObjectWriter:   objectWriter,
		Committer:      client,
		Verifier:       opts.CommitVerifier,
		OrphanRecorder: opts.OrphanRecorder,
		Now:            opts.Now,
		OrphanTTL:      opts.OrphanTTL,
	}
}

func NewCatalogRewriteDataFilesRunner(client api.CatalogClient, catalog api.CatalogRequest, opts CatalogRunnerOptions) Runner {
	return CommitRunner{
		Planner: RewriteDataFilesPlanner{
			Catalog: catalog,
			Loader:  CatalogMetadataLoader{Client: client, Catalog: catalog},
		},
		Committer:      client,
		Verifier:       opts.CommitVerifier,
		OrphanRecorder: opts.OrphanRecorder,
		Now:            opts.Now,
		OrphanTTL:      opts.OrphanTTL,
	}
}

type CatalogExpireSnapshotsRunner struct {
	CatalogFactory    CatalogClientFactory
	OrphanRecorder    write.OrphanRecorder
	CommitVerifier    CommitResultVerifier
	Now               func() time.Time
	OrphanTTL         time.Duration
	DefaultRetainLast int
}

func (r CatalogExpireSnapshotsRunner) RunMaintenance(ctx context.Context, req Request) (Result, error) {
	if r.CatalogFactory == nil {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg expire-snapshots runner requires a catalog client factory", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if req.Catalog.CatalogID == 0 {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg expire-snapshots runner requires resolved catalog metadata", map[string]string{
			"operation": string(req.Operation),
		})
	}
	client, err := r.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return Result{}, err
	}
	catalogReq, err := resolveMaintenanceCatalogRequestPrefix(ctx, client, api.CatalogRequest{
		Catalog:           req.Catalog,
		ExternalPrincipal: strings.TrimSpace(req.ExternalPrincipal),
	})
	if err != nil {
		return Result{}, err
	}
	return NewCatalogExpireSnapshotsRunner(
		client,
		catalogReq,
		CatalogRunnerOptions{
			OrphanRecorder:    r.OrphanRecorder,
			CommitVerifier:    r.CommitVerifier,
			Now:               r.Now,
			OrphanTTL:         r.OrphanTTL,
			DefaultRetainLast: r.DefaultRetainLast,
		},
	).RunMaintenance(ctx, req)
}

type CatalogRewriteDataFilesRunner struct {
	CatalogFactory CatalogClientFactory
	OrphanRecorder write.OrphanRecorder
	CommitVerifier CommitResultVerifier
	Now            func() time.Time
	OrphanTTL      time.Duration
}

func (r CatalogRewriteDataFilesRunner) RunMaintenance(ctx context.Context, req Request) (Result, error) {
	if r.CatalogFactory == nil {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files runner requires a catalog client factory", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if req.Catalog.CatalogID == 0 {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-data-files runner requires resolved catalog metadata", map[string]string{
			"operation": string(req.Operation),
		})
	}
	client, err := r.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return Result{}, err
	}
	catalogReq, err := resolveMaintenanceCatalogRequestPrefix(ctx, client, api.CatalogRequest{
		Catalog:           req.Catalog,
		ExternalPrincipal: strings.TrimSpace(req.ExternalPrincipal),
	})
	if err != nil {
		return Result{}, err
	}
	return NewCatalogRewriteDataFilesRunner(
		client,
		catalogReq,
		CatalogRunnerOptions{
			OrphanRecorder: r.OrphanRecorder,
			CommitVerifier: r.CommitVerifier,
			Now:            r.Now,
			OrphanTTL:      r.OrphanTTL,
		},
	).RunMaintenance(ctx, req)
}

type CatalogRewriteManifestsRunner struct {
	CatalogFactory CatalogClientFactory
	OrphanRecorder write.OrphanRecorder
	CommitVerifier CommitResultVerifier
	Now            func() time.Time
	OrphanTTL      time.Duration
}

func (r CatalogRewriteManifestsRunner) RunMaintenance(ctx context.Context, req Request) (Result, error) {
	if r.CatalogFactory == nil {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-manifests runner requires a catalog client factory", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if req.Catalog.CatalogID == 0 {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-manifests runner requires resolved catalog metadata", map[string]string{
			"operation": string(req.Operation),
		})
	}
	client, err := r.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return Result{}, err
	}
	catalogReq, err := resolveMaintenanceCatalogRequestPrefix(ctx, client, api.CatalogRequest{
		Catalog:           req.Catalog,
		ExternalPrincipal: strings.TrimSpace(req.ExternalPrincipal),
	})
	if err != nil {
		return Result{}, err
	}
	return NewCatalogRewriteManifestsRunner(
		client,
		catalogReq,
		CatalogRunnerOptions{
			OrphanRecorder: r.OrphanRecorder,
			CommitVerifier: r.CommitVerifier,
			Now:            r.Now,
			OrphanTTL:      r.OrphanTTL,
		},
	).RunMaintenance(ctx, req)
}

func resolveMaintenanceCatalogRequestPrefix(ctx context.Context, client api.CatalogClient, req api.CatalogRequest) (api.CatalogRequest, error) {
	if client == nil || strings.TrimSpace(req.Prefix) != "" {
		return req, nil
	}
	resp, err := client.GetConfig(ctx, api.GetConfigRequest{
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

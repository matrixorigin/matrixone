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

package iceberg

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	internalexecutor "github.com/matrixorigin/matrixone/pkg/util/executor"
)

type MaintenanceStore interface {
	CatalogByNameGetter
	OrphanFileInserter
	maintenance.JobRecorder
}

type MaintenanceProcedureExecutorOptions struct {
	Config            api.Config
	Account           api.AccountConfig
	CatalogFactory    maintenance.CatalogClientFactory
	OrphanTTL         time.Duration
	DefaultRetainLast int
	Now               func() time.Time
	ObjectIOProvider  icebergio.ObjectIOProvider
	ScopeForLocation  icebergio.ObjectScopeForLocation
	BuildFileService  icebergio.ScopedFileServiceBuilder
	ResidencyPolicies []model.ResidencyPolicy
	CommitVerifier    maintenance.CommitResultVerifier
	DataFileCompactor maintenance.RewriteDataFilesCompactor
	// RequireResidencyPolicy keeps real object IO fail-closed unless tests inject
	// a scoped ObjectIOProvider. The default mirrors append/DML runtime behavior.
	RequireResidencyPolicy bool

	// UseNativeRewriteManifests is deliberately opt-in. The default SQL path
	// stays catalog-backed until native snapshot metadata updates are complete.
	UseNativeRewriteManifests bool
	// UseNativeRewriteDataFiles is deliberately opt-in. The default SQL path
	// stays catalog-backed. When enabled without DataFileCompactor, MO uses the
	// built-in append-only Parquet concat compactor, which rejects delete manifests.
	UseNativeRewriteDataFiles bool
	// UseNativeExpireSnapshots enumerates manifest, data, delete, and deletion-vector
	// objects before recording post-commit cleanup candidates.
	UseNativeExpireSnapshots bool
}

func NewMaintenanceProcedureExecutor(store MaintenanceStore, opts MaintenanceProcedureExecutorOptions) maintenance.ProcedureExecutor {
	orphanTTL := opts.OrphanTTL
	if orphanTTL <= 0 {
		orphanTTL = opts.Config.Write.OrphanTTL
	}
	orphanRecorder := OrphanFileRecorder{DAO: store}
	rewriteManifestsRunner := maintenance.Runner(maintenance.CatalogRewriteManifestsRunner{
		CatalogFactory: opts.CatalogFactory,
		OrphanRecorder: orphanRecorder,
		CommitVerifier: opts.CommitVerifier,
		Now:            opts.Now,
		OrphanTTL:      orphanTTL,
	})
	if opts.UseNativeRewriteManifests {
		rewriteManifestsRunner = NativeRewriteManifestsProcedureRunner{
			CatalogFactory:   opts.CatalogFactory,
			ObjectIOProvider: opts.ObjectIOProvider,
			ScopeForLocation: opts.ScopeForLocation,
			BuildFileService: opts.BuildFileService,
			ResidencyPolicies: append([]model.ResidencyPolicy(nil),
				opts.ResidencyPolicies...),
			ResidencyPolicyLister:  maintenanceResidencyPolicyLister(store),
			RequireResidencyPolicy: opts.RequireResidencyPolicy,
			OrphanRecorder:         orphanRecorder,
			CommitVerifier:         opts.CommitVerifier,
			Now:                    opts.Now,
			OrphanTTL:              orphanTTL,
		}
	}
	rewriteDataFilesRunner := maintenance.Runner(maintenance.CatalogRewriteDataFilesRunner{
		CatalogFactory: opts.CatalogFactory,
		OrphanRecorder: orphanRecorder,
		CommitVerifier: opts.CommitVerifier,
		Now:            opts.Now,
		OrphanTTL:      orphanTTL,
	})
	if opts.UseNativeRewriteDataFiles {
		rewriteDataFilesRunner = NativeRewriteDataFilesProcedureRunner{
			CatalogFactory:   opts.CatalogFactory,
			ObjectIOProvider: opts.ObjectIOProvider,
			ScopeForLocation: opts.ScopeForLocation,
			BuildFileService: opts.BuildFileService,
			ResidencyPolicies: append([]model.ResidencyPolicy(nil),
				opts.ResidencyPolicies...),
			ResidencyPolicyLister:  maintenanceResidencyPolicyLister(store),
			RequireResidencyPolicy: opts.RequireResidencyPolicy,
			Compactor:              opts.DataFileCompactor,
			OrphanRecorder:         orphanRecorder,
			CommitVerifier:         opts.CommitVerifier,
			Now:                    opts.Now,
			OrphanTTL:              orphanTTL,
		}
	}
	expireSnapshotsRunner := maintenance.Runner(maintenance.CatalogExpireSnapshotsRunner{
		CatalogFactory:    opts.CatalogFactory,
		OrphanRecorder:    orphanRecorder,
		CommitVerifier:    opts.CommitVerifier,
		Now:               opts.Now,
		OrphanTTL:         orphanTTL,
		DefaultRetainLast: opts.DefaultRetainLast,
	})
	if opts.UseNativeExpireSnapshots {
		expireSnapshotsRunner = NativeExpireSnapshotsProcedureRunner{
			CatalogFactory:         opts.CatalogFactory,
			ObjectIOProvider:       opts.ObjectIOProvider,
			ScopeForLocation:       opts.ScopeForLocation,
			BuildFileService:       opts.BuildFileService,
			ResidencyPolicies:      append([]model.ResidencyPolicy(nil), opts.ResidencyPolicies...),
			ResidencyPolicyLister:  maintenanceResidencyPolicyLister(store),
			RequireResidencyPolicy: opts.RequireResidencyPolicy,
			OrphanRecorder:         orphanRecorder,
			CommitVerifier:         opts.CommitVerifier,
			Now:                    opts.Now,
			OrphanTTL:              orphanTTL,
			DefaultRetainLast:      opts.DefaultRetainLast,
		}
	}
	return maintenance.ProcedureExecutor{
		Resolver: MaintenanceCatalogResolver{DAO: store},
		Authorizer: maintenance.FeatureAuthorizer{
			Config:  opts.Config,
			Account: opts.Account,
		},
		Access: MaintenanceCatalogAccessChecker{Store: store},
		Dispatcher: maintenance.Dispatcher{
			Runners: map[maintenance.Operation]maintenance.Runner{
				maintenance.OperationRewriteDataFiles: rewriteDataFilesRunner,
				maintenance.OperationExpireSnapshots:  expireSnapshotsRunner,
				maintenance.OperationRewriteManifests: rewriteManifestsRunner,
			},
			Recorder: store,
			Now:      opts.Now,
		},
	}
}

type MaintenanceCatalogAccessChecker struct {
	Store MaintenanceStore
}

func (c MaintenanceCatalogAccessChecker) CheckMaintenanceCatalogAccess(ctx context.Context, req maintenance.ProcedureExecutionRequest, catalog maintenance.ProcedureCatalogResolution) (maintenance.ProcedureCatalogAccess, error) {
	access, err := checkRuntimeCatalogAccess(ctx, c.Store, req.AccountID, catalog.Catalog, req.RoleID, req.UserID)
	if err != nil {
		return maintenance.ProcedureCatalogAccess{}, err
	}
	return maintenance.ProcedureCatalogAccess{
		ExternalPrincipal: access.Decision.ExternalPrincipal,
		ResidencyPolicies: append([]model.ResidencyPolicy(nil), access.ResidencyPolicies...),
	}, nil
}

func NewMaintenanceProcedureExecutorFromInternalSQL(execSQL InternalSQLExecutorAdapter, opts MaintenanceProcedureExecutorOptions) maintenance.ProcedureExecutor {
	return NewMaintenanceProcedureExecutor(NewDAO(execSQL), opts)
}

func NewMaintenanceProcedureExecutorFromInternalSQLExecutor(execSQL internalexecutor.SQLExecutor, opts MaintenanceProcedureExecutorOptions) maintenance.ProcedureExecutor {
	return NewMaintenanceProcedureExecutorFromInternalSQL(InternalSQLExecutorAdapter{Executor: execSQL}, opts)
}

var _ MaintenanceStore = (*DAO)(nil)
var _ CatalogByNameGetter = (*DAO)(nil)
var _ OrphanFileInserter = (*DAO)(nil)
var _ maintenance.JobRecorder = (*DAO)(nil)

type NativeExpireSnapshotsProcedureRunner struct {
	CatalogFactory         maintenance.CatalogClientFactory
	ObjectIOProvider       icebergio.ObjectIOProvider
	ScopeForLocation       icebergio.ObjectScopeForLocation
	BuildFileService       icebergio.ScopedFileServiceBuilder
	ResidencyPolicies      []model.ResidencyPolicy
	ResidencyPolicyLister  ResidencyPolicyLister
	RequireResidencyPolicy bool
	OrphanRecorder         icebergwrite.OrphanRecorder
	CommitVerifier         maintenance.CommitResultVerifier
	Now                    func() time.Time
	OrphanTTL              time.Duration
	DefaultRetainLast      int
}

func (r NativeExpireSnapshotsProcedureRunner) RunMaintenance(ctx context.Context, req maintenance.Request) (maintenance.Result, error) {
	if r.CatalogFactory == nil {
		return maintenance.Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg native expire-snapshots runner requires a catalog client factory", map[string]string{"operation": string(req.Operation)})
	}
	if req.Catalog.CatalogID == 0 {
		return maintenance.Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg native expire-snapshots runner requires resolved catalog metadata", map[string]string{"operation": string(req.Operation)})
	}
	client, err := r.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return maintenance.Result{}, err
	}
	catalogReq, err := resolveRuntimeCatalogRequestPrefix(ctx, client, api.CatalogRequest{
		Catalog:           req.Catalog,
		ExternalPrincipal: strings.TrimSpace(req.ExternalPrincipal),
	})
	if err != nil {
		return maintenance.Result{}, err
	}
	loadResp, tableMeta, err := loadMaintenanceTableWithResponse(ctx, client, catalogReq, req)
	if err != nil {
		return maintenance.Result{}, err
	}
	ioCtx, err := maintenanceObjectIOContext(ctx, maintenanceObjectIORequest{
		Client:                 client,
		CatalogRequest:         catalogReq,
		LoadResponse:           loadResp,
		Namespace:              dottedNamespace(req.Namespace),
		Table:                  req.Table,
		Catalog:                req.Catalog,
		ObjectIOProvider:       r.ObjectIOProvider,
		ScopeForLocation:       r.ScopeForLocation,
		BuildFileService:       r.BuildFileService,
		ResidencyPolicies:      mergeMaintenanceResidencyPolicies(r.ResidencyPolicies, req.ResidencyPolicies),
		ResidencyPolicyLister:  r.ResidencyPolicyLister,
		RequireResidencyPolicy: r.RequireResidencyPolicy,
		Principal:              firstNonEmpty(req.ExternalPrincipal, "matrixone-maintenance"),
	})
	if err != nil {
		return maintenance.Result{}, err
	}
	reader := icebergio.ProviderObjectReader{
		Provider:         ioCtx.ReaderProvider,
		ScopeForLocation: ioCtx.ScopeForLocation,
	}
	return maintenance.CommitRunner{
		Planner: maintenance.ExpireSnapshotsPlanner{
			Catalog: catalogReq,
			Loader: maintenance.MaintenanceTableMetadataLoaderFunc(func(context.Context, maintenance.Request) (*api.TableMetadata, error) {
				return tableMeta, nil
			}),
			ObjectReader:      reader,
			Now:               r.Now,
			DefaultRetainLast: r.DefaultRetainLast,
		},
		Committer:      client,
		Verifier:       r.CommitVerifier,
		OrphanRecorder: r.OrphanRecorder,
		Now:            r.Now,
		OrphanTTL:      r.OrphanTTL,
	}.RunMaintenance(ctx, req)
}

type NativeRewriteManifestsProcedureRunner struct {
	CatalogFactory         maintenance.CatalogClientFactory
	ObjectIOProvider       icebergio.ObjectIOProvider
	ScopeForLocation       icebergio.ObjectScopeForLocation
	BuildFileService       icebergio.ScopedFileServiceBuilder
	ResidencyPolicies      []model.ResidencyPolicy
	ResidencyPolicyLister  ResidencyPolicyLister
	RequireResidencyPolicy bool
	OrphanRecorder         icebergwrite.OrphanRecorder
	CommitVerifier         maintenance.CommitResultVerifier
	Now                    func() time.Time
	OrphanTTL              time.Duration
}

func (r NativeRewriteManifestsProcedureRunner) RunMaintenance(ctx context.Context, req maintenance.Request) (maintenance.Result, error) {
	if r.CatalogFactory == nil {
		return maintenance.Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-manifests runner requires a catalog client factory", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if req.Catalog.CatalogID == 0 {
		return maintenance.Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-manifests runner requires resolved catalog metadata", map[string]string{
			"operation": string(req.Operation),
		})
	}
	client, err := r.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return maintenance.Result{}, err
	}
	catalogReq, err := resolveRuntimeCatalogRequestPrefix(ctx, client, api.CatalogRequest{
		Catalog:           req.Catalog,
		ExternalPrincipal: strings.TrimSpace(req.ExternalPrincipal),
	})
	if err != nil {
		return maintenance.Result{}, err
	}
	loadResp, tableMeta, err := loadMaintenanceTableWithResponse(ctx, client, catalogReq, req)
	if err != nil {
		return maintenance.Result{}, err
	}
	ioCtx, err := maintenanceObjectIOContext(ctx, maintenanceObjectIORequest{
		Client:                 client,
		CatalogRequest:         catalogReq,
		LoadResponse:           loadResp,
		Namespace:              dottedNamespace(req.Namespace),
		Table:                  req.Table,
		Catalog:                req.Catalog,
		ObjectIOProvider:       r.ObjectIOProvider,
		ScopeForLocation:       r.ScopeForLocation,
		BuildFileService:       r.BuildFileService,
		ResidencyPolicies:      mergeMaintenanceResidencyPolicies(r.ResidencyPolicies, req.ResidencyPolicies),
		ResidencyPolicyLister:  r.ResidencyPolicyLister,
		RequireResidencyPolicy: r.RequireResidencyPolicy,
		Principal:              firstNonEmpty(req.ExternalPrincipal, "matrixone-maintenance"),
	})
	if err != nil {
		return maintenance.Result{}, err
	}
	reader := icebergio.ProviderObjectReader{
		Provider:         ioCtx.ReaderProvider,
		ScopeForLocation: ioCtx.ScopeForLocation,
	}
	writer := icebergio.ProviderObjectWriter{
		Provider:         ioCtx.WriterProvider,
		ScopeForLocation: ioCtx.ScopeForLocation,
	}
	return maintenance.CommitRunner{
		Planner: maintenance.NativeRewriteManifestsPlanner{
			Catalog: catalogReq,
			Loader: maintenance.MaintenanceTableMetadataLoaderFunc(func(context.Context, maintenance.Request) (*api.TableMetadata, error) {
				return tableMeta, nil
			}),
			Now: r.Now,
			Materializer: maintenance.RewriteManifestsMaterializer{
				ObjectReader: reader,
			},
		},
		ObjectWriter:   writer,
		Committer:      client,
		Verifier:       r.CommitVerifier,
		OrphanRecorder: r.OrphanRecorder,
		Now:            r.Now,
		OrphanTTL:      r.OrphanTTL,
	}.RunMaintenance(ctx, req)
}

type NativeRewriteDataFilesProcedureRunner struct {
	CatalogFactory         maintenance.CatalogClientFactory
	ObjectIOProvider       icebergio.ObjectIOProvider
	ScopeForLocation       icebergio.ObjectScopeForLocation
	BuildFileService       icebergio.ScopedFileServiceBuilder
	ResidencyPolicies      []model.ResidencyPolicy
	ResidencyPolicyLister  ResidencyPolicyLister
	RequireResidencyPolicy bool
	Compactor              maintenance.RewriteDataFilesCompactor
	OrphanRecorder         icebergwrite.OrphanRecorder
	CommitVerifier         maintenance.CommitResultVerifier
	Now                    func() time.Time
	OrphanTTL              time.Duration
}

func (r NativeRewriteDataFilesProcedureRunner) RunMaintenance(ctx context.Context, req maintenance.Request) (maintenance.Result, error) {
	if r.CatalogFactory == nil {
		return maintenance.Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-data-files runner requires a catalog client factory", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if req.Catalog.CatalogID == 0 {
		return maintenance.Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-data-files runner requires resolved catalog metadata", map[string]string{
			"operation": string(req.Operation),
		})
	}
	client, err := r.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return maintenance.Result{}, err
	}
	catalogReq, err := resolveRuntimeCatalogRequestPrefix(ctx, client, api.CatalogRequest{
		Catalog:           req.Catalog,
		ExternalPrincipal: strings.TrimSpace(req.ExternalPrincipal),
	})
	if err != nil {
		return maintenance.Result{}, err
	}
	loadResp, tableMeta, err := loadMaintenanceTableWithResponse(ctx, client, catalogReq, req)
	if err != nil {
		return maintenance.Result{}, err
	}
	ioCtx, err := maintenanceObjectIOContext(ctx, maintenanceObjectIORequest{
		Client:                 client,
		CatalogRequest:         catalogReq,
		LoadResponse:           loadResp,
		Namespace:              dottedNamespace(req.Namespace),
		Table:                  req.Table,
		Catalog:                req.Catalog,
		ObjectIOProvider:       r.ObjectIOProvider,
		ScopeForLocation:       r.ScopeForLocation,
		BuildFileService:       r.BuildFileService,
		ResidencyPolicies:      mergeMaintenanceResidencyPolicies(r.ResidencyPolicies, req.ResidencyPolicies),
		ResidencyPolicyLister:  r.ResidencyPolicyLister,
		RequireResidencyPolicy: r.RequireResidencyPolicy,
		Principal:              firstNonEmpty(req.ExternalPrincipal, "matrixone-maintenance"),
	})
	if err != nil {
		return maintenance.Result{}, err
	}
	reader := icebergio.ProviderObjectReader{
		Provider:         ioCtx.ReaderProvider,
		ScopeForLocation: ioCtx.ScopeForLocation,
	}
	writer := icebergio.ProviderObjectWriter{
		Provider:         ioCtx.WriterProvider,
		ScopeForLocation: ioCtx.ScopeForLocation,
	}
	compactor := r.Compactor
	if compactor == nil {
		compactor = maintenance.ParquetConcatRewriteDataFilesCompactor{
			ObjectReader: reader,
		}
	}
	return maintenance.CommitRunner{
		Planner: maintenance.NativeRewriteDataFilesPlanner{
			Catalog: catalogReq,
			Loader: maintenance.MaintenanceTableMetadataLoaderFunc(func(context.Context, maintenance.Request) (*api.TableMetadata, error) {
				return tableMeta, nil
			}),
			Now: r.Now,
			Selector: maintenance.RewriteDataFilesSelector{
				ObjectReader: reader,
			},
			Compactor: compactor,
		},
		ObjectWriter:   writer,
		Committer:      client,
		Verifier:       r.CommitVerifier,
		OrphanRecorder: r.OrphanRecorder,
		Now:            r.Now,
		OrphanTTL:      r.OrphanTTL,
	}.RunMaintenance(ctx, req)
}

type maintenanceObjectIORequest struct {
	Client                 api.CatalogClient
	CatalogRequest         api.CatalogRequest
	LoadResponse           *api.LoadTableResponse
	Namespace              api.Namespace
	Table                  string
	Catalog                model.Catalog
	ObjectIOProvider       icebergio.ObjectIOProvider
	ScopeForLocation       icebergio.ObjectScopeForLocation
	BuildFileService       icebergio.ScopedFileServiceBuilder
	ResidencyPolicies      []model.ResidencyPolicy
	ResidencyPolicyLister  ResidencyPolicyLister
	RequireResidencyPolicy bool
	Principal              string
}

func maintenanceObjectIOContext(ctx context.Context, req maintenanceObjectIORequest) (appendObjectIOContext, error) {
	if req.ObjectIOProvider != nil {
		return appendObjectIOContext{
			WriterProvider:   req.ObjectIOProvider,
			ReaderProvider:   req.ObjectIOProvider,
			ScopeForLocation: req.ScopeForLocation,
		}, nil
	}
	var credentials []api.StorageCredential
	if req.LoadResponse != nil {
		credentials = append(credentials, req.LoadResponse.StorageCredentials...)
	}
	if len(credentials) == 0 {
		if req.Client == nil {
			return appendObjectIOContext{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance object IO requires a catalog client", map[string]string{
				"catalog": req.Catalog.Name,
				"table":   req.Table,
			})
		}
		creds, err := req.Client.LoadCredentials(ctx, api.LoadCredentialsRequest{
			CatalogRequest: req.CatalogRequest,
			Namespace:      req.Namespace,
			Table:          req.Table,
		})
		if err != nil {
			return appendObjectIOContext{}, err
		}
		if creds != nil {
			credentials = append(credentials, creds.StorageCredentials...)
		}
	}
	buildFileService := req.BuildFileService
	if buildFileService == nil {
		buildFileService = icebergio.NewS3VendedFileServiceBuilder().Build
	}
	principal := strings.TrimSpace(req.Principal)
	if principal == "" {
		principal = "matrixone-maintenance"
	}
	baseScope := icebergio.ObjectScope{
		AccountID: req.Catalog.AccountID,
		CatalogID: req.Catalog.CatalogID,
		Principal: principal,
	}
	scopeForLocation := req.ScopeForLocation
	if scopeForLocation == nil {
		scopeForLocation = icebergio.S3ObjectScopeForLocation(baseScope, credentials)
	}
	policies := append([]model.ResidencyPolicy(nil), req.ResidencyPolicies...)
	if len(policies) == 0 && req.ResidencyPolicyLister != nil {
		loaded, err := req.ResidencyPolicyLister.ListResidencyPolicies(ctx, req.Catalog.AccountID, req.Catalog.CatalogID)
		if err != nil {
			return appendObjectIOContext{}, err
		}
		policies = append(policies, loaded...)
	}
	residencyValidator := ObjectScopeResidencyValidator(policies, req.Catalog.URI)
	vendedCredentials := filterS3AccessCredentials(credentials)
	if len(vendedCredentials) == 0 {
		if req.LoadResponse != nil && req.LoadResponse.Capabilities.RemoteSigning {
			signerFactory, ok := req.Client.(interface {
				NewRemoteSigner(api.CatalogRequest, map[string]string) icebergio.RemoteSigner
			})
			if !ok {
				return appendObjectIOContext{}, api.NewError(api.ErrRemoteSigningDenied, "Iceberg maintenance catalog client cannot create remote signer", map[string]string{
					"catalog": req.Catalog.Name,
					"table":   req.Table,
				})
			}
			signer := signerFactory.NewRemoteSigner(req.CatalogRequest, req.LoadResponse.Config)
			if signer == nil {
				return appendObjectIOContext{}, api.NewError(api.ErrRemoteSigningDenied, "Iceberg maintenance catalog client returned empty remote signer", map[string]string{
					"catalog": req.Catalog.Name,
					"table":   req.Table,
				})
			}
			signedScopeForLocation := req.ScopeForLocation
			if signedScopeForLocation == nil {
				signedScopeForLocation = icebergio.S3ObjectScopeForConfig(baseScope, req.LoadResponse.Config)
			}
			buildSignedFileService := icebergio.SignedHTTPFileServiceBuilder{}.Build
			return appendObjectIOContext{
				WriterProvider: icebergio.RemoteSigningProvider{
					Signer:                 signer,
					BuildFileService:       buildSignedFileService,
					Method:                 http.MethodPut,
					ResidencyValidator:     residencyValidator,
					RequireResidencyPolicy: true,
				},
				ReaderProvider: icebergio.RemoteSigningProvider{
					Signer:                 signer,
					BuildFileService:       buildSignedFileService,
					Method:                 http.MethodGet,
					ResidencyValidator:     residencyValidator,
					RequireResidencyPolicy: true,
				},
				ScopeForLocation: signedScopeForLocation,
			}, nil
		}
		return appendObjectIOContext{}, api.NewError(api.ErrCredentialExpired, "Iceberg maintenance catalog did not return usable storage credentials", map[string]string{
			"catalog": req.Catalog.Name,
			"table":   req.Table,
		})
	}
	provider := icebergio.VendedCredentialProvider{
		Credentials:            vendedCredentials,
		BuildFileService:       buildFileService,
		ResidencyValidator:     residencyValidator,
		RequireResidencyPolicy: true,
	}
	if req.RequireResidencyPolicy {
		provider.RequireResidencyPolicy = true
	}
	return appendObjectIOContext{
		WriterProvider:   provider,
		ReaderProvider:   provider,
		ScopeForLocation: scopeForLocation,
	}, nil
}

func loadMaintenanceTableWithResponse(ctx context.Context, client api.CatalogClient, catalogReq api.CatalogRequest, req maintenance.Request) (*api.LoadTableResponse, *api.TableMetadata, error) {
	if client == nil {
		return nil, nil, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance native runner requires a catalog client", nil)
	}
	resp, err := client.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: catalogReq,
		Namespace:      dottedNamespace(req.Namespace),
		Table:          req.Table,
		Snapshots:      "all",
	})
	if err != nil {
		return nil, nil, err
	}
	if resp == nil || len(resp.MetadataJSON) == 0 {
		return nil, nil, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance catalog load did not return metadata JSON", map[string]string{"table": req.Table})
	}
	meta, err := metadata.ParseTableMetadata(resp.MetadataJSON, resp.MetadataLocation)
	if err != nil {
		return nil, nil, err
	}
	return resp, meta, nil
}

func maintenanceResidencyPolicyLister(store MaintenanceStore) ResidencyPolicyLister {
	lister, _ := store.(ResidencyPolicyLister)
	return lister
}

func mergeMaintenanceResidencyPolicies(primary, checked []model.ResidencyPolicy) []model.ResidencyPolicy {
	out := make([]model.ResidencyPolicy, 0, len(primary)+len(checked))
	out = append(out, primary...)
	out = append(out, checked...)
	return out
}

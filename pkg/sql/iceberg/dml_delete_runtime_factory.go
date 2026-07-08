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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergref "github.com/matrixorigin/matrixone/pkg/iceberg/ref"
	icebergwritecore "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	internalexecutor "github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type DMLDeleteRuntimeCoordinatorStore interface {
	CatalogByNameGetter
	DMLCommitWorkflowStore
}

type DMLDeleteSnapshotIDFunc func(time.Time, *api.TableMetadata) int64

type DMLDeleteRuntimeCoordinatorFactoryOptions struct {
	Store                  DMLDeleteRuntimeCoordinatorStore
	CatalogFactory         icebergcatalog.ClientFactory
	Config                 api.Config
	Now                    func() time.Time
	SnapshotID             DMLDeleteSnapshotIDFunc
	CommitVerifier         dml.CommitVerifier
	MetricsReporter        api.MetricsReporter
	CacheInvalidator       icebergwritecore.CacheInvalidator
	BuildFileService       icebergio.ScopedFileServiceBuilder
	ObjectIOProvider       icebergio.ObjectIOProvider
	ScopeForLocation       icebergio.ObjectScopeForLocation
	ResidencyPolicies      []model.ResidencyPolicy
	AllowTagMove           bool
	RequireResidencyPolicy bool
	StatementIDPrefix      string
}

type DMLDeleteRuntimeCoordinatorFactory struct {
	opts   DMLDeleteRuntimeCoordinatorFactoryOptions
	shared *dmlRuntimeCoordinatorCache
}

func NewDMLDeleteRuntimeCoordinatorFactory(opts DMLDeleteRuntimeCoordinatorFactoryOptions) DMLDeleteRuntimeCoordinatorFactory {
	return DMLDeleteRuntimeCoordinatorFactory{
		opts: opts,
		shared: &dmlRuntimeCoordinatorCache{
			entries: make(map[string]icebergwrite.Coordinator),
		},
	}
}

func NewDMLDeleteRuntimeCoordinatorFactoryFromInternalSQLExecutor(
	exec internalexecutor.SQLExecutor,
	opts DMLDeleteRuntimeCoordinatorFactoryOptions,
) DMLDeleteRuntimeCoordinatorFactory {
	if opts.Store == nil {
		opts.Store = NewDAO(InternalSQLExecutorAdapter{Executor: exec})
	}
	return NewDMLDeleteRuntimeCoordinatorFactory(opts)
}

func (f DMLDeleteRuntimeCoordinatorFactory) NewCoordinator(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
	if req.Operation != icebergwrite.OperationDelete &&
		req.Operation != icebergwrite.OperationUpdate &&
		req.Operation != icebergwrite.OperationMerge &&
		req.Operation != icebergwrite.OperationOverwrite {
		return nil, nil
	}
	if req.Operation == icebergwrite.OperationOverwrite && f.shared != nil {
		if key := dmlRuntimeCoordinatorCacheKey(req); key != "" {
			return f.shared.getOrCreate(ctx, key, func() (icebergwrite.Coordinator, error) {
				return f.newCoordinator(ctx, req)
			})
		}
	}
	return f.newCoordinator(ctx, req)
}

func (f DMLDeleteRuntimeCoordinatorFactory) newCoordinator(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
	if err := f.validateRuntimeRequest(ctx, req); err != nil {
		return nil, err
	}
	catalogModel, err := f.opts.Store.GetCatalogByName(ctx, req.AccountID, req.CatalogName)
	if err != nil {
		return nil, err
	}
	access, err := checkRuntimeCatalogAccess(ctx, f.opts.Store, req.AccountID, catalogModel, req.RoleID, req.UserID)
	if err != nil {
		return nil, err
	}
	req.ExternalPrincipal = access.Decision.ExternalPrincipal
	catalogCaps, err := icebergcatalog.ParseCapabilitiesJSON(catalogModel.CapabilitiesJSON)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	client, err := f.opts.CatalogFactory.NewClient(ctx, catalogModel)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	namespace := dottedNamespace(req.Namespace)
	rawTargetRef := firstNonEmpty(req.DMLScan.Ref, req.DefaultRef, model.DefaultRefMain)
	catalogReq, targetRef, targetRefType, err := resolveRuntimeCatalogRequestPrefixForWriteRef(ctx, client, api.CatalogRequest{
		Catalog:           catalogModel,
		ExternalPrincipal: req.ExternalPrincipal,
	}, rawTargetRef, catalogCaps, f.opts.AllowTagMove)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	loadResp, err := client.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: catalogReq,
		Namespace:      namespace,
		Table:          req.Table,
		Snapshots:      "all",
	})
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	catalogReq.TableToken = loadResp.TableToken
	metadataLocation := strings.TrimSpace(loadResp.MetadataLocation)
	if metadataLocation == "" && len(req.DMLScan.DataFiles) > 0 {
		metadataLocation = req.DMLScan.DataFiles[0].FilePath
	}
	tableMeta, err := metadata.ParseTableMetadata(loadResp.MetadataJSON, metadataLocation)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	schema, err := dmlDeleteSchemaForRequest(ctx, tableMeta, req.DMLScan.BaseSchemaID)
	if err != nil {
		return nil, err
	}
	baseSnapshot, ok := metadata.FindSnapshot(tableMeta, req.DMLScan.BaseSnapshotID)
	if !ok {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML base snapshot is no longer present in table metadata", map[string]string{
			"table": req.Table,
			"ref":   firstNonEmpty(req.DMLScan.Ref, req.DefaultRef, model.DefaultRefMain),
		}))
	}
	scanObjectIO := objectIORefDMLObjectWriter{ObjectIORef: req.DMLScan.ObjectIORef}
	preservedManifests, preservedSources, err := readDMLBaseManifests(ctx, scanObjectIO, baseSnapshot)
	if err != nil {
		return nil, err
	}
	caps := mergeCatalogCapabilities(catalogCaps, loadResp.Capabilities)
	if targetRef == "" {
		targetRef, targetRefType, err = resolveDMLTargetRef(ctx, tableMeta, rawTargetRef, caps, f.opts.AllowTagMove)
		if err != nil {
			return nil, err
		}
	}
	objectWriter := interface {
		dml.DeleteObjectWriter
		dml.ManifestObjectWriter
	}(scanObjectIO)
	if objectIO, ok, err := f.dmlWriteObjectIOContext(ctx, client, catalogReq, loadResp, namespace, req.Table, catalogModel); err != nil {
		return nil, err
	} else if ok {
		objectWriter = icebergio.ProviderObjectWriter{
			Provider:         objectIO.WriterProvider,
			ScopeForLocation: objectIO.ScopeForLocation,
		}
	}
	now := f.now()
	workflow := NewDMLCommitWorkflow(f.opts.Store, DMLCommitWorkflowOptions{
		Config:           f.effectiveConfig(req.AccountID),
		ManifestWriter:   objectWriter,
		Committer:        client,
		CommitVerifier:   f.opts.CommitVerifier,
		CacheInvalidator: f.opts.CacheInvalidator,
		MetricsReporter:  f.opts.MetricsReporter,
		Now:              f.opts.Now,
	})
	executor := DMLActionExecutor{
		Workflow:       workflow,
		Catalog:        catalogReq,
		TableLocation:  tableMeta.Location,
		SnapshotID:     f.nextSnapshotID(now, tableMeta),
		SequenceNumber: nextDMLSequenceNumber(tableMeta),
		TimestampMS:    now.UnixMilli(),
		PreservedManifests: append([]api.ManifestFile(nil),
			preservedManifests...),
		PreservedSources: append([]dml.PreservedManifestSource(nil),
			preservedSources...),
	}
	base := dml.CommitBase{
		Namespace:           namespace,
		Table:               strings.TrimSpace(req.Table),
		TargetRef:           targetRef,
		TargetRefType:       targetRefType,
		AllowTagMove:        f.opts.AllowTagMove,
		CatalogCapabilities: caps,
		BaseSnapshotID:      req.DMLScan.BaseSnapshotID,
		TableUUID:           tableMeta.TableUUID,
		BaseSchemaID:        schema.SchemaID,
		IdempotencyKey:      firstNonEmpty(req.IdempotencyKey, req.StatementID),
		StatementID:         firstNonEmpty(req.StatementID, req.IdempotencyKey),
	}
	partitionSpec, _ := tableMeta.DefaultSpec()
	base.BaseSpecID = partitionSpec.SpecID
	if req.Operation == icebergwrite.OperationOverwrite {
		scope := overwriteScopeOrDefault(dml.OverwriteScope(req.DMLScan.OverwriteScope))
		affectedFiles := append([]api.DataFile(nil), req.DMLScan.DataFiles...)
		if scope == dml.OverwritePartition {
			if err := validateOverwritePartitionKeys(ctx, req.DMLScan.OverwritePartition, partitionSpec, req.Table); err != nil {
				return nil, err
			}
			var filterErr error
			affectedFiles, filterErr = filterOverwritePartitionDataFiles(ctx, affectedFiles, req.DMLScan.OverwritePartition, req.Table)
			if filterErr != nil {
				return nil, filterErr
			}
		}
		return NewDMLOverwriteCoordinator(DMLOverwriteCoordinatorSpec{
			Committer:         executor,
			Base:              base,
			Schema:            schema,
			ObjectWriter:      objectWriter,
			AffectedDataFiles: affectedFiles,
			PartitionSpec:     partitionSpec,
			Scope:             scope,
			Partition:         cloneDMLAnyMap(req.DMLScan.OverwritePartition),
		}), nil
	}
	if req.Operation == icebergwrite.OperationMerge {
		return NewDMLMergeCoordinator(DMLMergeCoordinatorSpec{
			Committer:      executor,
			Base:           base,
			Schema:         schema,
			DeleteSchemaID: schema.SchemaID,
			ObjectWriter:   objectWriter,
			DataFiles:      append([]api.DataFile(nil), req.DMLScan.DataFiles...),
			PartitionSpec:  partitionSpec,
		}), nil
	}
	if req.Operation == icebergwrite.OperationUpdate {
		return NewDMLUpdateCoordinator(DMLUpdateCoordinatorSpec{
			Committer:      executor,
			Base:           base,
			Schema:         schema,
			DeleteSchemaID: schema.SchemaID,
			ObjectWriter:   objectWriter,
			DataFiles:      append([]api.DataFile(nil), req.DMLScan.DataFiles...),
			PartitionSpec:  partitionSpec,
		}), nil
	}
	spec := DMLDeleteCoordinatorSpec{
		Committer:           executor,
		Base:                base,
		Schema:              schema,
		DeleteSchemaID:      schema.SchemaID,
		ObjectWriter:        objectWriter,
		DataFiles:           append([]api.DataFile(nil), req.DMLScan.DataFiles...),
		IncludePositionRows: true,
	}
	return NewDMLDeleteCoordinator(spec), nil
}

type dmlRuntimeCoordinatorCache struct {
	mu      sync.Mutex
	entries map[string]icebergwrite.Coordinator
}

func (c *dmlRuntimeCoordinatorCache) getOrCreate(ctx context.Context, key string, build func() (icebergwrite.Coordinator, error)) (icebergwrite.Coordinator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if coord := c.entries[key]; coord != nil {
		return coord, nil
	}
	coord, err := build()
	if err != nil {
		return nil, err
	}
	shared := &dmlRuntimeSharedCoordinator{inner: coord}
	shared.release = func() {
		c.release(key, shared)
	}
	c.entries[key] = shared
	return shared, nil
}

func (c *dmlRuntimeCoordinatorCache) release(key string, coord icebergwrite.Coordinator) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[key] == coord {
		delete(c.entries, key)
	}
}

type dmlRuntimeSharedCoordinator struct {
	inner   icebergwrite.Coordinator
	release func()
	done    sync.Once
}

func (c *dmlRuntimeSharedCoordinator) Begin(ctx context.Context, req icebergwrite.AppendRequest) error {
	return c.inner.Begin(ctx, req)
}

func (c *dmlRuntimeSharedCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	return c.inner.Append(ctx, bat)
}

func (c *dmlRuntimeSharedCoordinator) AppendWithProcess(proc *process.Process, bat *batch.Batch) error {
	if processAware, ok := c.inner.(icebergwrite.ProcessAwareCoordinator); ok {
		return processAware.AppendWithProcess(proc, bat)
	}
	if proc == nil {
		return c.inner.Append(context.Background(), bat)
	}
	return c.inner.Append(proc.Ctx, bat)
}

func (c *dmlRuntimeSharedCoordinator) Commit(ctx context.Context) error {
	err := c.inner.Commit(ctx)
	if state, ok := c.inner.(interface{ CommitAttempted() bool }); ok && state.CommitAttempted() {
		c.releaseOnce()
	}
	return err
}

func (c *dmlRuntimeSharedCoordinator) Abort(ctx context.Context, cause error) error {
	err := c.inner.Abort(ctx, cause)
	c.releaseOnce()
	return err
}

func (c *dmlRuntimeSharedCoordinator) releaseOnce() {
	c.done.Do(func() {
		if c.release != nil {
			c.release()
		}
	})
}

func dmlRuntimeCoordinatorCacheKey(req icebergwrite.AppendRequest) string {
	statementKey := strings.TrimSpace(firstNonEmpty(req.IdempotencyKey, req.StatementID))
	if statementKey == "" {
		return ""
	}
	ref := strings.TrimSpace(firstNonEmpty(req.DMLScan.Ref, req.DefaultRef, model.DefaultRefMain))
	return strings.Join([]string{
		strconv.FormatUint(uint64(req.AccountID), 10),
		strconv.FormatUint(req.RoleID, 10),
		strconv.FormatUint(req.UserID, 10),
		strings.TrimSpace(req.ExternalPrincipal),
		strings.TrimSpace(req.Operation),
		strings.TrimSpace(req.CatalogName),
		strings.TrimSpace(req.Namespace),
		strings.TrimSpace(req.Table),
		ref,
		statementKey,
	}, "\x1f")
}

func (f DMLDeleteRuntimeCoordinatorFactory) dmlWriteObjectIOContext(
	ctx context.Context,
	client api.CatalogClient,
	catalogReq api.CatalogRequest,
	loadResp *api.LoadTableResponse,
	namespace api.Namespace,
	table string,
	catalog model.Catalog,
) (appendObjectIOContext, bool, error) {
	if f.opts.ObjectIOProvider != nil {
		return appendObjectIOContext{
			WriterProvider:   f.opts.ObjectIOProvider,
			ReaderProvider:   f.opts.ObjectIOProvider,
			ScopeForLocation: f.opts.ScopeForLocation,
		}, true, nil
	}
	var credentials []api.StorageCredential
	if loadResp != nil {
		credentials = append(credentials, loadResp.StorageCredentials...)
	}
	if len(credentials) == 0 {
		creds, err := client.LoadCredentials(ctx, api.LoadCredentialsRequest{
			CatalogRequest: catalogReq,
			Namespace:      namespace,
			Table:          table,
		})
		if err != nil {
			return appendObjectIOContext{}, false, api.ToMOErr(ctx, err)
		}
		if creds != nil {
			credentials = append(credentials, creds.StorageCredentials...)
		}
	}
	buildFileService := f.opts.BuildFileService
	if buildFileService == nil {
		buildFileService = icebergio.NewS3VendedFileServiceBuilder().Build
	}
	baseScope := icebergio.ObjectScope{
		AccountID: catalog.AccountID,
		CatalogID: catalog.CatalogID,
		Principal: firstNonEmpty(catalogReq.ExternalPrincipal, "matrixone-dml"),
	}
	scopeForLocation := f.opts.ScopeForLocation
	if scopeForLocation == nil {
		scopeForLocation = icebergio.S3ObjectScopeForLocation(baseScope, credentials)
	}
	policies := append([]model.ResidencyPolicy(nil), f.opts.ResidencyPolicies...)
	if len(policies) == 0 {
		if lister, ok := f.opts.Store.(ResidencyPolicyLister); ok {
			loaded, err := lister.ListResidencyPolicies(ctx, catalog.AccountID, catalog.CatalogID)
			if err != nil {
				return appendObjectIOContext{}, false, err
			}
			policies = append(policies, loaded...)
		}
	}
	residencyValidator := ObjectScopeResidencyValidator(policies, catalog.URI)
	vendedCredentials := filterS3AccessCredentials(credentials)
	if len(vendedCredentials) > 0 {
		provider := icebergio.VendedCredentialProvider{
			Credentials:            vendedCredentials,
			BuildFileService:       buildFileService,
			Now:                    f.opts.Now,
			ResidencyValidator:     residencyValidator,
			RequireResidencyPolicy: f.requireResidencyPolicy(),
		}
		return appendObjectIOContext{
			WriterProvider:   provider,
			ReaderProvider:   provider,
			ScopeForLocation: scopeForLocation,
		}, true, nil
	}
	if loadResp != nil && loadResp.Capabilities.RemoteSigning {
		signerFactory, ok := client.(interface {
			NewRemoteSigner(api.CatalogRequest, map[string]string) icebergio.RemoteSigner
		})
		if !ok {
			return appendObjectIOContext{}, false, api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg DML catalog client cannot create remote signer", map[string]string{
				"catalog": catalog.Name,
				"table":   table,
			}))
		}
		signer := signerFactory.NewRemoteSigner(catalogReq, loadResp.Config)
		if signer == nil {
			return appendObjectIOContext{}, false, api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg DML catalog client returned empty remote signer", map[string]string{
				"catalog": catalog.Name,
				"table":   table,
			}))
		}
		scopeForLocation = f.opts.ScopeForLocation
		if scopeForLocation == nil {
			scopeForLocation = icebergio.S3ObjectScopeForConfig(baseScope, loadResp.Config)
		}
		buildSignedFileService := icebergio.SignedHTTPFileServiceBuilder{}.Build
		return appendObjectIOContext{
			WriterProvider: icebergio.RemoteSigningProvider{
				Signer:                 signer,
				BuildFileService:       buildSignedFileService,
				Method:                 http.MethodPut,
				Now:                    f.opts.Now,
				ResidencyValidator:     residencyValidator,
				RequireResidencyPolicy: f.requireResidencyPolicy(),
			},
			ReaderProvider: icebergio.RemoteSigningProvider{
				Signer:                 signer,
				BuildFileService:       buildSignedFileService,
				Method:                 http.MethodGet,
				Now:                    f.opts.Now,
				ResidencyValidator:     residencyValidator,
				RequireResidencyPolicy: f.requireResidencyPolicy(),
			},
			ScopeForLocation: scopeForLocation,
		}, true, nil
	}
	return appendObjectIOContext{}, false, nil
}

func (f DMLDeleteRuntimeCoordinatorFactory) validateRuntimeRequest(ctx context.Context, req icebergwrite.AppendRequest) error {
	if f.opts.Store == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML runtime coordinator requires a store", nil))
	}
	if f.opts.CatalogFactory == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML runtime coordinator requires a catalog factory", nil))
	}
	cfg := f.effectiveConfig(req.AccountID)
	if !cfg.Enable || !cfg.Write.EnableWrite || !cfg.Write.EnableDML || !cfg.Write.EnableDelete {
		return api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML is disabled by configuration", nil))
	}
	if strings.TrimSpace(req.CatalogName) == "" || strings.TrimSpace(req.Namespace) == "" || strings.TrimSpace(req.Table) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML requires catalog, namespace, and table", nil))
	}
	if req.DMLScan.BaseSnapshotID <= 0 || req.DMLScan.BaseSchemaID < 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML requires compiled base snapshot and schema metadata", map[string]string{
			"table": req.Table,
		}))
	}
	if len(req.DMLScan.DataFiles) == 0 && req.Operation != icebergwrite.OperationOverwrite {
		return api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML requires planned data files", map[string]string{
			"table": req.Table,
		}))
	}
	if strings.TrimSpace(req.DMLScan.ObjectIORef) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML requires object IO ref", map[string]string{
			"table": req.Table,
		}))
	}
	if strings.TrimSpace(firstNonEmpty(req.IdempotencyKey, req.StatementID)) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML requires a statement idempotency key", map[string]string{
			"table": req.Table,
		}))
	}
	return nil
}

func (f DMLDeleteRuntimeCoordinatorFactory) effectiveConfig(accountID uint32) api.Config {
	return f.opts.Config.EffectiveForAccount(api.AccountConfig{AccountID: accountID, Enable: true})
}

func (f DMLDeleteRuntimeCoordinatorFactory) requireResidencyPolicy() bool {
	return f.opts.ObjectIOProvider == nil
}

func (f DMLDeleteRuntimeCoordinatorFactory) now() time.Time {
	if f.opts.Now != nil {
		return f.opts.Now()
	}
	return time.Now()
}

func (f DMLDeleteRuntimeCoordinatorFactory) nextSnapshotID(now time.Time, meta *api.TableMetadata) int64 {
	if f.opts.SnapshotID != nil {
		return f.opts.SnapshotID(now, meta)
	}
	return nextDMLSnapshotID(now, meta)
}

type objectIORefDMLObjectWriter struct {
	ObjectIORef string
}

func (w objectIORefDMLObjectWriter) WriteObject(ctx context.Context, location string, payload []byte) error {
	return w.write(ctx, location, payload)
}

func (w objectIORefDMLObjectWriter) WriteManifestObject(ctx context.Context, location string, payload []byte) error {
	return w.write(ctx, location, payload)
}

func (w objectIORefDMLObjectWriter) ReadManifestObject(ctx context.Context, location string) ([]byte, error) {
	location = strings.TrimSpace(location)
	if location == "" {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg DML object reader requires location", nil))
	}
	fs, filePath, err := icebergio.ResolveObjectIORef(ctx, w.ObjectIORef, location)
	if err != nil {
		return nil, err
	}
	if fs == nil || strings.TrimSpace(filePath) == "" {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg DML object IO ref resolved to an empty file service path", map[string]string{
			"location": api.RedactPath(location),
		}))
	}
	vec := fileservice.IOVector{
		FilePath: strings.TrimSpace(filePath),
		Policy:   fileservice.SkipFullFilePreloads,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	if err := fs.Read(ctx, &vec); err != nil {
		return nil, api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg DML object reader failed to read object", map[string]string{
			"location": api.RedactPath(location),
		}, err))
	}
	if len(vec.Entries) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg DML object reader returned no entries", map[string]string{
			"location": api.RedactPath(location),
		}))
	}
	return append([]byte(nil), vec.Entries[0].Data...), nil
}

func (w objectIORefDMLObjectWriter) write(ctx context.Context, location string, payload []byte) error {
	location = strings.TrimSpace(location)
	if location == "" || len(payload) == 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg DML object writer requires a non-empty location and payload", nil))
	}
	fs, filePath, err := icebergio.ResolveObjectIORef(ctx, w.ObjectIORef, location)
	if err != nil {
		return err
	}
	if fs == nil || strings.TrimSpace(filePath) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg DML object IO ref resolved to an empty file service path", map[string]string{
			"location": api.RedactPath(location),
		}))
	}
	data := append([]byte(nil), payload...)
	if err := fs.Write(ctx, fileservice.IOVector{
		FilePath: filePath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   data,
		}},
	}); err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg DML object writer failed to write object", map[string]string{
			"location": api.RedactPath(location),
		}, err))
	}
	return nil
}

func dmlDeleteSchemaForRequest(ctx context.Context, meta *api.TableMetadata, schemaID int) (api.Schema, error) {
	if meta == nil {
		return api.Schema{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML DELETE metadata is empty", nil))
	}
	if schemaID > 0 {
		for _, schema := range meta.Schemas {
			if schema.SchemaID == schemaID {
				return schema, nil
			}
		}
		return api.Schema{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML DELETE scan schema id was not found in table metadata", nil))
	}
	if schema, ok := meta.CurrentSchema(); ok {
		return schema, nil
	}
	return api.Schema{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML DELETE table metadata has no current schema", nil))
}

func readDMLBaseManifests(ctx context.Context, reader objectIORefDMLObjectWriter, snapshot api.Snapshot) ([]api.ManifestFile, []dml.PreservedManifestSource, error) {
	manifestListPath := strings.TrimSpace(snapshot.ManifestList)
	if manifestListPath == "" {
		return nil, nil, nil
	}
	data, err := reader.ReadManifestObject(ctx, manifestListPath)
	if err != nil {
		return nil, nil, err
	}
	manifests, err := metadata.ReadManifestList(data)
	if err != nil {
		return nil, nil, api.ToMOErr(ctx, err)
	}
	sources := make([]dml.PreservedManifestSource, 0, len(manifests))
	for _, manifest := range manifests {
		if manifest.Content != "" && manifest.Content != api.ManifestContentData {
			continue
		}
		manifestPath := strings.TrimSpace(manifest.Path)
		if manifestPath == "" {
			return nil, nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML base manifest has no path", map[string]string{
				"manifest_list": api.RedactPath(manifestListPath),
			}))
		}
		manifestData, err := reader.ReadManifestObject(ctx, manifestPath)
		if err != nil {
			return nil, nil, err
		}
		entries, err := metadata.ReadManifest(manifestData)
		if err != nil {
			return nil, nil, api.ToMOErr(ctx, err)
		}
		sources = append(sources, dml.PreservedManifestSource{
			Manifest: manifest,
			Entries:  entries,
		})
	}
	return append([]api.ManifestFile(nil), manifests...), sources, nil
}

func resolveDMLTargetRef(ctx context.Context, meta *api.TableMetadata, raw string, caps api.CatalogCapabilities, allowTagMove bool) (string, string, error) {
	spec, err := icebergref.ParseNessieRef(raw, meta)
	if err != nil {
		return "", "", api.ToMOErr(ctx, err)
	}
	if strings.TrimSpace(spec.Name) == "" {
		spec.Name = model.DefaultRefMain
	}
	if spec.Type == "" {
		spec.Type = icebergref.TypeBranch
	}
	if err := icebergref.ValidateWrite(spec, caps, allowTagMove); err != nil {
		return "", "", api.ToMOErr(ctx, err)
	}
	return spec.Name, string(spec.Type), nil
}

func dottedNamespace(namespace string) api.Namespace {
	parts := strings.Split(namespace, ".")
	out := make(api.Namespace, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func nextDMLSnapshotID(now time.Time, meta *api.TableMetadata) int64 {
	candidate := now.UnixNano()
	if candidate <= 0 {
		candidate = time.Now().UnixNano()
	}
	maxSnapshotID := int64(0)
	if meta != nil {
		if meta.CurrentSnapshotID != nil && *meta.CurrentSnapshotID > maxSnapshotID {
			maxSnapshotID = *meta.CurrentSnapshotID
		}
		for _, snapshot := range meta.Snapshots {
			if snapshot.SnapshotID > maxSnapshotID {
				maxSnapshotID = snapshot.SnapshotID
			}
		}
	}
	if candidate <= maxSnapshotID {
		return maxSnapshotID + 1
	}
	return candidate
}

func nextDMLSequenceNumber(meta *api.TableMetadata) int64 {
	next := int64(1)
	if meta == nil {
		return next
	}
	if meta.LastSequenceNumber >= next {
		next = meta.LastSequenceNumber + 1
	}
	for _, snapshot := range meta.Snapshots {
		if snapshot.SequenceNumber >= next {
			next = snapshot.SequenceNumber + 1
		}
	}
	return next
}

func mergeCatalogCapabilities(left, right api.CatalogCapabilities) api.CatalogCapabilities {
	return api.CatalogCapabilities{
		CredentialVending:  left.CredentialVending || right.CredentialVending,
		RemoteSigning:      left.RemoteSigning || right.RemoteSigning,
		ServerSidePlanning: left.ServerSidePlanning || right.ServerSidePlanning,
		BranchTag:          left.BranchTag || right.BranchTag,
		Commit:             left.Commit || right.Commit,
		CreateTable:        left.CreateTable || right.CreateTable,
		MetricsReport:      left.MetricsReport || right.MetricsReport,
	}
}

var _ icebergwrite.CoordinatorFactory = DMLDeleteRuntimeCoordinatorFactory{}

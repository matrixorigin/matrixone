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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwritecore "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	internalexecutor "github.com/matrixorigin/matrixone/pkg/util/executor"
)

type TableMappingGetter interface {
	GetTableMapping(ctx context.Context, accountID uint32, databaseID uint64, tableID uint64) (model.TableMapping, error)
}

type ResidencyPolicyLister interface {
	ListResidencyPolicies(ctx context.Context, accountID uint32, catalogID uint64) ([]model.ResidencyPolicy, error)
}

type AppendRuntimeCoordinatorStore interface {
	CatalogByNameGetter
	TableMappingGetter
	DMLCommitWorkflowStore
}

type AppendRuntimeSnapshotIDFunc func(time.Time, *api.TableMetadata) int64

type AppendRuntimeCoordinatorFactoryOptions struct {
	Store                  AppendRuntimeCoordinatorStore
	CatalogFactory         icebergcatalog.ClientFactory
	Config                 api.Config
	Now                    func() time.Time
	SnapshotID             AppendRuntimeSnapshotIDFunc
	CommitVerifier         icebergwritecore.CommitVerifier
	MetricsReporter        api.MetricsReporter
	CacheInvalidator       icebergwritecore.CacheInvalidator
	AllowTagMove           bool
	TargetFileSizeBytes    int64
	BuildFileService       icebergio.ScopedFileServiceBuilder
	ObjectIOProvider       icebergio.ObjectIOProvider
	ScopeForLocation       icebergio.ObjectScopeForLocation
	ResidencyPolicies      []model.ResidencyPolicy
	RequireResidencyPolicy bool
}

type AppendRuntimeCoordinatorFactory struct {
	opts AppendRuntimeCoordinatorFactoryOptions
}

type appendObjectIOContext struct {
	WriterProvider   icebergio.ObjectIOProvider
	ReaderProvider   icebergio.ObjectIOProvider
	ScopeForLocation icebergio.ObjectScopeForLocation
}

func NewAppendRuntimeCoordinatorFactory(opts AppendRuntimeCoordinatorFactoryOptions) AppendRuntimeCoordinatorFactory {
	return AppendRuntimeCoordinatorFactory{opts: opts}
}

func NewAppendRuntimeCoordinatorFactoryFromInternalSQLExecutor(
	exec internalexecutor.SQLExecutor,
	opts AppendRuntimeCoordinatorFactoryOptions,
) AppendRuntimeCoordinatorFactory {
	if opts.Store == nil {
		opts.Store = NewDAO(InternalSQLExecutorAdapter{Executor: exec})
	}
	return NewAppendRuntimeCoordinatorFactory(opts)
}

func (f AppendRuntimeCoordinatorFactory) NewCoordinator(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
	if req.Operation != "" && req.Operation != icebergwrite.OperationAppend {
		return nil, nil
	}
	if err := f.validateRuntimeRequest(ctx, req); err != nil {
		return nil, err
	}
	catalogModel, err := f.opts.Store.GetCatalogByName(ctx, req.AccountID, req.CatalogName)
	if err != nil {
		return nil, err
	}
	mapping, err := f.tableMapping(ctx, req, catalogModel)
	if err != nil {
		return nil, err
	}
	catalogCaps, err := icebergcatalog.ParseCapabilitiesJSON(catalogModel.CapabilitiesJSON)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	client, err := f.opts.CatalogFactory.NewClient(ctx, catalogModel)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	namespace := dottedNamespace(firstNonEmpty(mapping.Namespace, req.Namespace))
	rawTargetRef := firstNonEmpty(req.DefaultRef, mapping.DefaultRef, model.DefaultRefMain)
	catalogReq, targetRef, targetRefType, err := resolveRuntimeCatalogRequestPrefixForWriteRef(ctx, client, api.CatalogRequest{Catalog: catalogModel}, rawTargetRef, catalogCaps, f.opts.AllowTagMove)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	loadResp, err := client.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: catalogReq,
		Namespace:      namespace,
		Table:          firstNonEmpty(mapping.TableName, req.Table),
		Snapshots:      "all",
	})
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	catalogReq.TableToken = loadResp.TableToken
	tableMeta, err := metadata.ParseTableMetadata(loadResp.MetadataJSON, strings.TrimSpace(loadResp.MetadataLocation))
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	schema, err := appendCurrentSchema(ctx, tableMeta, req.Table)
	if err != nil {
		return nil, err
	}
	spec, err := appendDefaultSpec(ctx, tableMeta, req.Table)
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
	baseSnapshot, baseSnapshotID, err := appendBaseSnapshot(ctx, tableMeta, targetRef)
	if err != nil {
		return nil, err
	}
	objectIO, err := f.objectIOContext(ctx, client, catalogReq, loadResp, namespace, firstNonEmpty(mapping.TableName, req.Table), catalogModel)
	if err != nil {
		return nil, err
	}
	objectWriter := icebergio.ProviderObjectWriter{Provider: objectIO.WriterProvider, ScopeForLocation: objectIO.ScopeForLocation}
	objectReader := icebergio.ProviderObjectReader{Provider: objectIO.ReaderProvider, ScopeForLocation: objectIO.ScopeForLocation}
	preserved, err := readAppendBaseManifestList(ctx, objectReader, baseSnapshot)
	if err != nil {
		return nil, err
	}
	now := f.now()
	snapshotID := f.nextSnapshotID(now, tableMeta)
	sequenceNumber := nextDMLSequenceNumber(tableMeta)
	writerID := "append-" + api.PathHash(firstNonEmpty(req.StatementID, req.IdempotencyKey))
	manifestPaths, err := buildAppendManifestPaths(ctx, tableMeta.Location, firstNonEmpty(req.StatementID, req.IdempotencyKey), snapshotID)
	if err != nil {
		return nil, err
	}
	writer, err := icebergwritecore.NewFanoutParquetDataWriter(ctx, icebergwritecore.FanoutWriterConfig{
		Schema:              schema,
		PartitionSpec:       spec,
		TableLocation:       strings.TrimRight(strings.TrimSpace(tableMeta.Location), "/"),
		DataDir:             appendDataDir(firstNonEmpty(req.StatementID, req.IdempotencyKey), snapshotID),
		WriterID:            writerID,
		TargetFileSizeBytes: f.opts.TargetFileSizeBytes,
		TimeZone:            time.UTC,
	}, bufferedDataFileOutputFactory{Writer: objectWriter})
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	builder := &appendManifestWritingBuilder{
		ManifestAttemptBuilder: icebergwritecore.ManifestAttemptBuilder{
			SnapshotID:         snapshotID,
			SequenceNumber:     sequenceNumber,
			TimestampMS:        now.UnixMilli(),
			ManifestPath:       manifestPaths.ManifestPath,
			ManifestListPath:   manifestPaths.ManifestListPath,
			PreservedManifests: preserved,
		},
		Writer: objectWriter,
	}
	workflow := icebergwritecore.AppendWorkflow{
		Builder:          builder,
		Committer:        client,
		Verifier:         f.opts.CommitVerifier,
		OrphanRecorder:   OrphanFileRecorder{DAO: f.opts.Store},
		AuditRecorder:    PublishAuditRecorder{DAO: f.opts.Store},
		CacheInvalidator: f.opts.CacheInvalidator,
		MetricsReporter:  appendMetricsReporter(f.opts.MetricsReporter, client),
		Now:              f.opts.Now,
		OrphanTTL:        f.effectiveConfig(req.AccountID).Write.OrphanTTL,
	}
	appendReq, err := icebergwritecore.BuildAppendRequest(ctx, icebergwritecore.AppendRequestSpec{
		CatalogRequest:       catalogReq,
		Namespace:            namespace,
		Table:                firstNonEmpty(mapping.TableName, req.Table),
		TableLocation:        tableMeta.Location,
		TargetRef:            targetRef,
		TargetRefType:        targetRefType,
		AllowTagMove:         f.opts.AllowTagMove,
		CatalogCapabilities:  caps,
		TableUUID:            tableMeta.TableUUID,
		BaseSnapshotID:       baseSnapshotID,
		BaseSchemaID:         schema.SchemaID,
		BaseSpecID:           spec.SpecID,
		BaseSchema:           schema,
		BaseSpec:             spec,
		KnownPartitionSpecs:  append([]api.PartitionSpec(nil), tableMeta.PartitionSpecs...),
		WriterOwnerAccountID: mapping.WriterOwnerAccountID,
		IdempotencyKey:       firstNonEmpty(req.IdempotencyKey, req.StatementID),
		SourceKind:           icebergwritecore.AppendSourceSQLInsert,
		SourceQueryID:        req.StatementID,
		WriterID:             writerID,
		StatementID:          req.StatementID,
		Summary: map[string]string{
			"mo-operation": string(icebergwrite.OperationAppend),
		},
		PublishAuditHint: api.PublishAuditHint{
			JobID:       firstNonEmpty(req.StatementID, req.IdempotencyKey),
			SourceBatch: firstNonEmpty(req.StatementID, req.IdempotencyKey),
		},
	})
	if err != nil {
		_ = writer.Abort(ctx)
		return nil, api.ToMOErr(ctx, err)
	}
	return &AppendRuntimeCoordinator{
		req:        req,
		appendReq:  appendReq,
		writer:     writer,
		workflow:   workflow,
		closed:     false,
		committed:  false,
		dataFiles:  nil,
		objectRef:  "",
		releaseRef: nil,
	}, nil
}

func (f AppendRuntimeCoordinatorFactory) validateRuntimeRequest(ctx context.Context, req icebergwrite.AppendRequest) error {
	if f.opts.Store == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append runtime coordinator requires a store", nil))
	}
	if f.opts.CatalogFactory == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append runtime coordinator requires a catalog factory", nil))
	}
	cfg := f.effectiveConfig(req.AccountID)
	if !cfg.Enable || !cfg.Write.EnableWrite {
		return api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg append write is disabled by configuration", nil))
	}
	if strings.TrimSpace(req.CatalogName) == "" || strings.TrimSpace(req.Namespace) == "" || strings.TrimSpace(req.Table) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append write requires catalog, namespace, and table", nil))
	}
	if strings.TrimSpace(firstNonEmpty(req.IdempotencyKey, req.StatementID)) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append write requires a statement idempotency key", map[string]string{
			"table": req.Table,
		}))
	}
	if req.TableDef == nil || req.TableDef.GetDbId() == 0 || req.TableDef.GetTblId() == 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append write requires table mapping object ids", map[string]string{
			"table": req.Table,
		}))
	}
	return nil
}

func (f AppendRuntimeCoordinatorFactory) tableMapping(ctx context.Context, req icebergwrite.AppendRequest, catalog model.Catalog) (model.TableMapping, error) {
	mapping, err := f.opts.Store.GetTableMapping(ctx, req.AccountID, req.TableDef.GetDbId(), req.TableDef.GetTblId())
	if err != nil {
		return model.TableMapping{}, err
	}
	if mapping.CatalogID != catalog.CatalogID {
		return model.TableMapping{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg append table mapping catalog does not match catalog name", map[string]string{
			"table": req.Table,
		}))
	}
	if mapping.WriteMode != model.WriteModeAppendOnly && mapping.WriteMode != model.WriteModeMergeOnRead {
		return model.TableMapping{}, api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg append target is not writable", map[string]string{
			"table":      req.Table,
			"write_mode": mapping.WriteMode,
		}))
	}
	return mapping, nil
}

func (f AppendRuntimeCoordinatorFactory) objectIOContext(
	ctx context.Context,
	client api.CatalogClient,
	catalogReq api.CatalogRequest,
	loadResp *api.LoadTableResponse,
	namespace api.Namespace,
	table string,
	catalog model.Catalog,
) (appendObjectIOContext, error) {
	if f.opts.ObjectIOProvider != nil {
		return appendObjectIOContext{
			WriterProvider:   f.opts.ObjectIOProvider,
			ReaderProvider:   f.opts.ObjectIOProvider,
			ScopeForLocation: f.opts.ScopeForLocation,
		}, nil
	}
	credentials := append([]api.StorageCredential(nil), loadResp.StorageCredentials...)
	if len(credentials) == 0 {
		creds, err := client.LoadCredentials(ctx, api.LoadCredentialsRequest{
			CatalogRequest: catalogReq,
			Namespace:      namespace,
			Table:          table,
		})
		if err != nil {
			return appendObjectIOContext{}, api.ToMOErr(ctx, err)
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
		Principal: "matrixone-append",
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
				return appendObjectIOContext{}, err
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
		}, nil
	}
	if loadResp != nil && loadResp.Capabilities.RemoteSigning {
		signerFactory, ok := client.(interface {
			NewRemoteSigner(api.CatalogRequest, map[string]string) icebergio.RemoteSigner
		})
		if !ok {
			return appendObjectIOContext{}, api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg append catalog client cannot create remote signer", map[string]string{
				"catalog": catalog.Name,
				"table":   table,
			}))
		}
		signer := signerFactory.NewRemoteSigner(catalogReq, loadResp.Config)
		if signer == nil {
			return appendObjectIOContext{}, api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg append catalog client returned empty remote signer", map[string]string{
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
		}, nil
	}
	return appendObjectIOContext{}, api.ToMOErr(ctx, api.NewError(api.ErrCredentialExpired, "Iceberg append catalog did not return usable storage credentials", map[string]string{
		"catalog": catalog.Name,
		"table":   table,
	}))
}

func (f AppendRuntimeCoordinatorFactory) requireResidencyPolicy() bool {
	if f.opts.ObjectIOProvider != nil {
		return false
	}
	if f.opts.RequireResidencyPolicy {
		return true
	}
	return true
}

func filterS3AccessCredentials(credentials []api.StorageCredential) []api.StorageCredential {
	if len(credentials) == 0 {
		return nil
	}
	out := make([]api.StorageCredential, 0, len(credentials))
	for _, credential := range credentials {
		cfg := make(map[string]string, len(credential.Config))
		for key, value := range credential.Config {
			cfg[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
		}
		if firstNonEmpty(cfg["s3.access-key-id"], cfg["s3.access_key_id"], cfg["aws.access-key-id"]) == "" {
			continue
		}
		if firstNonEmpty(cfg["s3.secret-access-key"], cfg["s3.secret_access_key"], cfg["aws.secret-access-key"]) == "" {
			continue
		}
		out = append(out, credential)
	}
	return out
}

func (f AppendRuntimeCoordinatorFactory) effectiveConfig(accountID uint32) api.Config {
	return f.opts.Config.EffectiveForAccount(api.AccountConfig{AccountID: accountID, Enable: true})
}

func (f AppendRuntimeCoordinatorFactory) now() time.Time {
	if f.opts.Now != nil {
		return f.opts.Now()
	}
	return time.Now()
}

func (f AppendRuntimeCoordinatorFactory) nextSnapshotID(now time.Time, meta *api.TableMetadata) int64 {
	if f.opts.SnapshotID != nil {
		return f.opts.SnapshotID(now, meta)
	}
	return nextDMLSnapshotID(now, meta)
}

type AppendRuntimeCoordinator struct {
	req        icebergwrite.AppendRequest
	appendReq  api.AppendRequest
	writer     *icebergwritecore.FanoutParquetDataWriter
	workflow   icebergwritecore.AppendWorkflow
	closed     bool
	committed  bool
	dataFiles  []api.DataFile
	objectRef  string
	releaseRef func()
}

func (c *AppendRuntimeCoordinator) Begin(ctx context.Context, req icebergwrite.AppendRequest) error {
	return nil
}

func (c *AppendRuntimeCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	if c == nil || c.writer == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append coordinator is not initialized", nil))
	}
	if c.closed {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append coordinator is already closed", map[string]string{
			"table": c.req.Table,
		}))
	}
	return api.ToMOErr(ctx, c.writer.WriteBatch(ctx, c.req.Attrs, appendRuntimeVisibleBatch(c.req.Attrs, bat)))
}

func appendRuntimeVisibleBatch(attrs []string, bat *batch.Batch) *batch.Batch {
	if bat == nil || len(attrs) == 0 || len(bat.Vecs) == len(attrs) {
		return bat
	}
	if len(bat.Vecs) < len(attrs) {
		return bat
	}
	visible := batch.NewWithSize(len(attrs))
	visible.Attrs = append([]string(nil), attrs...)
	if len(bat.Attrs) == len(bat.Vecs) {
		positions := make(map[string]int, len(bat.Attrs))
		for idx, attr := range bat.Attrs {
			key := strings.ToLower(strings.TrimSpace(attr))
			if key == "" {
				continue
			}
			if _, ok := positions[key]; !ok {
				positions[key] = idx
			}
		}
		matched := true
		for idx, attr := range attrs {
			pos, ok := positions[strings.ToLower(strings.TrimSpace(attr))]
			if !ok || pos < 0 || pos >= len(bat.Vecs) {
				matched = false
				break
			}
			visible.Vecs[idx] = bat.Vecs[pos]
		}
		if matched {
			visible.SetRowCount(bat.RowCount())
			return visible
		}
	}
	copy(visible.Vecs, bat.Vecs[:len(attrs)])
	visible.SetRowCount(bat.RowCount())
	return visible
}

func (c *AppendRuntimeCoordinator) Commit(ctx context.Context) error {
	if c == nil || c.writer == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append coordinator is not initialized", nil))
	}
	if c.committed {
		return nil
	}
	files, err := c.writer.Close(ctx)
	c.closed = true
	if err != nil {
		return api.ToMOErr(ctx, err)
	}
	c.dataFiles = append([]api.DataFile(nil), files...)
	if len(files) == 0 {
		c.committed = true
		return nil
	}
	req := c.appendReq
	req.DataFiles = append([]api.DataFile(nil), files...)
	if _, err := c.workflow.CommitAppend(ctx, req); err != nil {
		return api.ToMOErr(ctx, err)
	}
	c.committed = true
	return nil
}

func (c *AppendRuntimeCoordinator) Abort(ctx context.Context, cause error) error {
	if c == nil || c.writer == nil || c.closed || c.committed {
		return nil
	}
	c.closed = true
	return api.ToMOErr(ctx, c.writer.Abort(ctx))
}

type appendManifestWritingBuilder struct {
	icebergwritecore.ManifestAttemptBuilder
	Writer dml.ManifestObjectWriter
}

func (b *appendManifestWritingBuilder) BuildAppend(ctx context.Context, req api.AppendRequest) (*api.CommitAttempt, error) {
	attempt, err := b.ManifestAttemptBuilder.BuildAppend(ctx, req)
	if err != nil {
		return nil, err
	}
	if b.Writer == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg append manifest writer is not configured", nil)
	}
	result := b.ManifestAttemptBuilder.LastResult
	if result == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg append manifest builder did not return artifacts", nil)
	}
	if err := b.Writer.WriteManifestObject(ctx, result.ManifestFile.Path, result.ManifestBytes); err != nil {
		return nil, err
	}
	if err := b.Writer.WriteManifestObject(ctx, b.ManifestListPath, result.ManifestListBytes); err != nil {
		return nil, err
	}
	return attempt, nil
}

type appendManifestPaths struct {
	ManifestPath     string
	ManifestListPath string
}

func buildAppendManifestPaths(ctx context.Context, tableLocation, statementKey string, snapshotID int64) (appendManifestPaths, error) {
	tableLocation = strings.TrimRight(strings.TrimSpace(tableLocation), "/")
	if tableLocation == "" {
		return appendManifestPaths{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append manifest paths require table location", nil))
	}
	if strings.TrimSpace(statementKey) == "" || snapshotID <= 0 {
		return appendManifestPaths{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg append manifest paths require statement key and snapshot id", nil))
	}
	base := joinDMLObjectPath(tableLocation, "metadata", "mo-append", "stmt-"+api.PathHash(statementKey))
	return appendManifestPaths{
		ManifestPath:     joinDMLObjectPath(base, "data-manifest-snap-"+strconv.FormatInt(snapshotID, 10)+".avro"),
		ManifestListPath: joinDMLObjectPath(base, "manifest-list-snap-"+strconv.FormatInt(snapshotID, 10)+".avro"),
	}, nil
}

func appendDataDir(statementKey string, snapshotID int64) string {
	return joinDMLObjectPath("data", "mo-append", "stmt-"+api.PathHash(statementKey), "snap-"+strconv.FormatInt(snapshotID, 10))
}

func appendCurrentSchema(ctx context.Context, meta *api.TableMetadata, table string) (api.Schema, error) {
	if meta == nil {
		return api.Schema{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg append table metadata is empty", map[string]string{"table": table}))
	}
	schema, ok := meta.CurrentSchema()
	if !ok {
		return api.Schema{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg append table metadata has no current schema", map[string]string{"table": table}))
	}
	return schema, nil
}

func appendDefaultSpec(ctx context.Context, meta *api.TableMetadata, table string) (api.PartitionSpec, error) {
	if meta == nil {
		return api.PartitionSpec{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg append table metadata is empty", map[string]string{"table": table}))
	}
	spec, ok := meta.DefaultSpec()
	if !ok {
		return api.PartitionSpec{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg append table metadata has no default partition spec", map[string]string{"table": table}))
	}
	return spec, nil
}

func appendBaseSnapshot(ctx context.Context, meta *api.TableMetadata, ref string) (api.Snapshot, int64, error) {
	if meta == nil || meta.CurrentSnapshotID == nil {
		return api.Snapshot{}, 0, nil
	}
	snapshot, err := metadata.ResolveSnapshot(meta, metadata.SnapshotSelector{
		RefName:           firstNonEmpty(ref, model.DefaultRefMain),
		AllowMainFallback: true,
	})
	if err != nil {
		return api.Snapshot{}, 0, api.ToMOErr(ctx, err)
	}
	return snapshot, snapshot.SnapshotID, nil
}

func readAppendBaseManifestList(ctx context.Context, reader icebergio.ProviderObjectReader, snapshot api.Snapshot) ([]api.ManifestFile, error) {
	if strings.TrimSpace(snapshot.ManifestList) == "" {
		return nil, nil
	}
	data, err := reader.Read(ctx, snapshot.ManifestList, 0, -1)
	if err != nil {
		return nil, err
	}
	manifests, err := metadata.ReadManifestList(data)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	return append([]api.ManifestFile(nil), manifests...), nil
}

func appendMetricsReporter(configured api.MetricsReporter, client api.CatalogClient) api.MetricsReporter {
	if configured != nil {
		return configured
	}
	if reporter, ok := client.(api.MetricsReporter); ok {
		return reporter
	}
	return nil
}

var _ icebergwrite.CoordinatorFactory = AppendRuntimeCoordinatorFactory{}
var _ icebergwrite.Coordinator = (*AppendRuntimeCoordinator)(nil)

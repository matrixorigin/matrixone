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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type CatalogClientFactory interface {
	NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error)
}

type ObjectReaderContext struct {
	CredentialHash  string
	CredentialScope string
	ObjectIORef     string
}

type ObjectReaderFactory func(ctx context.Context, catalog api.CatalogClient, req api.ScanPlanRequest) (api.ObjectReader, ObjectReaderContext, error)

type CatalogRequestValidator = api.CatalogRequestValidator

type RuntimeScanPlanner struct {
	CatalogFactory    CatalogClientFactory
	CatalogValidator  CatalogRequestValidator
	Metadata          api.MetadataFacade
	ObjectReader      ObjectReaderFactory
	Cache             *Cache
	RefCacheRefresher RefCacheRefresher
	Config            api.Config
}

func (p RuntimeScanPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	if p.CatalogFactory == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg runtime scan planner requires catalog factory", nil)
	}
	if p.ObjectReader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg runtime scan planner requires object reader factory", nil)
	}
	catalogClient, err := p.CatalogFactory.NewClient(ctx, req.Catalog)
	if err != nil {
		return nil, err
	}
	if validator := combineCatalogValidators(p.CatalogValidator, req.CatalogValidator); validator != nil {
		catalogClient = validatingCatalogClient{upstream: catalogClient, validator: validator}
	}
	var refDisplay string
	req, refDisplay, err = p.resolveCatalogPrefix(ctx, catalogClient, req)
	if err != nil {
		return nil, err
	}
	objectReader, readerCtx, err := p.ObjectReader(ctx, catalogClient, req)
	if err != nil {
		return nil, err
	}
	objectIOHandedOff := false
	defer func() {
		if !objectIOHandedOff {
			icebergio.ReleaseObjectIORef(readerCtx.ObjectIORef)
		}
	}()
	metadataFacade := p.Metadata
	if metadataFacade == nil {
		metadataFacade = NativeFacade{}
	}
	scan := p.Config.Scan
	localPlanner := LocalScanPlanner{
		Catalog:                 catalogClient,
		Metadata:                metadataFacade,
		ObjectReader:            objectReader,
		Cache:                   p.Cache,
		RefCacheRefresher:       p.RefCacheRefresher,
		CredentialHash:          readerCtx.CredentialHash,
		CredentialScope:         readerCtx.CredentialScope,
		ManifestReadParallelism: scan.ManifestReadParallelism,
		MaxManifestFiles:        scan.MaxManifestFiles,
		MaxDataFiles:            scan.MaxDataFiles,
		PlanningTimeout:         scan.PlanningTimeout,
	}
	var planner api.ScanPlanner = localPlanner
	if scan.ServerPlanningMode != api.ServerPlanningOff {
		serverPlanner, _ := catalogClient.(api.ScanPlanner)
		planner = ServerPlanningPlanner{
			Server:                 serverPlanner,
			Client:                 localPlanner,
			Mode:                   scan.ServerPlanningMode,
			SupportsServerPlanning: catalogServerPlanningCapability(catalogClient),
		}
	}
	scanPlan, err := planner.PlanScan(ctx, req)
	if err != nil {
		return nil, err
	}
	if scanPlan == nil {
		return nil, api.NewError(api.ErrInternal, "Iceberg scan planner returned an empty plan", nil)
	}
	if refDisplay != "" {
		scanPlan.Snapshot.RefName = refDisplay
	}
	scanPlan.ObjectIORef = readerCtx.ObjectIORef
	scanPlan.DeleteMaxMemoryBytes = req.DeleteMaxMemoryBytes
	scanPlan.EnableDeleteSpill = req.EnableDeleteSpill
	p.reportScanMetrics(ctx, catalogClient, req, scanPlan)
	objectIOHandedOff = true
	return scanPlan, nil
}

func (p RuntimeScanPlanner) resolveCatalogPrefix(ctx context.Context, client api.CatalogClient, req api.ScanPlanRequest) (api.ScanPlanRequest, string, error) {
	if client == nil || strings.TrimSpace(req.Prefix) != "" {
		return req, "", nil
	}
	resp, err := client.GetConfig(ctx, api.GetConfigRequest{
		CatalogRequest: req.CatalogRequest,
		Warehouse:      req.Catalog.Warehouse,
	})
	if err != nil {
		return req, "", err
	}
	if resp != nil && strings.TrimSpace(resp.Prefix) != "" {
		req.Prefix = strings.TrimSpace(resp.Prefix)
	}
	refDisplay := ""
	if resp != nil && isNessieCatalogConfig(resp) {
		if rewritten, display, ok := rewriteNessieRefPrefix(req.Prefix, req.Ref, req.Snapshot.RefName); ok {
			req.Prefix = rewritten
			req.Ref = ""
			req.Snapshot.RefName = model.DefaultRefMain
			req.Snapshot.AllowMainFallback = true
			refDisplay = display
		}
	}
	return req, refDisplay, nil
}

func isNessieCatalogConfig(resp *api.ConfigResponse) bool {
	if resp == nil {
		return false
	}
	for _, cfg := range []map[string]string{resp.Overrides, resp.Defaults} {
		for key, value := range cfg {
			if strings.EqualFold(strings.TrimSpace(key), "nessie.is-nessie-catalog") &&
				strings.EqualFold(strings.TrimSpace(value), "true") {
				return true
			}
		}
	}
	return false
}

func rewriteNessieRefPrefix(prefix, requestRef, selectorRef string) (string, string, bool) {
	ref := strings.TrimSpace(requestRef)
	if ref == "" {
		ref = strings.TrimSpace(selectorRef)
	}
	if ref == "" || ref == model.DefaultRefMain {
		return prefix, "", false
	}
	refName := nessieRefName(ref)
	if refName == "" || refName == model.DefaultRefMain {
		return prefix, "", false
	}
	parts := strings.SplitN(strings.TrimSpace(prefix), "|", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
		plainPrefix := strings.TrimSpace(prefix)
		if plainPrefix == "" || plainPrefix == model.DefaultRefMain {
			return refName, ref, true
		}
		return prefix, "", false
	}
	return refName + "|" + parts[1], ref, true
}

func nessieRefName(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if typ, value, ok := strings.Cut(raw, ":"); ok {
		switch strings.ToLower(strings.TrimSpace(typ)) {
		case "branch", "tag", "hash":
			return strings.TrimSpace(value)
		}
	}
	return raw
}

func (p RuntimeScanPlanner) reportScanMetrics(ctx context.Context, client api.CatalogClient, req api.ScanPlanRequest, plan *api.IcebergScanPlan) {
	if plan == nil {
		return
	}
	reporter, ok := client.(api.MetricsReporter)
	if !ok {
		return
	}
	supported, err := catalogMetricsReportCapability(client)(ctx, req)
	if err != nil {
		logScanMetricsWarning("Iceberg scan metrics capability check failed", req, plan, err)
		return
	}
	if !supported {
		return
	}
	ref := strings.TrimSpace(req.Ref)
	if ref == "" {
		ref = strings.TrimSpace(req.Snapshot.RefName)
	}
	if ref == "" {
		ref = strings.TrimSpace(plan.Snapshot.RefName)
	}
	if ref == "" {
		ref = model.DefaultRefMain
	}
	if err := reporter.ReportMetrics(ctx, api.MetricsReportRequest{
		CatalogRequest:       req.CatalogRequest,
		Namespace:            append(api.Namespace(nil), req.Namespace...),
		Table:                req.Table,
		Ref:                  ref,
		SnapshotID:           plan.Snapshot.SnapshotID,
		Kind:                 api.MetricsReportScan,
		PlanningProfile:      plan.Profile,
		MetadataLocationHash: plan.Snapshot.MetadataLocationHash,
		Files:                len(plan.DataTasks),
	}); err != nil {
		logScanMetricsWarning("Iceberg scan metrics report failed", req, plan, err)
	}
}

func logScanMetricsWarning(message string, req api.ScanPlanRequest, plan *api.IcebergScanPlan, err error) {
	snapshotID := int64(0)
	if plan != nil {
		snapshotID = plan.Snapshot.SnapshotID
	}
	logutil.Warn(message,
		zap.Uint32("account-id", req.Catalog.AccountID),
		zap.Uint64("catalog-id", req.Catalog.CatalogID),
		zap.String("namespace", strings.Join(req.Namespace, ".")),
		zap.String("table", req.Table),
		zap.Int64("snapshot-id", snapshotID),
		zap.Error(err))
}

var _ api.ScanPlanner = RuntimeScanPlanner{}

func catalogServerPlanningCapability(client api.CatalogClient) ServerPlanningCapability {
	return func(ctx context.Context, req api.ScanPlanRequest) (bool, error) {
		if client == nil {
			return false, nil
		}
		resp, err := client.GetConfig(ctx, api.GetConfigRequest{
			CatalogRequest: req.CatalogRequest,
			Warehouse:      req.Catalog.Warehouse,
		})
		if err != nil {
			return false, err
		}
		registry := icebergcatalog.CapabilityRegistryFromConfig(resp)
		return registry.Supports(icebergcatalog.CapabilityServerSidePlanning), nil
	}
}

func catalogMetricsReportCapability(client api.CatalogClient) ServerPlanningCapability {
	return func(ctx context.Context, req api.ScanPlanRequest) (bool, error) {
		if client == nil {
			return false, nil
		}
		resp, err := client.GetConfig(ctx, api.GetConfigRequest{
			CatalogRequest: req.CatalogRequest,
			Warehouse:      req.Catalog.Warehouse,
		})
		if err != nil {
			return false, err
		}
		registry := icebergcatalog.CapabilityRegistryFromConfig(resp)
		return registry.Supports(icebergcatalog.CapabilityMetricsReport), nil
	}
}

func combineCatalogValidators(validators ...api.CatalogRequestValidator) api.CatalogRequestValidator {
	out := make([]api.CatalogRequestValidator, 0, len(validators))
	for _, validator := range validators {
		if validator != nil {
			out = append(out, validator)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return func(ctx context.Context, req api.CatalogRequest) error {
		for _, validator := range out {
			if err := validator(ctx, req); err != nil {
				return err
			}
		}
		return nil
	}
}

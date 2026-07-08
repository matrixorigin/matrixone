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

package compile

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const IcebergScanPlannerRuntimeKey = "iceberg.scan.planner"

func (c *Compile) compileIcebergScan(node *plan.Node, strictSqlMode bool) ([]*Scope, error) {
	return c.compileIcebergScanWithAccess(node, strictSqlMode, icebergScanAccessContext{})
}

func (c *Compile) compileIcebergScanWithAccess(node *plan.Node, strictSqlMode bool, access icebergScanAccessContext) ([]*Scope, error) {
	return c.compileIcebergScanWithAccessForPlanNode(-1, node, strictSqlMode, access)
}

func (c *Compile) compileIcebergScanWithAccessForPlanNode(planNodeID int32, node *plan.Node, strictSqlMode bool, access icebergScanAccessContext) ([]*Scope, error) {
	ctx := c.icebergSecurityContext()
	planner, err := c.icebergScanPlannerForCompile(ctx)
	if err != nil {
		return nil, err
	}
	if node == nil || node.ExternScan == nil || node.ExternScan.IcebergScan == nil {
		return nil, moerr.NewInvalidInput(ctx, "iceberg scan requires IcebergScan plan metadata")
	}
	icebergScan := node.ExternScan.IcebergScan
	req := icebergScanPlanRequest(icebergScan)
	applyIcebergScanAccessContext(&req, access)
	planningTimeout, err := c.icebergPlanningTimeoutHint(ctx)
	if err != nil {
		return nil, err
	}
	req.PlanningTimeout = planningTimeout
	if cfg, ok, cfgErr := c.icebergConfig(ctx); cfgErr != nil {
		return nil, cfgErr
	} else if ok {
		accountID, err := defines.GetAccountId(ctx)
		if err != nil {
			accountID = 0
		}
		if err := sqliceberg.EnsureFeatureEnabled(ctx, cfg, accountID, "Iceberg scan"); err != nil {
			return nil, err
		}
		req.DeleteMaxMemoryBytes = cfg.Write.DeleteMaxMemory
		req.EnableDeleteSpill = cfg.Write.EnableDeleteSpill
	}
	req.PrunePredicates = icebergPrunePredicatesFromNode(node)
	req.EnableRowGroupPlanning = len(req.PrunePredicates) > 0
	if filterDigest := icebergResidualFilterDigestFromNode(node); filterDigest != "" {
		req.ResidualSQL = "filter_digest:" + filterDigest
		icebergScan.FilterDigest = filterDigest
	}
	scanPlan, err := planner.PlanScan(ctx, req)
	if err != nil {
		return nil, icebergapi.ToMOErr(ctx, err)
	}
	applyIcebergExecutionOptionsToPlan(scanPlan, req)
	if fieldIDs := icebergFieldIDsByColumnMapping(node, scanPlan.ColumnMapping); len(fieldIDs) > 0 {
		icebergScan.ProjectedFieldIds = intSliceToInt32(fieldIDs)
		req.ProjectionIDs = append([]int(nil), fieldIDs...)
		if len(req.PrunePredicates) == 0 {
			req.PrunePredicates = icebergPrunePredicatesFromNodeWithFieldIDs(node, fieldIDs)
			req.EnableRowGroupPlanning = len(req.PrunePredicates) > 0
			if len(req.PrunePredicates) > 0 {
				scanPlan, err = planner.PlanScan(ctx, req)
				if err != nil {
					return nil, icebergapi.ToMOErr(ctx, err)
				}
				applyIcebergExecutionOptionsToPlan(scanPlan, req)
			}
		}
	}
	runtime, err := icebergScanPlanToRuntimeForTable(ctx, scanPlan, "", node.TableDef)
	if err != nil {
		return nil, err
	}
	updateIcebergScanStats(node, scanPlan)
	c.recordIcebergScanPlan(planNodeID, scanPlan)

	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.S3,
			Filepath: icebergFilepathHint(icebergScan),
			Format:   tree.PARQUET,
			Tail:     &tree.TailParameter{},
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_ICEBERG_TB),
			Ctx:        ctx,
			Parallel:   true,
		},
	}
	return c.compileExternScanIcebergFileFanout(node, param, runtime, strictSqlMode)
}

func applyIcebergScanAccessContext(req *icebergapi.ScanPlanRequest, access icebergScanAccessContext) {
	if req == nil {
		return
	}
	if access.catalog.CatalogID != 0 {
		req.Catalog = access.catalog
	}
	if strings.TrimSpace(access.externalPrincipal) != "" {
		req.ExternalPrincipal = strings.TrimSpace(access.externalPrincipal)
	}
	if req.Namespace == nil && strings.TrimSpace(access.table.Namespace) != "" {
		req.Namespace = icebergNamespaceFromPlan(access.table.Namespace)
	}
	if strings.TrimSpace(req.Table) == "" && strings.TrimSpace(access.table.TableName) != "" {
		req.Table = strings.TrimSpace(access.table.TableName)
	}
	if strings.TrimSpace(req.Ref) == "" && strings.TrimSpace(access.table.DefaultRef) != "" {
		req.Ref = strings.TrimSpace(access.table.DefaultRef)
		req.Snapshot.RefName = req.Ref
	}
	if strings.EqualFold(strings.TrimSpace(access.table.ReadMode), model.ReadModeMergeOnRead) {
		req.EnableDeleteApply = true
	}
	if len(access.residencyPolicies) > 0 {
		// Catalog metadata checks are request-scoped and only run when policy rows
		// are configured; object IO remains fail-closed in the default planner.
		req.ResidencyPolicies = append([]model.ResidencyPolicy(nil), access.residencyPolicies...)
		req.CatalogValidator = sqliceberg.CatalogRequestResidencyValidator(req.ResidencyPolicies)
		req.ObjectResidencyValidator = sqliceberg.ObjectResidencyRequestValidator(req.ResidencyPolicies, req.Catalog.URI)
	}
}

func (c *Compile) icebergScanPlannerForCompile(ctx context.Context) (icebergapi.ScanPlanner, error) {
	if c != nil && c.icebergScanPlanner != nil {
		return c.icebergScanPlanner, nil
	}
	if c != nil && c.proc != nil {
		if rt := moruntime.ServiceRuntime(c.proc.GetService()); rt != nil {
			value, ok := rt.GetGlobalVariables(IcebergScanPlannerRuntimeKey)
			if ok && value != nil {
				planner, ok := value.(icebergapi.ScanPlanner)
				if !ok {
					return nil, icebergapi.ToMOErr(ctx, icebergapi.NewError(icebergapi.ErrConfigInvalid, "Iceberg scan planner runtime variable has invalid type", nil))
				}
				return planner, nil
			}
		}
	}
	return nil, moerr.NewNotSupported(ctx, "Iceberg scan planner is not configured")
}

func (c *Compile) icebergPlanningTimeoutHint(ctx context.Context) (string, error) {
	cfg, ok, err := c.icebergConfig(ctx)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	if cfg.Scan.PlanningTimeout <= 0 {
		return "", nil
	}
	return cfg.Scan.PlanningTimeout.String(), nil
}

func icebergParameterUnitFromContext(ctx context.Context) *config.ParameterUnit {
	if ctx == nil {
		return nil
	}
	value := ctx.Value(config.ParameterUnitKey)
	pu, _ := value.(*config.ParameterUnit)
	return pu
}

func icebergScanPlanRequest(scan *plan.IcebergScan) icebergapi.ScanPlanRequest {
	req := icebergapi.ScanPlanRequest{
		CatalogRequest: icebergapi.CatalogRequest{
			Catalog: icebergapiCatalogFromPlan(scan),
		},
		Namespace:     icebergNamespaceFromPlan(scan.GetNamespace()),
		Table:         scan.GetTable(),
		Ref:           scan.GetRef(),
		ProjectionIDs: int32SliceToInt(scan.GetProjectedFieldIds()),
	}
	if scan.GetSnapshotId() != 0 {
		req.Snapshot.SnapshotID = scan.GetSnapshotId()
		req.Snapshot.HasSnapshotID = true
	}
	if scan.GetTimestampAsOf() != 0 {
		req.Snapshot.TimestampMS = scan.GetTimestampAsOf()
		req.Snapshot.HasTimestampMS = true
	}
	if req.Ref != "" {
		req.Snapshot.RefName = req.Ref
	}
	if strings.EqualFold(strings.TrimSpace(scan.GetReadMode()), model.ReadModeMergeOnRead) {
		req.EnableDeleteApply = true
	}
	return req
}

func icebergapiCatalogFromPlan(scan *plan.IcebergScan) model.Catalog {
	return model.Catalog{
		CatalogID: scan.GetCatalogId(),
	}
}

func icebergNamespaceFromPlan(namespace string) icebergapi.Namespace {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil
	}
	parts := strings.Split(namespace, ".")
	out := make(icebergapi.Namespace, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func int32SliceToInt(in []int32) []int {
	if len(in) == 0 {
		return nil
	}
	out := make([]int, 0, len(in))
	for _, value := range in {
		out = append(out, int(value))
	}
	return out
}

func updateIcebergScanStats(node *plan.Node, scanPlan *icebergapi.IcebergScanPlan) {
	if node == nil || scanPlan == nil {
		return
	}
	if node.Stats == nil {
		node.Stats = &plan.Stats{}
	}
	var bytes int64
	var records int64
	for _, task := range scanPlan.DataTasks {
		if task.DataFile.FileSizeInBytes > 0 {
			bytes += task.DataFile.FileSizeInBytes
		}
		if task.DataFile.RecordCount > 0 {
			records += task.DataFile.RecordCount
		}
	}
	if bytes <= 0 {
		bytes = int64(len(scanPlan.DataTasks))
	}
	node.Stats.BlockNum = int32(len(scanPlan.DataTasks))
	node.Stats.Cost = float64(bytes)
	if node.Stats.TableCnt == 0 {
		node.Stats.TableCnt = float64(records)
		if records == 0 {
			node.Stats.TableCnt = float64(len(scanPlan.DataTasks))
		}
	}
	if node.Stats.Outcnt == 0 {
		node.Stats.Outcnt = node.Stats.TableCnt
	}
	if node.Stats.Rowsize == 0 && records > 0 {
		node.Stats.Rowsize = float64(bytes) / float64(records)
	}
	if node.Stats.Selectivity == 0 && node.Stats.TableCnt > 0 {
		node.Stats.Selectivity = node.Stats.Outcnt / node.Stats.TableCnt
	}
}

func applyIcebergExecutionOptionsToPlan(scanPlan *icebergapi.IcebergScanPlan, req icebergapi.ScanPlanRequest) {
	if scanPlan == nil {
		return
	}
	scanPlan.DeleteMaxMemoryBytes = req.DeleteMaxMemoryBytes
	scanPlan.EnableDeleteSpill = req.EnableDeleteSpill
}

func icebergFilepathHint(scan *plan.IcebergScan) string {
	if scan == nil {
		return ""
	}
	if strings.TrimSpace(scan.Table) != "" {
		return strings.TrimSpace(scan.Table)
	}
	return strings.Join(icebergNamespaceFromPlan(scan.GetNamespace()), "/")
}

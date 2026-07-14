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
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func (c *Compile) constructIcebergInsert(nodes []*plan.Node, node *plan.Node) (vm.Operator, error) {
	op, err := constructIcebergInsert(c.proc, node)
	if err != nil {
		return nil, err
	}
	writer, ok := op.(*icebergwrite.IcebergWrite)
	if !ok {
		return op, nil
	}
	if writer.Request.Operation == icebergwrite.OperationOverwrite {
		planned := writer.Request.DMLScan
		metadata, err := c.icebergOverwriteScanMetadataForInsert(c.icebergSecurityContext(), node)
		if err != nil {
			return nil, err
		}
		metadata.OverwriteScope = planned.OverwriteScope
		metadata.OverwritePartition = planned.OverwritePartition
		writer.Request.DMLScan = metadata
		if err := writer.RetainObjectIORef(c.proc.Ctx); err != nil {
			return nil, err
		}
		if writer.Factory != nil {
			coordinator, err := writer.Factory.NewCoordinator(c.proc.Ctx, writer.Request)
			if err != nil {
				writer.ReleaseObjectIORef()
				return nil, err
			}
			writer.Coordinator = coordinator
		}
		return writer, nil
	}
	if !icebergWriteNeedsDMLScanMetadata(writer.Request.Operation) {
		return op, nil
	}
	metadata, err := c.icebergDMLScanMetadataForInsert(c.proc.Ctx, nodes, node)
	if err != nil {
		return nil, err
	}
	writer.Request.DMLScan = metadata
	alignIcebergDMLWriteRequestToInput(nodes, node, &writer.Request)
	return writer, nil
}

func icebergWriteNeedsDMLScanMetadata(operation string) bool {
	return operation == icebergwrite.OperationDelete ||
		operation == icebergwrite.OperationUpdate ||
		operation == icebergwrite.OperationMerge
}

func alignIcebergDMLWriteRequestToInput(nodes []*plan.Node, node *plan.Node, req *icebergwrite.AppendRequest) {
	if req == nil || node == nil || len(node.Children) != 1 {
		return
	}
	attrs, ok := icebergDMLInputAttrs(nodes, node.Children[0], req.Attrs)
	if !ok {
		return
	}
	dataFilePathColumnIndex, rowOrdinalColumnIndex, mergeActionColumnIndex := icebergDMLMetadataIndexes(attrs)
	if dataFilePathColumnIndex < 0 && rowOrdinalColumnIndex < 0 && mergeActionColumnIndex < 0 {
		return
	}
	req.Attrs = attrs
	if dataFilePathColumnIndex >= 0 {
		req.DataFilePathColumnIndex = dataFilePathColumnIndex
	}
	if rowOrdinalColumnIndex >= 0 {
		req.RowOrdinalColumnIndex = rowOrdinalColumnIndex
	}
	if mergeActionColumnIndex >= 0 {
		req.MergeActionColumnIndex = mergeActionColumnIndex
	}
}

func icebergDMLInputAttrs(nodes []*plan.Node, rootID int32, fallback []string) ([]string, bool) {
	if rootID < 0 || int(rootID) >= len(nodes) {
		return nil, false
	}
	names := icebergPlanNodeOutputNames(nodes[rootID])
	if len(names) == 0 {
		return nil, false
	}
	attrs := make([]string, len(names))
	hasDMLMetadata := false
	for idx, name := range names {
		name = strings.TrimSpace(name)
		if name == "" && idx < len(fallback) {
			name = strings.TrimSpace(fallback[idx])
		}
		attrs[idx] = name
		if isIcebergDMLWriteMetadataName(name) {
			hasDMLMetadata = true
		}
	}
	if !hasDMLMetadata {
		return nil, false
	}
	return attrs, true
}

func icebergPlanNodeOutputNames(node *plan.Node) []string {
	if node == nil {
		return nil
	}
	if len(node.ProjectList) > 0 {
		names := make([]string, len(node.ProjectList))
		for idx, expr := range node.ProjectList {
			names[idx] = icebergExprColumnName(expr)
		}
		return names
	}
	if node.TableDef == nil || len(node.TableDef.Cols) == 0 {
		return nil
	}
	names := make([]string, 0, len(node.TableDef.Cols))
	for _, col := range node.TableDef.Cols {
		if col == nil {
			continue
		}
		names = append(names, col.GetOriginCaseName())
	}
	return names
}

func icebergExprColumnName(expr *plan.Expr) string {
	if expr == nil || expr.GetCol() == nil {
		return ""
	}
	return expr.GetCol().Name
}

func icebergDMLMetadataIndexes(attrs []string) (dataFilePathColumnIndex, rowOrdinalColumnIndex, mergeActionColumnIndex int32) {
	dataFilePathColumnIndex = -1
	rowOrdinalColumnIndex = -1
	mergeActionColumnIndex = -1
	for idx, attr := range attrs {
		name := strings.TrimSpace(attr)
		switch {
		case strings.EqualFold(name, icebergapi.DMLDataFilePathColumnName):
			dataFilePathColumnIndex = int32(idx)
		case strings.EqualFold(name, icebergapi.DMLRowOrdinalColumnName):
			rowOrdinalColumnIndex = int32(idx)
		case strings.EqualFold(name, icebergapi.DMLMergeActionColumnName):
			mergeActionColumnIndex = int32(idx)
		}
	}
	return dataFilePathColumnIndex, rowOrdinalColumnIndex, mergeActionColumnIndex
}

func isIcebergDMLWriteMetadataName(name string) bool {
	name = strings.TrimSpace(name)
	return strings.EqualFold(name, icebergapi.DMLDataFilePathColumnName) ||
		strings.EqualFold(name, icebergapi.DMLRowOrdinalColumnName) ||
		strings.EqualFold(name, icebergapi.DMLMergeActionColumnName)
}

func (c *Compile) recordIcebergScanPlan(planNodeID int32, scanPlan *icebergapi.IcebergScanPlan) {
	if c == nil || planNodeID < 0 || scanPlan == nil {
		return
	}
	if c.icebergScanPlans == nil {
		c.icebergScanPlans = make(map[int32]*icebergapi.IcebergScanPlan)
	}
	c.icebergScanPlans[planNodeID] = scanPlan
}

func (c *Compile) icebergScanPlanForNode(planNodeID int32) *icebergapi.IcebergScanPlan {
	if c == nil || planNodeID < 0 || c.icebergScanPlans == nil {
		return nil
	}
	return c.icebergScanPlans[planNodeID]
}

func (c *Compile) icebergDMLScanMetadataForInsert(ctx context.Context, nodes []*plan.Node, node *plan.Node) (icebergwrite.DMLScanMetadata, error) {
	if node == nil {
		return icebergwrite.DMLScanMetadata{}, moerr.NewInvalidInput(ctx, "Iceberg DML insert requires an insert node")
	}
	if len(node.Children) != 1 {
		return icebergwrite.DMLScanMetadata{}, moerr.NewInvalidInput(ctx, "Iceberg DML insert requires one scan input")
	}
	scanID, found, err := findSingleIcebergDMLScanNode(ctx, nodes, node.Children[0], icebergDMLInsertTargetRef(node))
	if err != nil {
		return icebergwrite.DMLScanMetadata{}, err
	}
	if !found {
		return icebergwrite.DMLScanMetadata{}, moerr.NewInvalidInput(ctx, "Iceberg DML insert requires one Iceberg scan input")
	}
	scanPlan := c.icebergScanPlanForNode(scanID)
	if scanPlan == nil {
		return icebergwrite.DMLScanMetadata{}, moerr.NewInvalidInput(ctx, "Iceberg DML insert is missing compiled scan metadata")
	}
	return icebergDMLScanMetadataFromPlan(scanPlan), nil
}

func (c *Compile) icebergOverwriteScanMetadataForInsert(ctx context.Context, node *plan.Node) (icebergwrite.DMLScanMetadata, error) {
	if node == nil {
		return icebergwrite.DMLScanMetadata{}, moerr.NewInvalidInput(ctx, "Iceberg overwrite insert requires an insert node")
	}
	access, err := c.checkIcebergScanAccess(node)
	if err != nil {
		return icebergwrite.DMLScanMetadata{}, err
	}
	planner, err := c.icebergScanPlannerForCompile(ctx)
	if err != nil {
		return icebergwrite.DMLScanMetadata{}, err
	}
	ref := strings.TrimSpace(access.table.DefaultRef)
	if ref == "" {
		ref = model.DefaultRefMain
	}
	req := icebergapi.ScanPlanRequest{
		CatalogRequest: icebergapi.CatalogRequest{
			Catalog: access.catalog,
		},
		Namespace: icebergNamespaceFromPlan(access.table.Namespace),
		Table:     strings.TrimSpace(access.table.TableName),
		Ref:       ref,
		Snapshot: icebergapi.SnapshotSelector{
			RefName: ref,
		},
	}
	applyIcebergScanAccessContext(&req, access)
	req.EnableRowGroupPlanning = false
	planningTimeout, err := c.icebergPlanningTimeoutHint(ctx)
	if err != nil {
		return icebergwrite.DMLScanMetadata{}, err
	}
	req.PlanningTimeout = planningTimeout
	if cfg, ok, cfgErr := c.icebergConfig(ctx); cfgErr != nil {
		return icebergwrite.DMLScanMetadata{}, cfgErr
	} else if ok {
		req.DeleteMaxMemoryBytes = cfg.Write.DeleteMaxMemory
		req.EnableDeleteSpill = cfg.Write.EnableDeleteSpill
	}
	scanPlan, err := planner.PlanScan(ctx, req)
	if err != nil {
		return icebergwrite.DMLScanMetadata{}, icebergapi.ToMOErr(ctx, err)
	}
	applyIcebergExecutionOptionsToPlan(scanPlan, req)
	return icebergDMLScanMetadataFromPlan(scanPlan), nil
}

func icebergDMLInsertTargetRef(node *plan.Node) *plan.ObjectRef {
	if node == nil {
		return nil
	}
	if node.ObjRef != nil {
		return node.ObjRef
	}
	if node.InsertCtx != nil {
		return node.InsertCtx.Ref
	}
	return nil
}

func findSingleIcebergDMLScanNode(ctx context.Context, nodes []*plan.Node, rootID int32, targetRef *plan.ObjectRef) (int32, bool, error) {
	var found []int32
	var targetMatches []int32
	var walk func(int32) error
	seen := make(map[int32]struct{})
	walk = func(nodeID int32) error {
		if nodeID < 0 || int(nodeID) >= len(nodes) {
			return moerr.NewInvalidInputf(ctx, "Iceberg DML insert references invalid node %d", nodeID)
		}
		if _, ok := seen[nodeID]; ok {
			return nil
		}
		seen[nodeID] = struct{}{}
		node := nodes[nodeID]
		if isIcebergExternScanNode(node) {
			found = append(found, nodeID)
			if icebergDMLObjectRefMatches(node.ObjRef, targetRef) {
				targetMatches = append(targetMatches, nodeID)
			}
		}
		for _, child := range node.Children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(rootID); err != nil {
		return 0, false, err
	}
	if len(found) == 0 {
		return 0, false, nil
	}
	if len(targetMatches) == 1 {
		return targetMatches[0], true, nil
	}
	if len(targetMatches) > 1 {
		return 0, false, moerr.NewInvalidInput(ctx, "Iceberg DML insert requires exactly one target Iceberg scan input")
	}
	if len(found) > 1 {
		return 0, false, moerr.NewInvalidInput(ctx, "Iceberg DML insert requires exactly one Iceberg scan input")
	}
	return found[0], true, nil
}

func icebergDMLObjectRefMatches(scanRef, targetRef *plan.ObjectRef) bool {
	if scanRef == nil || targetRef == nil {
		return false
	}
	if scanRef.Obj != 0 && targetRef.Obj != 0 {
		return scanRef.Obj == targetRef.Obj &&
			(scanRef.Schema == 0 || targetRef.Schema == 0 || scanRef.Schema == targetRef.Schema) &&
			(scanRef.Db == 0 || targetRef.Db == 0 || scanRef.Db == targetRef.Db)
	}
	if !strings.EqualFold(strings.TrimSpace(scanRef.ObjName), strings.TrimSpace(targetRef.ObjName)) {
		return false
	}
	if scanRef.SchemaName != "" && targetRef.SchemaName != "" &&
		!strings.EqualFold(strings.TrimSpace(scanRef.SchemaName), strings.TrimSpace(targetRef.SchemaName)) {
		return false
	}
	if scanRef.DbName != "" && targetRef.DbName != "" &&
		!strings.EqualFold(strings.TrimSpace(scanRef.DbName), strings.TrimSpace(targetRef.DbName)) {
		return false
	}
	return strings.TrimSpace(scanRef.ObjName) != ""
}

func isIcebergExternScanNode(node *plan.Node) bool {
	return node != nil &&
		node.GetExternScan() != nil &&
		node.GetExternScan().GetType() == int32(plan.ExternType_ICEBERG_TB)
}

func icebergDMLScanMetadataFromPlan(scanPlan *icebergapi.IcebergScanPlan) icebergwrite.DMLScanMetadata {
	if scanPlan == nil {
		return icebergwrite.DMLScanMetadata{}
	}
	out := icebergwrite.DMLScanMetadata{
		BaseSnapshotID: scanPlan.Snapshot.SnapshotID,
		BaseSchemaID:   scanPlan.Snapshot.SchemaID,
		Ref:            strings.TrimSpace(scanPlan.Snapshot.RefName),
		ObjectIORef:    strings.TrimSpace(scanPlan.ObjectIORef),
	}
	if len(scanPlan.DataTasks) == 0 {
		return out
	}
	out.DataFiles = make([]icebergapi.DataFile, 0, len(scanPlan.DataTasks))
	seen := make(map[string]struct{}, len(scanPlan.DataTasks))
	for _, task := range scanPlan.DataTasks {
		path := strings.TrimSpace(task.DataFile.FilePath)
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out.DataFiles = append(out.DataFiles, task.DataFile)
	}
	return out
}

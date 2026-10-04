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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
)

type DMLDeleteActionCommitter interface {
	CommitDelete(ctx context.Context, req DMLDeleteActionStreamRequest) (DMLCommitActionStreamResult, error)
}

type DMLDeleteCoordinatorSpec struct {
	Committer DMLDeleteActionCommitter

	Base           dml.CommitBase
	Schema         api.Schema
	DeleteSchemaID int
	ObjectWriter   dml.DeleteObjectWriter

	DataFiles             []api.DataFile
	EqualityFieldIDs      []int
	EqualityColumnIndexes []int32
	PredicateStable       bool
	IncludePositionRows   bool
	MemoryLimitBytes      int64
	InitialMemoryBytes    int64
}

type DMLDeleteCoordinatorFromScanPlanRequest struct {
	Committer DMLDeleteActionCommitter

	Base           dml.CommitBase
	Schema         api.Schema
	DeleteSchemaID int
	ObjectWriter   dml.DeleteObjectWriter
	ScanPlan       *api.IcebergScanPlan

	EqualityFieldIDs      []int
	EqualityColumnIndexes []int32
	PredicateStable       bool
	IncludePositionRows   bool
}

type DMLDeleteCoordinatorFactory struct {
	Spec DMLDeleteCoordinatorSpec
}

func NewDMLDeleteCoordinatorFactoryFromScanPlan(ctx context.Context, req DMLDeleteCoordinatorFromScanPlanRequest) (DMLDeleteCoordinatorFactory, error) {
	spec, err := BuildDMLDeleteCoordinatorSpecFromScanPlan(ctx, req)
	if err != nil {
		return DMLDeleteCoordinatorFactory{}, err
	}
	return DMLDeleteCoordinatorFactory{Spec: spec}, nil
}

func BuildDMLDeleteCoordinatorSpecFromScanPlan(ctx context.Context, req DMLDeleteCoordinatorFromScanPlanRequest) (DMLDeleteCoordinatorSpec, error) {
	if req.ScanPlan == nil {
		return DMLDeleteCoordinatorSpec{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator requires a scan plan", nil))
	}
	if req.Committer == nil {
		return DMLDeleteCoordinatorSpec{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator requires a committer", nil))
	}
	if req.ObjectWriter == nil {
		return DMLDeleteCoordinatorSpec{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator requires an object writer", nil))
	}
	baseSnapshotID := req.ScanPlan.Snapshot.SnapshotID
	if baseSnapshotID <= 0 {
		return DMLDeleteCoordinatorSpec{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML delete scan plan requires a resolved base snapshot", nil))
	}
	schema := cloneDMLDeleteSchema(req.Schema)
	if schema.SchemaID < 0 {
		return DMLDeleteCoordinatorSpec{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML delete coordinator requires a schema id", nil))
	}
	base := req.Base
	if base.BaseSnapshotID == 0 {
		base.BaseSnapshotID = baseSnapshotID
	}
	if base.BaseSchemaID == 0 {
		base.BaseSchemaID = firstPositiveInt(req.ScanPlan.Snapshot.SchemaID, schema.SchemaID)
	}
	if strings.TrimSpace(base.TargetRef) == "" {
		base.TargetRef = strings.TrimSpace(req.ScanPlan.Snapshot.RefName)
	}
	if strings.TrimSpace(base.TargetRef) == "" {
		base.TargetRef = "main"
	}
	deleteSchemaID := req.DeleteSchemaID
	if deleteSchemaID == 0 {
		deleteSchemaID = schema.SchemaID
	}
	return DMLDeleteCoordinatorSpec{
		Committer:             req.Committer,
		Base:                  base,
		Schema:                schema,
		DeleteSchemaID:        deleteSchemaID,
		ObjectWriter:          req.ObjectWriter,
		DataFiles:             dataFilesFromScanPlan(req.ScanPlan),
		EqualityFieldIDs:      append([]int(nil), req.EqualityFieldIDs...),
		EqualityColumnIndexes: append([]int32(nil), req.EqualityColumnIndexes...),
		PredicateStable:       req.PredicateStable,
		IncludePositionRows:   req.IncludePositionRows,
	}, nil
}

func (f DMLDeleteCoordinatorFactory) NewCoordinator(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
	if req.Operation != icebergwrite.OperationDelete {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML delete coordinator only accepts DELETE requests", map[string]string{
			"operation": req.Operation,
		}))
	}
	return NewDMLDeleteCoordinator(dmlDeleteCoordinatorSpecForRequest(f.Spec, req)), nil
}

type DMLDeleteCoordinator struct {
	spec      DMLDeleteCoordinatorSpec
	writeReq  icebergwrite.AppendRequest
	collector *DMLMatchedScanCollector
	budget    *dmlMemoryBudget
}

func NewDMLDeleteCoordinator(spec DMLDeleteCoordinatorSpec) *DMLDeleteCoordinator {
	return &DMLDeleteCoordinator{spec: cloneDMLDeleteCoordinatorSpec(spec)}
}

func (c *DMLDeleteCoordinator) Begin(ctx context.Context, req icebergwrite.AppendRequest) error {
	if c == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator is nil", nil))
	}
	if req.Operation != icebergwrite.OperationDelete {
		return api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML delete coordinator requires DELETE operation", map[string]string{
			"operation": req.Operation,
		}))
	}
	if c.spec.Committer == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator requires a committer", nil))
	}
	if req.DataFilePathColumnIndex < 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator requires data-file path column index", nil))
	}
	includePositionRows := c.spec.IncludePositionRows
	if includePositionRows && req.RowOrdinalColumnIndex < 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator requires row-ordinal column index", nil))
	}
	c.writeReq = req
	c.budget = newDMLMemoryBudget(c.spec.MemoryLimitBytes, c.spec.InitialMemoryBytes)
	c.collector = NewDMLMatchedScanCollector(DMLMatchedScanCollectorSpec{
		DataFiles:               append([]api.DataFile(nil), c.spec.DataFiles...),
		DataFilePathColumnIndex: req.DataFilePathColumnIndex,
		RowOrdinalColumnIndex:   req.RowOrdinalColumnIndex,
		EqualityFieldIDs:        append([]int(nil), c.spec.EqualityFieldIDs...),
		EqualityColumnIndexes:   append([]int32(nil), c.spec.EqualityColumnIndexes...),
		PredicateStable:         c.spec.PredicateStable,
		IncludePositionRows:     includePositionRows,
		MemoryBudget:            c.budget,
	})
	return nil
}

func (c *DMLDeleteCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	if c == nil || c.collector == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator was not opened", nil))
	}
	return c.collector.AddBatch(ctx, bat)
}

func (c *DMLDeleteCoordinator) Commit(ctx context.Context) error {
	if c == nil || c.collector == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete coordinator was not opened", nil))
	}
	defer c.cleanRuntimeState()
	requestBytes := c.collector.RetainedBytes()
	if err := c.budget.reserve(ctx, requestBytes); err != nil {
		return err
	}
	defer c.budget.release(requestBytes)
	targets := c.collector.Targets()
	if len(targets) == 0 {
		return nil
	}
	req := DMLDeleteActionStreamRequest{
		Schema:             c.spec.Schema,
		Base:               c.commitBase(),
		DeleteSchemaID:     c.spec.DeleteSchemaID,
		ObjectWriter:       c.spec.ObjectWriter,
		Targets:            targets,
		MemoryLimitBytes:   c.spec.MemoryLimitBytes,
		InitialMemoryBytes: c.budget.usedBytes(),
		MemoryBudget:       c.budget,
	}
	_, err := c.spec.Committer.CommitDelete(ctx, req)
	return err
}

func (c *DMLDeleteCoordinator) Abort(ctx context.Context, cause error) error {
	c.cleanRuntimeState()
	return nil
}

func (c *DMLDeleteCoordinator) cleanRuntimeState() {
	if c == nil {
		return
	}
	if c.collector != nil {
		c.collector.Reset()
	}
	c.collector = nil
	c.budget = nil
}

func (c *DMLDeleteCoordinator) commitBase() dml.CommitBase {
	base := c.spec.Base
	if len(base.Namespace) == 0 && strings.TrimSpace(c.writeReq.Namespace) != "" {
		base.Namespace = dottedNamespace(c.writeReq.Namespace)
	}
	if strings.TrimSpace(base.Table) == "" {
		base.Table = strings.TrimSpace(c.writeReq.Table)
	}
	if strings.TrimSpace(base.TargetRef) == "" {
		base.TargetRef = strings.TrimSpace(c.writeReq.DefaultRef)
	}
	return base
}

func cloneDMLDeleteCoordinatorSpec(spec DMLDeleteCoordinatorSpec) DMLDeleteCoordinatorSpec {
	spec.Schema = cloneDMLDeleteSchema(spec.Schema)
	spec.DataFiles = append([]api.DataFile(nil), spec.DataFiles...)
	spec.EqualityFieldIDs = append([]int(nil), spec.EqualityFieldIDs...)
	spec.EqualityColumnIndexes = append([]int32(nil), spec.EqualityColumnIndexes...)
	return spec
}

func dmlDeleteCoordinatorSpecForRequest(spec DMLDeleteCoordinatorSpec, req icebergwrite.AppendRequest) DMLDeleteCoordinatorSpec {
	spec = cloneDMLDeleteCoordinatorSpec(spec)
	if len(spec.DataFiles) == 0 && len(req.DMLScan.DataFiles) > 0 {
		spec.DataFiles = append([]api.DataFile(nil), req.DMLScan.DataFiles...)
	}
	if spec.Base.BaseSnapshotID == 0 {
		spec.Base.BaseSnapshotID = req.DMLScan.BaseSnapshotID
	}
	if spec.Base.BaseSchemaID == 0 {
		spec.Base.BaseSchemaID = req.DMLScan.BaseSchemaID
	}
	if strings.TrimSpace(spec.Base.TargetRef) == "" {
		spec.Base.TargetRef = firstNonEmpty(strings.TrimSpace(req.DMLScan.Ref), strings.TrimSpace(req.DefaultRef), "main")
	}
	if spec.Schema.SchemaID == 0 {
		spec.Schema.SchemaID = req.DMLScan.BaseSchemaID
	}
	if spec.DeleteSchemaID == 0 {
		spec.DeleteSchemaID = spec.Schema.SchemaID
	}
	return spec
}

func cloneDMLDeleteSchema(schema api.Schema) api.Schema {
	schema.Fields = append([]api.SchemaField(nil), schema.Fields...)
	schema.IdentifierFieldIDs = append([]int(nil), schema.IdentifierFieldIDs...)
	return schema
}

func firstPositiveInt(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

var _ icebergwrite.Coordinator = (*DMLDeleteCoordinator)(nil)
var _ icebergwrite.CoordinatorFactory = DMLDeleteCoordinatorFactory{}
var _ DMLDeleteActionCommitter = DMLActionExecutor{}

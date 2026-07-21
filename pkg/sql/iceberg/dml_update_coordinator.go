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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type DMLUpdateActionCommitter interface {
	CommitUpdate(ctx context.Context, req DMLUpdateActionStreamRequest) (DMLCommitActionStreamResult, error)
}

type DMLUpdateCoordinatorSpec struct {
	Committer DMLUpdateActionCommitter

	Base           dml.CommitBase
	Schema         api.Schema
	DeleteSchemaID int
	ObjectWriter   dml.DeleteObjectWriter
	DataFiles      []api.DataFile
	PartitionSpec  api.PartitionSpec

	TargetFileSizeBytes int64
	TimeZone            *time.Location
	MemoryLimitBytes    int64
	InitialMemoryBytes  int64
}

type DMLUpdateCoordinator struct {
	spec             DMLUpdateCoordinatorSpec
	writeReq         icebergwrite.AppendRequest
	collector        *DMLMatchedScanCollector
	replacementCols  []int
	replacementAttrs []string
	replacementBats  []*batch.Batch
	mp               *mpool.MPool
	budget           *dmlMemoryBudget
	replacementBytes int64
}

func NewDMLUpdateCoordinator(spec DMLUpdateCoordinatorSpec) *DMLUpdateCoordinator {
	spec.DataFiles = append([]api.DataFile(nil), spec.DataFiles...)
	spec.Schema = cloneDMLDeleteSchema(spec.Schema)
	return &DMLUpdateCoordinator{spec: spec}
}

func (c *DMLUpdateCoordinator) Begin(ctx context.Context, req icebergwrite.AppendRequest) error {
	if c == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator is nil", nil))
	}
	if req.Operation != icebergwrite.OperationUpdate {
		return api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML update coordinator requires UPDATE operation", map[string]string{
			"operation": req.Operation,
		}))
	}
	if c.spec.Committer == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator requires a committer", nil))
	}
	if c.spec.ObjectWriter == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator requires an object writer", nil))
	}
	if req.DataFilePathColumnIndex <= 0 || int(req.DataFilePathColumnIndex) > len(req.Attrs) {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator requires metadata columns after replacement columns", nil))
	}
	if req.RowOrdinalColumnIndex < 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator requires row-ordinal column index", nil))
	}
	c.writeReq = req
	c.budget = newDMLMemoryBudget(c.spec.MemoryLimitBytes, c.spec.InitialMemoryBytes)
	c.replacementAttrs = append([]string(nil), req.Attrs[:req.DataFilePathColumnIndex]...)
	c.replacementCols = make([]int, len(c.replacementAttrs))
	for idx := range c.replacementCols {
		c.replacementCols[idx] = idx
	}
	c.collector = NewDMLMatchedScanCollector(DMLMatchedScanCollectorSpec{
		DataFiles:               append([]api.DataFile(nil), c.spec.DataFiles...),
		DataFilePathColumnIndex: req.DataFilePathColumnIndex,
		RowOrdinalColumnIndex:   req.RowOrdinalColumnIndex,
		IncludePositionRows:     true,
		MemoryBudget:            c.budget,
	})
	return nil
}

func (c *DMLUpdateCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator requires process-aware append", nil))
}

func (c *DMLUpdateCoordinator) AppendWithProcess(proc *process.Process, bat *batch.Batch) error {
	if proc == nil {
		return api.ToMOErr(context.Background(), api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator requires process", nil))
	}
	if c == nil || c.collector == nil {
		return api.ToMOErr(proc.Ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator was not opened", nil))
	}
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	ctx := proc.Ctx
	if err := c.collector.AddBatch(ctx, bat); err != nil {
		return err
	}
	replacementCols, err := dmlReplacementColumnIndexes(ctx, bat, c.replacementAttrs, c.replacementCols)
	if err != nil {
		return err
	}
	retainedBytes := retainedDMLBatchBytes(bat)
	if err := c.budget.reserve(ctx, retainedBytes); err != nil {
		return err
	}
	cloned, err := bat.CloneSelectedColumns(replacementCols, append([]string(nil), c.replacementAttrs...), proc.Mp())
	if err != nil {
		c.budget.release(retainedBytes)
		return api.ToMOErr(ctx, api.WrapError(api.ErrInternal, "Iceberg DML update coordinator failed to clone replacement rows", nil, err))
	}
	c.mp = proc.Mp()
	c.replacementBats = append(c.replacementBats, cloned)
	c.replacementBytes = saturatingDMLAdd(c.replacementBytes, retainedBytes)
	return nil
}

func (c *DMLUpdateCoordinator) Commit(ctx context.Context) error {
	if c == nil || c.collector == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML update coordinator was not opened", nil))
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
	replacements := make([]DMLReplacementDataBatch, 0, len(c.replacementBats))
	for _, bat := range c.replacementBats {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		replacements = append(replacements, DMLReplacementDataBatch{
			Attrs:               append([]string(nil), c.replacementAttrs...),
			Batch:               bat,
			PartitionSpec:       c.spec.PartitionSpec,
			TargetFileSizeBytes: c.spec.TargetFileSizeBytes,
			TimeZone:            c.spec.TimeZone,
			ObjectWriter:        c.spec.ObjectWriter,
			MemoryBudget:        c.budget,
		})
	}
	if len(replacements) == 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML update coordinator matched rows but has no replacement rows", map[string]string{
			"table": c.writeReq.Table,
		}))
	}
	req := DMLUpdateActionStreamRequest{
		DMLDeleteActionStreamRequest: DMLDeleteActionStreamRequest{
			Schema:             c.spec.Schema,
			Base:               c.commitBase(),
			DeleteSchemaID:     c.spec.DeleteSchemaID,
			ObjectWriter:       c.spec.ObjectWriter,
			Targets:            targets,
			MemoryLimitBytes:   c.spec.MemoryLimitBytes,
			InitialMemoryBytes: c.budget.usedBytes(),
			MemoryBudget:       c.budget,
		},
		ReplacementBatches: replacements,
	}
	_, err := c.spec.Committer.CommitUpdate(ctx, req)
	return err
}

func (c *DMLUpdateCoordinator) Abort(ctx context.Context, cause error) error {
	c.cleanRuntimeState()
	return nil
}

func (c *DMLUpdateCoordinator) commitBase() dml.CommitBase {
	base := c.spec.Base
	if len(base.Namespace) == 0 && c.writeReq.Namespace != "" {
		base.Namespace = dottedNamespace(c.writeReq.Namespace)
	}
	if base.Table == "" {
		base.Table = c.writeReq.Table
	}
	if base.TargetRef == "" {
		base.TargetRef = firstNonEmpty(c.writeReq.DMLScan.Ref, c.writeReq.DefaultRef, "main")
	}
	if base.IdempotencyKey == "" {
		base.IdempotencyKey = firstNonEmpty(c.writeReq.IdempotencyKey, c.writeReq.StatementID)
	}
	if base.StatementID == "" {
		base.StatementID = firstNonEmpty(c.writeReq.StatementID, c.writeReq.IdempotencyKey)
	}
	if base.BaseSnapshotID == 0 {
		base.BaseSnapshotID = c.writeReq.DMLScan.BaseSnapshotID
	}
	if base.BaseSchemaID == 0 {
		base.BaseSchemaID = c.writeReq.DMLScan.BaseSchemaID
	}
	return base
}

func (c *DMLUpdateCoordinator) cleanReplacementBatches() {
	if c == nil {
		return
	}
	for _, bat := range c.replacementBats {
		if bat != nil {
			bat.Clean(c.mp)
		}
	}
	c.replacementBats = nil
	c.budget.release(c.replacementBytes)
	c.replacementBytes = 0
}

func (c *DMLUpdateCoordinator) cleanRuntimeState() {
	if c == nil {
		return
	}
	c.cleanReplacementBatches()
	if c.collector != nil {
		c.collector.Reset()
	}
	c.collector = nil
	c.budget = nil
}

var _ icebergwrite.Coordinator = (*DMLUpdateCoordinator)(nil)
var _ icebergwrite.ProcessAwareCoordinator = (*DMLUpdateCoordinator)(nil)
var _ DMLUpdateActionCommitter = DMLActionExecutor{}

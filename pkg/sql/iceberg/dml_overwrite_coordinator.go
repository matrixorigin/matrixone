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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type DMLOverwriteActionCommitter interface {
	CommitOverwrite(ctx context.Context, req DMLOverwriteActionStreamRequest) (DMLCommitActionStreamResult, error)
}

type DMLOverwriteCoordinatorSpec struct {
	Committer DMLOverwriteActionCommitter

	Base              dml.CommitBase
	Schema            api.Schema
	ObjectWriter      dml.DeleteObjectWriter
	AffectedDataFiles []api.DataFile
	PartitionSpec     api.PartitionSpec
	Scope             dml.OverwriteScope
	Partition         map[string]any

	TargetFileSizeBytes int64
	TimeZone            *time.Location
}

type DMLOverwriteCoordinator struct {
	mu              sync.Mutex
	spec            DMLOverwriteCoordinatorSpec
	writeReq        icebergwrite.AppendRequest
	replacementCols []int
	replacementBats []*batch.Batch
	mp              *mpool.MPool
	opened          bool
	activeScopes    int
	commitAttempted bool
	committed       bool
	aborted         bool
	commitErr       error
}

func NewDMLOverwriteCoordinator(spec DMLOverwriteCoordinatorSpec) *DMLOverwriteCoordinator {
	spec.Schema = cloneDMLDeleteSchema(spec.Schema)
	spec.AffectedDataFiles = append([]api.DataFile(nil), spec.AffectedDataFiles...)
	spec.Partition = cloneDMLAnyMap(spec.Partition)
	return &DMLOverwriteCoordinator{spec: spec}
}

func (c *DMLOverwriteCoordinator) Begin(ctx context.Context, req icebergwrite.AppendRequest) error {
	if c == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator is nil", nil))
	}
	if req.Operation != icebergwrite.OperationOverwrite {
		return api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML overwrite coordinator requires OVERWRITE operation", map[string]string{
			"operation": req.Operation,
		}))
	}
	if c.spec.Committer == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator requires a committer", nil))
	}
	if c.spec.ObjectWriter == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator requires an object writer", nil))
	}
	if c.spec.Scope == dml.OverwritePartition {
		if err := validateOverwritePartitionKeys(ctx, c.spec.Partition, c.spec.PartitionSpec, req.Table); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return api.ToMOErr(ctx, api.NewError(api.ErrInternal, "Iceberg DML overwrite coordinator was aborted", nil))
	}
	if c.commitAttempted {
		return c.commitErr
	}
	if !c.opened {
		c.writeReq = req
		c.replacementCols = make([]int, len(req.Attrs))
		for idx := range c.replacementCols {
			c.replacementCols[idx] = idx
		}
		c.opened = true
	}
	c.activeScopes++
	return nil
}

func (c *DMLOverwriteCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator requires process-aware append", nil))
}

func (c *DMLOverwriteCoordinator) AppendWithProcess(proc *process.Process, bat *batch.Batch) error {
	if proc == nil {
		return api.ToMOErr(context.Background(), api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator requires process", nil))
	}
	if c == nil {
		return api.ToMOErr(proc.Ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator was not opened", nil))
	}
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	c.mu.Lock()
	if !c.opened {
		c.mu.Unlock()
		return api.ToMOErr(proc.Ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator was not opened", nil))
	}
	if c.aborted {
		c.mu.Unlock()
		return api.ToMOErr(proc.Ctx, api.NewError(api.ErrInternal, "Iceberg DML overwrite coordinator was aborted", nil))
	}
	if c.commitAttempted {
		c.mu.Unlock()
		return api.ToMOErr(proc.Ctx, api.NewError(api.ErrCommitUnknown, "Iceberg DML overwrite coordinator already committed before all rows were appended", nil))
	}
	replacementCols := append([]int(nil), c.replacementCols...)
	attrs := append([]string(nil), c.writeReq.Attrs...)
	c.mu.Unlock()

	cloned, err := bat.CloneSelectedColumns(replacementCols, attrs, proc.Mp())
	if err != nil {
		return api.ToMOErr(proc.Ctx, api.WrapError(api.ErrInternal, "Iceberg DML overwrite coordinator failed to clone replacement rows", nil, err))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		cloned.Clean(proc.Mp())
		return api.ToMOErr(proc.Ctx, api.NewError(api.ErrInternal, "Iceberg DML overwrite coordinator was aborted", nil))
	}
	if c.commitAttempted {
		cloned.Clean(proc.Mp())
		return api.ToMOErr(proc.Ctx, api.NewError(api.ErrCommitUnknown, "Iceberg DML overwrite coordinator already committed before all rows were appended", nil))
	}
	c.mp = proc.Mp()
	c.replacementBats = append(c.replacementBats, cloned)
	return nil
}

func (c *DMLOverwriteCoordinator) Commit(ctx context.Context) error {
	if c == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator was not opened", nil))
	}
	c.mu.Lock()
	if !c.opened {
		c.mu.Unlock()
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML overwrite coordinator was not opened", nil))
	}
	if c.aborted {
		c.mu.Unlock()
		return api.ToMOErr(ctx, api.NewError(api.ErrInternal, "Iceberg DML overwrite coordinator was aborted", nil))
	}
	if c.commitAttempted {
		err := c.commitErr
		c.mu.Unlock()
		return err
	}
	if c.activeScopes > 1 {
		c.activeScopes--
		c.mu.Unlock()
		return nil
	}
	if c.activeScopes == 1 {
		c.activeScopes--
	}
	req := c.commitRequestLocked()
	if len(req.ReplacementBatches) == 0 && len(req.AffectedDataFiles) == 0 {
		c.commitAttempted = true
		c.committed = true
		c.cleanReplacementBatchesLocked()
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	_, err := c.spec.Committer.CommitOverwrite(ctx, req)

	c.mu.Lock()
	c.commitAttempted = true
	c.commitErr = err
	c.committed = err == nil
	c.cleanReplacementBatchesLocked()
	c.mu.Unlock()
	return err
}

func (c *DMLOverwriteCoordinator) CommitAttempted() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commitAttempted
}

func (c *DMLOverwriteCoordinator) commitRequestLocked() DMLOverwriteActionStreamRequest {
	replacements := make([]DMLReplacementDataBatch, 0, len(c.replacementBats))
	for _, bat := range c.replacementBats {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		replacements = append(replacements, DMLReplacementDataBatch{
			Attrs:               append([]string(nil), c.writeReq.Attrs...),
			Batch:               bat,
			PartitionSpec:       c.spec.PartitionSpec,
			TargetFileSizeBytes: c.spec.TargetFileSizeBytes,
			TimeZone:            c.spec.TimeZone,
			ObjectWriter:        c.spec.ObjectWriter,
		})
	}
	return DMLOverwriteActionStreamRequest{
		Schema:             c.spec.Schema,
		Base:               c.commitBase(),
		Scope:              overwriteScopeOrDefault(c.spec.Scope),
		Partition:          cloneDMLAnyMap(c.spec.Partition),
		PartitionSpec:      c.spec.PartitionSpec,
		ObjectWriter:       c.spec.ObjectWriter,
		AffectedDataFiles:  append([]api.DataFile(nil), c.spec.AffectedDataFiles...),
		ReplacementBatches: replacements,
		ReplacementBatch: DMLReplacementDataBatch{
			ObjectWriter: c.spec.ObjectWriter,
		},
	}
}

func (c *DMLOverwriteCoordinator) Abort(ctx context.Context, cause error) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.commitAttempted || c.aborted {
		return nil
	}
	c.aborted = true
	if c.activeScopes > 0 {
		c.activeScopes--
	}
	c.cleanReplacementBatchesLocked()
	return nil
}

func (c *DMLOverwriteCoordinator) commitBase() dml.CommitBase {
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

func (c *DMLOverwriteCoordinator) cleanReplacementBatchesLocked() {
	for _, bat := range c.replacementBats {
		if bat != nil {
			bat.Clean(c.mp)
		}
	}
	c.replacementBats = nil
}

var _ icebergwrite.Coordinator = (*DMLOverwriteCoordinator)(nil)
var _ icebergwrite.ProcessAwareCoordinator = (*DMLOverwriteCoordinator)(nil)
var _ DMLOverwriteActionCommitter = DMLActionExecutor{}

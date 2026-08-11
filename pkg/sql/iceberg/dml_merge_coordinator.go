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
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type DMLMergeActionCommitter interface {
	CommitMerge(ctx context.Context, req DMLMergeActionStreamRequest) (DMLCommitActionStreamResult, error)
}

type DMLMergeCoordinatorSpec struct {
	Committer DMLMergeActionCommitter

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

type DMLMergeCoordinator struct {
	spec             DMLMergeCoordinatorSpec
	writeReq         icebergwrite.AppendRequest
	deleteCollector  *DMLMatchedScanCollector
	updateCollector  *DMLMatchedScanCollector
	matchedRows      map[string]struct{}
	replacementCols  []int
	replacementAttrs []string
	updateBats       []*batch.Batch
	insertBats       []*batch.Batch
	mp               *mpool.MPool
	budget           *dmlMemoryBudget
	replacementBytes int64
	matchedKeyBytes  int64
}

func NewDMLMergeCoordinator(spec DMLMergeCoordinatorSpec) *DMLMergeCoordinator {
	spec.Schema = cloneDMLDeleteSchema(spec.Schema)
	spec.DataFiles = append([]api.DataFile(nil), spec.DataFiles...)
	return &DMLMergeCoordinator{spec: spec}
}

func (c *DMLMergeCoordinator) Begin(ctx context.Context, req icebergwrite.AppendRequest) error {
	if c == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator is nil", nil))
	}
	if req.Operation != icebergwrite.OperationMerge {
		return api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML merge coordinator requires MERGE operation", map[string]string{
			"operation": req.Operation,
		}))
	}
	if c.spec.Committer == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator requires a committer", nil))
	}
	if c.spec.ObjectWriter == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator requires an object writer", nil))
	}
	if req.DataFilePathColumnIndex <= 0 || int(req.DataFilePathColumnIndex) > len(req.Attrs) {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator requires metadata columns after replacement columns", nil))
	}
	if req.RowOrdinalColumnIndex < 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator requires row-ordinal column index", nil))
	}
	if req.MergeActionColumnIndex < 0 || int(req.MergeActionColumnIndex) >= len(req.Attrs) {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator requires merge action column index", nil))
	}
	c.writeReq = req
	c.budget = newDMLMemoryBudget(c.spec.MemoryLimitBytes, c.spec.InitialMemoryBytes)
	c.replacementAttrs = append([]string(nil), req.Attrs[:req.DataFilePathColumnIndex]...)
	c.replacementCols = make([]int, len(c.replacementAttrs))
	for idx := range c.replacementCols {
		c.replacementCols[idx] = idx
	}
	collectorSpec := DMLMatchedScanCollectorSpec{
		DataFiles:               append([]api.DataFile(nil), c.spec.DataFiles...),
		DataFilePathColumnIndex: req.DataFilePathColumnIndex,
		RowOrdinalColumnIndex:   req.RowOrdinalColumnIndex,
		IncludePositionRows:     true,
		MemoryBudget:            c.budget,
	}
	c.deleteCollector = NewDMLMatchedScanCollector(collectorSpec)
	c.updateCollector = NewDMLMatchedScanCollector(collectorSpec)
	c.matchedRows = make(map[string]struct{})
	return nil
}

func (c *DMLMergeCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator requires process-aware append", nil))
}

func (c *DMLMergeCoordinator) AppendWithProcess(proc *process.Process, bat *batch.Batch) error {
	if proc == nil {
		return api.ToMOErr(context.Background(), api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator requires process", nil))
	}
	if c == nil || c.deleteCollector == nil || c.updateCollector == nil {
		return api.ToMOErr(proc.Ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator was not opened", nil))
	}
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	ctx := proc.Ctx
	// splitMergeActionBatch duplicates the input before the retained subsets are
	// cloned. Account for that transient copy up front and release it when this
	// call returns; retained collectors/batches take their own reservations.
	transientBytes := retainedDMLBatchBytes(bat)
	if err := c.budget.reserve(ctx, transientBytes); err != nil {
		return err
	}
	defer c.budget.release(transientBytes)
	deleteBatch, updateBatch, insertBatch, err := c.splitMergeActionBatch(ctx, bat, proc.Mp())
	if err != nil {
		return err
	}
	defer cleanMergeSplitBatch(deleteBatch, proc.Mp())
	defer cleanMergeSplitBatch(updateBatch, proc.Mp())
	defer cleanMergeSplitBatch(insertBatch, proc.Mp())

	if deleteBatch != nil && deleteBatch.RowCount() > 0 {
		if err := c.rejectDuplicateMatchedTargets(ctx, deleteBatch); err != nil {
			return err
		}
		if err := c.deleteCollector.AddBatch(ctx, deleteBatch); err != nil {
			return err
		}
	}
	if updateBatch != nil && updateBatch.RowCount() > 0 {
		if err := c.rejectDuplicateMatchedTargets(ctx, updateBatch); err != nil {
			return err
		}
		if err := c.updateCollector.AddBatch(ctx, updateBatch); err != nil {
			return err
		}
		replacementCols, err := dmlReplacementColumnIndexes(ctx, updateBatch, c.replacementAttrs, c.replacementCols)
		if err != nil {
			return err
		}
		retainedBytes := retainedDMLBatchBytes(updateBatch)
		if err := c.budget.reserve(ctx, retainedBytes); err != nil {
			return err
		}
		cloned, err := updateBatch.CloneSelectedColumns(replacementCols, append([]string(nil), c.replacementAttrs...), proc.Mp())
		if err != nil {
			c.budget.release(retainedBytes)
			return api.ToMOErr(ctx, api.WrapError(api.ErrInternal, "Iceberg DML merge coordinator failed to clone matched update rows", nil, err))
		}
		c.mp = proc.Mp()
		c.updateBats = append(c.updateBats, cloned)
		c.replacementBytes = saturatingDMLAdd(c.replacementBytes, retainedBytes)
	}
	if insertBatch != nil && insertBatch.RowCount() > 0 {
		replacementCols, err := dmlReplacementColumnIndexes(ctx, insertBatch, c.replacementAttrs, c.replacementCols)
		if err != nil {
			return err
		}
		retainedBytes := retainedDMLBatchBytes(insertBatch)
		if err := c.budget.reserve(ctx, retainedBytes); err != nil {
			return err
		}
		cloned, err := insertBatch.CloneSelectedColumns(replacementCols, append([]string(nil), c.replacementAttrs...), proc.Mp())
		if err != nil {
			c.budget.release(retainedBytes)
			return api.ToMOErr(ctx, api.WrapError(api.ErrInternal, "Iceberg DML merge coordinator failed to clone unmatched insert rows", nil, err))
		}
		c.mp = proc.Mp()
		c.insertBats = append(c.insertBats, cloned)
		c.replacementBytes = saturatingDMLAdd(c.replacementBytes, retainedBytes)
	}
	return nil
}

func (c *DMLMergeCoordinator) rejectDuplicateMatchedTargets(ctx context.Context, bat *batch.Batch) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	pathIdx, err := dmlBatchColumnIndexByNameOrError(
		ctx,
		bat,
		api.DMLDataFilePathColumnName,
		c.writeReq.DataFilePathColumnIndex,
		"data file path",
	)
	if err != nil {
		return err
	}
	ordinalIdx, err := dmlBatchColumnIndexByNameOrError(
		ctx,
		bat,
		api.DMLRowOrdinalColumnName,
		c.writeReq.RowOrdinalColumnIndex,
		"row ordinal",
	)
	if err != nil {
		return err
	}
	if c.matchedRows == nil {
		c.matchedRows = make(map[string]struct{})
	}
	for row := 0; row < bat.RowCount(); row++ {
		path, err := dmlStringValue(ctx, bat.Vecs[pathIdx], row)
		if err != nil {
			return err
		}
		path = strings.TrimSpace(path)
		ordinal, err := dmlInt64Value(ctx, bat.Vecs[ordinalIdx], row)
		if err != nil {
			return err
		}
		key := lengthPrefixedKey(path, strconv.FormatInt(ordinal, 10))
		if _, exists := c.matchedRows[key]; exists {
			return api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg MERGE matched multiple source rows for the same target row", map[string]string{
				"path":        api.RedactPath(path),
				"row_ordinal": strconv.FormatInt(ordinal, 10),
			}))
		}
		keyBytes := int64(len(key) + 32)
		if err := c.budget.reserve(ctx, keyBytes); err != nil {
			return err
		}
		c.matchedRows[key] = struct{}{}
		c.matchedKeyBytes = saturatingDMLAdd(c.matchedKeyBytes, keyBytes)
	}
	return nil
}

func (c *DMLMergeCoordinator) Commit(ctx context.Context) error {
	if c == nil || c.deleteCollector == nil || c.updateCollector == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML merge coordinator was not opened", nil))
	}
	defer c.cleanRuntimeState()
	requestBytes := saturatingDMLAdd(c.deleteCollector.RetainedBytes(), c.updateCollector.RetainedBytes())
	if err := c.budget.reserve(ctx, requestBytes); err != nil {
		return err
	}
	defer c.budget.release(requestBytes)
	matchedDeletes := c.deleteCollector.Targets()
	updateTargets := c.updateCollector.Targets()
	if len(matchedDeletes) == 0 && len(updateTargets) == 0 && len(c.insertBats) == 0 {
		return nil
	}
	matchedUpdates := make([]DMLMatchedUpdateTarget, 0, len(updateTargets))
	for _, target := range updateTargets {
		matchedUpdates = append(matchedUpdates, DMLMatchedUpdateTarget{DeleteTarget: target})
	}
	updateReplacements := c.replacementBatches(c.updateBats)
	insertReplacements := c.replacementBatches(c.insertBats)
	req := DMLMergeActionStreamRequest{
		TableLocation:                   "",
		Schema:                          c.spec.Schema,
		Base:                            c.commitBase(),
		DeleteSchemaID:                  c.spec.DeleteSchemaID,
		ObjectWriter:                    c.spec.ObjectWriter,
		MatchedDeletes:                  matchedDeletes,
		MatchedUpdates:                  matchedUpdates,
		MatchedUpdateReplacementBatches: updateReplacements,
		UnmatchedAppendBatches:          insertReplacements,
		MemoryLimitBytes:                c.spec.MemoryLimitBytes,
		InitialMemoryBytes:              c.budget.usedBytes(),
		MemoryBudget:                    c.budget,
	}
	_, err := c.spec.Committer.CommitMerge(ctx, req)
	return err
}

func (c *DMLMergeCoordinator) Abort(ctx context.Context, cause error) error {
	c.cleanRuntimeState()
	return nil
}

func (c *DMLMergeCoordinator) splitMergeActionBatch(ctx context.Context, bat *batch.Batch, mp *mpool.MPool) (deleteBatch, updateBatch, insertBatch *batch.Batch, err error) {
	defer func() {
		if err != nil {
			cleanMergeSplitBatch(deleteBatch, mp)
			cleanMergeSplitBatch(updateBatch, mp)
			cleanMergeSplitBatch(insertBatch, mp)
			deleteBatch, updateBatch, insertBatch = nil, nil, nil
		}
	}()
	actionIdx, idxErr := dmlBatchColumnIndexByNameOrError(
		ctx,
		bat,
		api.DMLMergeActionColumnName,
		c.writeReq.MergeActionColumnIndex,
		"merge action",
	)
	if idxErr != nil {
		return nil, nil, nil, idxErr
	}
	for row := 0; row < bat.RowCount(); row++ {
		action, err := dmlStringValue(ctx, bat.Vecs[actionIdx], row)
		if err != nil {
			return nil, nil, nil, err
		}
		switch normalizeMergeAction(action) {
		case api.DMLMergeActionDelete:
			if deleteBatch == nil {
				deleteBatch = emptyBatchLike(bat)
			}
			if err := deleteBatch.UnionOne(bat, int64(row), mp); err != nil {
				return nil, nil, nil, api.ToMOErr(ctx, api.WrapError(api.ErrInternal, "Iceberg DML merge coordinator failed to copy delete row", nil, err))
			}
		case api.DMLMergeActionUpdate:
			if updateBatch == nil {
				updateBatch = emptyBatchLike(bat)
			}
			if err := updateBatch.UnionOne(bat, int64(row), mp); err != nil {
				return nil, nil, nil, api.ToMOErr(ctx, api.WrapError(api.ErrInternal, "Iceberg DML merge coordinator failed to copy update row", nil, err))
			}
		case api.DMLMergeActionInsert:
			if insertBatch == nil {
				insertBatch = emptyBatchLike(bat)
			}
			if err := insertBatch.UnionOne(bat, int64(row), mp); err != nil {
				return nil, nil, nil, api.ToMOErr(ctx, api.WrapError(api.ErrInternal, "Iceberg DML merge coordinator failed to copy insert row", nil, err))
			}
		case api.DMLMergeActionNoop:
			continue
		default:
			return nil, nil, nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML merge action is unsupported", map[string]string{
				"action": strings.TrimSpace(action),
			}))
		}
	}
	return deleteBatch, updateBatch, insertBatch, nil
}

func normalizeMergeAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case api.DMLMergeActionDelete, api.DMLMergeActionMatchedDelete:
		return api.DMLMergeActionDelete
	case api.DMLMergeActionUpdate, api.DMLMergeActionMatchedUpdate:
		return api.DMLMergeActionUpdate
	case api.DMLMergeActionInsert, api.DMLMergeActionNotMatched:
		return api.DMLMergeActionInsert
	case api.DMLMergeActionNoop:
		return api.DMLMergeActionNoop
	default:
		return ""
	}
}

func emptyBatchLike(src *batch.Batch) *batch.Batch {
	if src == nil {
		return nil
	}
	out := batch.NewWithSize(len(src.Vecs))
	out.Attrs = append([]string(nil), src.Attrs...)
	for idx, vec := range src.Vecs {
		if vec == nil {
			continue
		}
		out.Vecs[idx] = vector.NewVec(*vec.GetType())
	}
	return out
}

func cleanMergeSplitBatch(bat *batch.Batch, mp *mpool.MPool) {
	if bat != nil {
		bat.Clean(mp)
	}
}

func (c *DMLMergeCoordinator) replacementBatches(bats []*batch.Batch) []DMLReplacementDataBatch {
	out := make([]DMLReplacementDataBatch, 0, len(bats))
	for _, bat := range bats {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		out = append(out, DMLReplacementDataBatch{
			Attrs:               append([]string(nil), c.replacementAttrs...),
			Batch:               bat,
			PartitionSpec:       c.spec.PartitionSpec,
			TargetFileSizeBytes: c.spec.TargetFileSizeBytes,
			TimeZone:            c.spec.TimeZone,
			ObjectWriter:        c.spec.ObjectWriter,
			MemoryBudget:        c.budget,
		})
	}
	return out
}

func (c *DMLMergeCoordinator) commitBase() dml.CommitBase {
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

func (c *DMLMergeCoordinator) cleanReplacementBatches() {
	if c == nil {
		return
	}
	for _, bat := range c.updateBats {
		if bat != nil {
			bat.Clean(c.mp)
		}
	}
	for _, bat := range c.insertBats {
		if bat != nil {
			bat.Clean(c.mp)
		}
	}
	c.updateBats = nil
	c.insertBats = nil
	c.budget.release(c.replacementBytes)
	c.replacementBytes = 0
}

func (c *DMLMergeCoordinator) cleanRuntimeState() {
	if c == nil {
		return
	}
	c.cleanReplacementBatches()
	if c.deleteCollector != nil {
		c.deleteCollector.Reset()
	}
	if c.updateCollector != nil {
		c.updateCollector.Reset()
	}
	c.deleteCollector = nil
	c.updateCollector = nil
	c.budget.release(c.matchedKeyBytes)
	c.matchedKeyBytes = 0
	c.matchedRows = nil
	c.budget = nil
}

var _ icebergwrite.Coordinator = (*DMLMergeCoordinator)(nil)
var _ icebergwrite.ProcessAwareCoordinator = (*DMLMergeCoordinator)(nil)
var _ DMLMergeActionCommitter = DMLActionExecutor{}

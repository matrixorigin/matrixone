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
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

type DMLDeleteActionStreamRequest struct {
	TableLocation  string
	Schema         api.Schema
	Base           dml.CommitBase
	Operation      dml.Operation
	SnapshotID     int64
	DeleteSchemaID int
	ObjectWriter   dml.DeleteObjectWriter
	Targets        []DMLMatchedDeleteTarget
	MatchedBatches []DMLMatchedRowsBatchRequest
}

type DMLMatchedDeleteTarget struct {
	DataFile        api.DataFile
	EqualityIDs     []int
	EqualityRows    []dml.EqualityDeleteRow
	PositionRows    []dml.PositionDeleteRow
	PredicateStable bool
	HasRowOrdinal   bool
}

type DMLUpdateActionStreamRequest struct {
	DMLDeleteActionStreamRequest
	AppendedDataFiles  []api.DataFile
	ReplacementBatch   DMLReplacementDataBatch
	ReplacementBatches []DMLReplacementDataBatch
}

type DMLMatchedUpdateTarget struct {
	DeleteTarget     DMLMatchedDeleteTarget
	ReplacementFiles []api.DataFile
}

type DMLReplacementDataBatch struct {
	Attrs               []string
	Batch               *batch.Batch
	PartitionSpec       api.PartitionSpec
	TargetFileSizeBytes int64
	TimeZone            *time.Location
	OutputFactory       icebergwrite.DataFileOutputFactory
	ObjectWriter        dml.DeleteObjectWriter
	FileSequence        int
}

type DMLMergeActionStreamRequest struct {
	TableLocation                   string
	Schema                          api.Schema
	Base                            dml.CommitBase
	SnapshotID                      int64
	DeleteSchemaID                  int
	ObjectWriter                    dml.DeleteObjectWriter
	MatchedDeletes                  []DMLMatchedDeleteTarget
	MatchedUpdates                  []DMLMatchedUpdateTarget
	MatchedUpdateReplacementBatch   DMLReplacementDataBatch
	MatchedUpdateReplacementBatches []DMLReplacementDataBatch
	UnmatchedAppends                []api.DataFile
	UnmatchedAppendBatch            DMLReplacementDataBatch
	UnmatchedAppendBatches          []DMLReplacementDataBatch
}

type DMLOverwriteActionStreamRequest struct {
	TableLocation      string
	SnapshotID         int64
	Schema             api.Schema
	Base               dml.CommitBase
	Scope              dml.OverwriteScope
	Partition          map[string]any
	PartitionSpec      api.PartitionSpec
	ObjectWriter       dml.DeleteObjectWriter
	AffectedDataFiles  []api.DataFile
	AffectedScanPlan   *api.IcebergScanPlan
	ReplacementFiles   []api.DataFile
	ReplacementBatch   DMLReplacementDataBatch
	ReplacementBatches []DMLReplacementDataBatch
}

func BuildDMLDeleteActionStream(ctx context.Context, req DMLDeleteActionStreamRequest) (*dml.ActionStream, error) {
	req.Operation = dmlOperationOrDefault(req.Operation, dml.OperationDelete)
	base, err := normalizeDMLActionBase(ctx, req.Base)
	if err != nil {
		return nil, err
	}
	req.Base = base
	targets, err := buildDMLDeleteTargets(ctx, req)
	if err != nil {
		return nil, err
	}
	stream, err := (dml.NativePlanner{}).PlanDelete(ctx, dml.DeleteRequest{
		Base:    req.Base,
		Mode:    dml.TableModeMergeOnRead,
		Targets: targets,
	})
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	return stream, nil
}

func BuildDMLUpdateActionStream(ctx context.Context, req DMLUpdateActionStreamRequest) (*dml.ActionStream, error) {
	base, err := normalizeDMLActionBase(ctx, req.Base)
	if err != nil {
		return nil, err
	}
	req.Base = base
	req.DMLDeleteActionStreamRequest.Base = base
	appendedDataFiles := append([]api.DataFile(nil), req.AppendedDataFiles...)
	if len(appendedDataFiles) == 0 {
		for idx, replacementBatch := range req.ReplacementBatches {
			replacementBatch = replacementBatchWithSequence(replacementBatch, idx+1)
			files, err := materializeDMLReplacementBatch(ctx, dml.OperationUpdate, req.Base, req.TableLocation, req.SnapshotID, req.Schema, replacementBatch, req.ObjectWriter)
			if err != nil {
				return nil, err
			}
			appendedDataFiles = append(appendedDataFiles, files...)
		}
	}
	if len(appendedDataFiles) == 0 {
		files, err := materializeDMLReplacementBatch(ctx, dml.OperationUpdate, req.Base, req.TableLocation, req.SnapshotID, req.Schema, req.ReplacementBatch, req.ObjectWriter)
		if err != nil {
			return nil, err
		}
		appendedDataFiles = append(appendedDataFiles, files...)
	}
	if len(appendedDataFiles) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML update action stream requires replacement data files", map[string]string{
			"table": req.Base.Table,
		}))
	}
	deleteReq := req.DMLDeleteActionStreamRequest
	deleteReq.Operation = dml.OperationUpdate
	deleteTargets, err := buildDMLDeleteTargets(ctx, deleteReq)
	if err != nil {
		return nil, err
	}
	updateTargets := make([]dml.UpdateTarget, 0, len(deleteTargets))
	for _, target := range deleteTargets {
		updateTargets = append(updateTargets, dml.UpdateTarget{DeleteTarget: target})
	}
	stream, err := (dml.NativePlanner{}).PlanUpdate(ctx, dml.UpdateRequest{
		Base:             req.Base,
		Mode:             dml.TableModeMergeOnRead,
		Targets:          updateTargets,
		AppendedDataFile: appendedDataFiles,
	})
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	return stream, nil
}

func BuildDMLMergeActionStream(ctx context.Context, req DMLMergeActionStreamRequest) (*dml.ActionStream, error) {
	base, err := normalizeDMLActionBase(ctx, req.Base)
	if err != nil {
		return nil, err
	}
	req.Base = base
	deleteReq := DMLDeleteActionStreamRequest{
		TableLocation:  req.TableLocation,
		Schema:         req.Schema,
		Base:           req.Base,
		Operation:      dml.OperationMerge,
		SnapshotID:     req.SnapshotID,
		DeleteSchemaID: req.DeleteSchemaID,
		ObjectWriter:   req.ObjectWriter,
	}
	var matchedDeletes []dml.DeleteTarget
	if len(req.MatchedDeletes) > 0 {
		deleteReq.Targets = req.MatchedDeletes
		var err error
		matchedDeletes, err = buildDMLDeleteTargets(ctx, deleteReq)
		if err != nil {
			return nil, err
		}
	}
	matchedUpdates := make([]dml.UpdateTarget, 0, len(req.MatchedUpdates))
	if len(req.MatchedUpdates) > 0 {
		updateTargets := make([]DMLMatchedDeleteTarget, 0, len(req.MatchedUpdates))
		for _, target := range req.MatchedUpdates {
			updateTargets = append(updateTargets, target.DeleteTarget)
		}
		deleteReq.Targets = updateTargets
		deleteTargets, err := buildDMLDeleteTargets(ctx, deleteReq)
		if err != nil {
			return nil, err
		}
		for idx, deleteTarget := range deleteTargets {
			matchedUpdates = append(matchedUpdates, dml.UpdateTarget{
				DeleteTarget:     deleteTarget,
				ReplacementFiles: append([]api.DataFile(nil), req.MatchedUpdates[idx].ReplacementFiles...),
			})
		}
	}
	var matchedUpdateReplacements []api.DataFile
	nextReplacementSeq := 1
	for _, replacementBatch := range req.MatchedUpdateReplacementBatches {
		replacementBatch = replacementBatchWithSequence(replacementBatch, nextReplacementSeq)
		nextReplacementSeq++
		files, err := materializeDMLReplacementBatch(ctx, dml.OperationMerge, req.Base, req.TableLocation, req.SnapshotID, req.Schema, replacementBatch, req.ObjectWriter)
		if err != nil {
			return nil, err
		}
		matchedUpdateReplacements = append(matchedUpdateReplacements, files...)
	}
	if len(matchedUpdateReplacements) == 0 {
		replacementBatch := replacementBatchWithSequence(req.MatchedUpdateReplacementBatch, nextReplacementSeq)
		files, err := materializeDMLReplacementBatch(ctx, dml.OperationMerge, req.Base, req.TableLocation, req.SnapshotID, req.Schema, replacementBatch, req.ObjectWriter)
		if err != nil {
			return nil, err
		}
		if len(files) > 0 {
			nextReplacementSeq++
		}
		matchedUpdateReplacements = append(matchedUpdateReplacements, files...)
	}
	if len(matchedUpdateReplacements) > 0 {
		if len(matchedUpdates) == 0 {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg MERGE matched-update replacement batch requires matched update targets", map[string]string{
				"table": req.Base.Table,
			}))
		}
		matchedUpdates[0].ReplacementFiles = append(matchedUpdates[0].ReplacementFiles, matchedUpdateReplacements...)
	}
	unmatchedAppends := append([]api.DataFile(nil), req.UnmatchedAppends...)
	for _, unmatchedBatch := range req.UnmatchedAppendBatches {
		unmatchedBatch = replacementBatchWithSequence(unmatchedBatch, nextReplacementSeq)
		nextReplacementSeq++
		files, err := materializeDMLReplacementBatch(ctx, dml.OperationMerge, req.Base, req.TableLocation, req.SnapshotID, req.Schema, unmatchedBatch, req.ObjectWriter)
		if err != nil {
			return nil, err
		}
		unmatchedAppends = append(unmatchedAppends, files...)
	}
	if len(unmatchedAppends) == 0 {
		unmatchedBatch := replacementBatchWithSequence(req.UnmatchedAppendBatch, nextReplacementSeq)
		unmatchedBatchFiles, err := materializeDMLReplacementBatch(ctx, dml.OperationMerge, req.Base, req.TableLocation, req.SnapshotID, req.Schema, unmatchedBatch, req.ObjectWriter)
		if err != nil {
			return nil, err
		}
		unmatchedAppends = append(unmatchedAppends, unmatchedBatchFiles...)
	}
	stream, err := (dml.NativePlanner{}).PlanMerge(ctx, dml.MergeRequest{
		Base:             req.Base,
		Mode:             dml.TableModeMergeOnRead,
		MatchedDeletes:   matchedDeletes,
		MatchedUpdates:   matchedUpdates,
		UnmatchedAppends: unmatchedAppends,
	})
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	return stream, nil
}

func BuildDMLOverwriteActionStream(ctx context.Context, req DMLOverwriteActionStreamRequest) (*dml.ActionStream, error) {
	base, err := normalizeDMLActionBase(ctx, req.Base)
	if err != nil {
		return nil, err
	}
	req.Base = base
	if req.Scope == dml.OverwritePartition {
		partition, err := canonicalizeOverwritePartition(ctx, req.Partition, req.PartitionSpec, req.Base.Table)
		if err != nil {
			return nil, err
		}
		req.Partition = partition
	}
	affectedDataFiles := append([]api.DataFile(nil), req.AffectedDataFiles...)
	if len(affectedDataFiles) == 0 && req.AffectedScanPlan != nil {
		files, err := dmlAffectedDataFilesFromScanPlan(ctx, req.AffectedScanPlan, req.Base.Table, req.Scope, req.Partition)
		if err != nil {
			return nil, err
		}
		affectedDataFiles = files
	} else if req.Scope == dml.OverwritePartition {
		var err error
		affectedDataFiles, err = filterOverwritePartitionDataFiles(ctx, affectedDataFiles, req.Partition, req.Base.Table)
		if err != nil {
			return nil, err
		}
	}
	replacementFiles := append([]api.DataFile(nil), req.ReplacementFiles...)
	if len(replacementFiles) == 0 {
		for idx, replacementBatch := range req.ReplacementBatches {
			replacementBatch = replacementBatchWithSequence(replacementBatch, idx+1)
			files, err := materializeDMLReplacementBatch(ctx, dml.OperationOverwrite, req.Base, req.TableLocation, req.SnapshotID, req.Schema, replacementBatch, req.ObjectWriter)
			if err != nil {
				return nil, err
			}
			replacementFiles = append(replacementFiles, files...)
		}
	}
	if len(replacementFiles) == 0 {
		files, err := materializeDMLReplacementBatch(ctx, dml.OperationOverwrite, req.Base, req.TableLocation, req.SnapshotID, req.Schema, req.ReplacementBatch, req.ReplacementBatch.ObjectWriter)
		if err != nil {
			return nil, err
		}
		replacementFiles = append(replacementFiles, files...)
	}
	stream, err := (dml.NativePlanner{}).PlanOverwrite(ctx, dml.OverwriteRequest{
		Base:              req.Base,
		Scope:             req.Scope,
		Partition:         cloneDMLAnyMap(req.Partition),
		AffectedDataFiles: affectedDataFiles,
		ReplacementFiles:  replacementFiles,
	})
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	return stream, nil
}

func normalizeDMLActionBase(ctx context.Context, base dml.CommitBase) (dml.CommitBase, error) {
	normalized, err := dml.NormalizeCommitBaseRef(base)
	if err != nil {
		return dml.CommitBase{}, api.ToMOErr(ctx, err)
	}
	return normalized, nil
}

func buildDMLDeleteTargets(ctx context.Context, req DMLDeleteActionStreamRequest) ([]dml.DeleteTarget, error) {
	targetsFromBatches, err := buildDMLMatchedBatchTargets(ctx, req.MatchedBatches)
	if err != nil {
		return nil, err
	}
	req.Targets = append(append([]DMLMatchedDeleteTarget(nil), req.Targets...), targetsFromBatches...)
	tableLocation := strings.TrimRight(strings.TrimSpace(req.TableLocation), "/")
	if tableLocation == "" {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete action stream requires table location", map[string]string{
			"table": req.Base.Table,
		}))
	}
	if req.SnapshotID <= 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete action stream requires snapshot id", map[string]string{
			"table": req.Base.Table,
		}))
	}
	if req.ObjectWriter == nil {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete action stream requires object writer", map[string]string{
			"table": req.Base.Table,
		}))
	}
	operation := dmlOperationOrDefault(req.Operation, dml.OperationDelete)
	if len(req.Targets) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML delete action stream requires matched targets", map[string]string{
			"table": req.Base.Table,
		}))
	}
	deleteSchemaID := req.DeleteSchemaID
	if deleteSchemaID == 0 {
		deleteSchemaID = req.Schema.SchemaID
	}
	targets := make([]dml.DeleteTarget, 0, len(req.Targets))
	for idx, target := range req.Targets {
		built, err := buildDMLDeleteTarget(ctx, req, operation, tableLocation, deleteSchemaID, idx+1, target)
		if err != nil {
			return nil, err
		}
		targets = append(targets, built)
	}
	return targets, nil
}

func buildDMLDeleteTarget(ctx context.Context, req DMLDeleteActionStreamRequest, operation dml.Operation, tableLocation string, deleteSchemaID, sequence int, target DMLMatchedDeleteTarget) (dml.DeleteTarget, error) {
	dataPath := strings.TrimSpace(target.DataFile.FilePath)
	if dataPath == "" {
		return dml.DeleteTarget{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML delete target requires data file path", map[string]string{
			"table": req.Base.Table,
		}))
	}
	out := dml.DeleteTarget{DataFile: target.DataFile}
	if target.PredicateStable && len(target.EqualityIDs) > 0 && len(target.EqualityRows) > 0 {
		path, err := BuildDMLDeleteFilePath(ctx, DMLDeleteFilePathRequest{
			TableLocation:      tableLocation,
			Stream:             dml.ActionStream{Operation: operation, Base: req.Base},
			SnapshotID:         req.SnapshotID,
			DeleteKind:         dml.ActionAddEqualityDelete,
			TargetDataFilePath: dataPath,
			FileSequence:       sequence,
		})
		if err != nil {
			return dml.DeleteTarget{}, err
		}
		file, err := dml.WriteEqualityDeleteObject(ctx, req.ObjectWriter, dml.EqualityDeleteWriteRequest{
			FilePath:       path,
			Schema:         req.Schema,
			EqualityIDs:    target.EqualityIDs,
			Rows:           target.EqualityRows,
			Partition:      target.DataFile.Partition,
			SpecID:         target.DataFile.SpecID,
			DeleteSchemaID: deleteSchemaID,
		})
		if err != nil {
			return dml.DeleteTarget{}, api.ToMOErr(ctx, err)
		}
		out.MatchedRows = int64(len(target.EqualityRows))
		out.EqualityFieldIDs = append([]int(nil), target.EqualityIDs...)
		out.PredicateStable = true
		out.EqualityDeleteFile = file
		return out, nil
	}
	if target.HasRowOrdinal && len(target.PositionRows) > 0 {
		rows := append([]dml.PositionDeleteRow(nil), target.PositionRows...)
		for idx := range rows {
			if strings.TrimSpace(rows[idx].FilePath) == "" {
				rows[idx].FilePath = dataPath
			}
		}
		path, err := BuildDMLDeleteFilePath(ctx, DMLDeleteFilePathRequest{
			TableLocation:      tableLocation,
			Stream:             dml.ActionStream{Operation: operation, Base: req.Base},
			SnapshotID:         req.SnapshotID,
			DeleteKind:         dml.ActionAddPositionDelete,
			TargetDataFilePath: dataPath,
			FileSequence:       sequence,
		})
		if err != nil {
			return dml.DeleteTarget{}, err
		}
		file, err := dml.WritePositionDeleteObject(ctx, req.ObjectWriter, dml.PositionDeleteWriteRequest{
			FilePath:           path,
			Rows:               rows,
			ReferencedDataFile: dataPath,
			Partition:          target.DataFile.Partition,
			SpecID:             target.DataFile.SpecID,
			DeleteSchemaID:     deleteSchemaID,
		})
		if err != nil {
			return dml.DeleteTarget{}, api.ToMOErr(ctx, err)
		}
		out.MatchedRows = int64(len(rows))
		out.HasRowOrdinal = true
		out.PositionDeleteFile = file
		return out, nil
	}
	return dml.DeleteTarget{}, api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML delete target cannot be materialized from matched rows", map[string]string{
		"table": req.Base.Table,
		"path":  api.RedactPath(dataPath),
	}))
}

func dmlOperationOrDefault(operation, fallback dml.Operation) dml.Operation {
	if strings.TrimSpace(string(operation)) == "" {
		return fallback
	}
	return operation
}

func buildDMLMatchedBatchTargets(ctx context.Context, batches []DMLMatchedRowsBatchRequest) ([]DMLMatchedDeleteTarget, error) {
	if len(batches) == 0 {
		return nil, nil
	}
	targets := make([]DMLMatchedDeleteTarget, 0, len(batches))
	for _, bat := range batches {
		target, err := BuildDMLMatchedDeleteTargetFromBatch(ctx, bat)
		if err != nil {
			return nil, err
		}
		if len(target.EqualityRows) == 0 && len(target.PositionRows) == 0 {
			continue
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func dmlAffectedDataFilesFromScanPlan(ctx context.Context, plan *api.IcebergScanPlan, table string, scope dml.OverwriteScope, partition map[string]any) ([]api.DataFile, error) {
	if plan == nil {
		return nil, nil
	}
	if len(plan.DeleteTasks) > 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg overwrite affected-file discovery with delete tasks is not supported yet", map[string]string{
			"table": table,
		}))
	}
	files := make([]api.DataFile, 0, len(plan.DataTasks))
	seen := make(map[string]struct{}, len(plan.DataTasks))
	for _, task := range plan.DataTasks {
		file := task.DataFile
		path := strings.TrimSpace(file.FilePath)
		if path == "" {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg overwrite affected data file is missing path", map[string]string{
				"table": table,
			}))
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		files = append(files, file)
	}
	if scope == dml.OverwritePartition {
		return filterOverwritePartitionDataFiles(ctx, files, partition, table)
	}
	return files, nil
}

func materializeDMLReplacementBatch(ctx context.Context, operation dml.Operation, base dml.CommitBase, tableLocation string, snapshotID int64, schema api.Schema, replacement DMLReplacementDataBatch, fallbackWriter dml.DeleteObjectWriter) ([]api.DataFile, error) {
	if replacement.Batch == nil || replacement.Batch.RowCount() == 0 {
		return nil, nil
	}
	attrs := replacement.Attrs
	if len(attrs) == 0 {
		attrs = replacement.Batch.Attrs
	}
	if replacement.ObjectWriter == nil {
		replacement.ObjectWriter = fallbackWriter
	}
	return WriteDMLReplacementDataFiles(ctx, DMLReplacementDataFilesRequest{
		TableLocation:       tableLocation,
		Operation:           operation,
		Base:                base,
		SnapshotID:          snapshotID,
		Schema:              schema,
		PartitionSpec:       replacement.PartitionSpec,
		Attrs:               attrs,
		Batch:               replacement.Batch,
		TargetFileSizeBytes: replacement.TargetFileSizeBytes,
		TimeZone:            replacement.TimeZone,
		OutputFactory:       replacement.OutputFactory,
		ObjectWriter:        replacement.ObjectWriter,
		FileSequence:        replacement.FileSequence,
	})
}

func replacementBatchWithSequence(replacement DMLReplacementDataBatch, sequence int) DMLReplacementDataBatch {
	if replacement.FileSequence == 0 && replacement.Batch != nil && replacement.Batch.RowCount() > 0 {
		replacement.FileSequence = sequence
	}
	return replacement
}

func overwriteScopeOrDefault(scope dml.OverwriteScope) dml.OverwriteScope {
	if strings.TrimSpace(string(scope)) == "" {
		return dml.OverwriteTable
	}
	return scope
}

func filterOverwritePartitionDataFiles(ctx context.Context, files []api.DataFile, partition map[string]any, table string) ([]api.DataFile, error) {
	if len(partition) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg partition overwrite requires an explicit partition tuple", map[string]string{
			"table": table,
		}))
	}
	out := make([]api.DataFile, 0, len(files))
	for _, file := range files {
		if dmlPartitionContains(file.Partition, partition) {
			out = append(out, file)
		}
	}
	return out, nil
}

func canonicalizeOverwritePartition(ctx context.Context, partition map[string]any, spec api.PartitionSpec, table string) (map[string]any, error) {
	if len(partition) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg partition overwrite requires an explicit partition tuple", map[string]string{
			"table": table,
		}))
	}
	allowed := make(map[string]string, len(spec.Fields))
	for _, field := range spec.Fields {
		name := strings.TrimSpace(field.Name)
		if name != "" {
			allowed[strings.ToLower(name)] = name
		}
	}
	if len(allowed) == 0 {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg partition overwrite requires a partitioned Iceberg table", map[string]string{
			"table": table,
		}))
	}
	canonical := make(map[string]any, len(partition))
	for key := range partition {
		normalized := strings.ToLower(strings.TrimSpace(key))
		canonicalKey, ok := allowed[normalized]
		if !ok {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg partition overwrite field is not present in the target partition spec", map[string]string{
				"table":           table,
				"partition_field": key,
			}))
		}
		if _, exists := canonical[canonicalKey]; exists {
			return nil, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "duplicate Iceberg partition overwrite field after canonicalization", map[string]string{
				"table":           table,
				"partition_field": key,
			}))
		}
		canonical[canonicalKey] = partition[key]
	}
	return canonical, nil
}

func dmlPartitionContains(filePartition, target map[string]any) bool {
	if len(target) == 0 {
		return false
	}
	for key, want := range target {
		got, ok := lookupDMLPartitionValue(filePartition, key)
		if !ok || !dmlPartitionValueEqual(got, want) {
			return false
		}
	}
	return true
}

func lookupDMLPartitionValue(partition map[string]any, key string) (any, bool) {
	got, ok := partition[key]
	if ok {
		return got, true
	}
	normalized := strings.ToLower(strings.TrimSpace(key))
	for candidate, value := range partition {
		if strings.ToLower(strings.TrimSpace(candidate)) == normalized {
			return value, true
		}
	}
	return nil, false
}

func dmlPartitionValueEqual(left, right any) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return dmlPartitionValueToken(left) == dmlPartitionValueToken(right)
}

func dmlPartitionValueToken(value any) string {
	switch v := value.(type) {
	case string:
		return "s:" + v
	case bool:
		if v {
			return "b:1"
		}
		return "b:0"
	case int:
		return fmt.Sprintf("i:%d", v)
	case int8:
		return fmt.Sprintf("i:%d", v)
	case int16:
		return fmt.Sprintf("i:%d", v)
	case int32:
		return fmt.Sprintf("i:%d", v)
	case int64:
		return fmt.Sprintf("i:%d", v)
	case uint:
		return fmt.Sprintf("u:%d", v)
	case uint8:
		return fmt.Sprintf("u:%d", v)
	case uint16:
		return fmt.Sprintf("u:%d", v)
	case uint32:
		return fmt.Sprintf("u:%d", v)
	case uint64:
		return fmt.Sprintf("u:%d", v)
	case float32:
		return fmt.Sprintf("f:%g", v)
	case float64:
		return fmt.Sprintf("f:%g", v)
	default:
		return fmt.Sprintf("%T:%v", value, value)
	}
}

func cloneDMLAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

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

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
)

type DMLManifestPathRequest struct {
	TableLocation string
	Stream        dml.ActionStream
	SnapshotID    int64
}

type DMLManifestPaths struct {
	DataManifestPath   string
	DeleteManifestPath string
	ManifestListPath   string
}

type DMLDeleteFilePathRequest struct {
	TableLocation      string
	Stream             dml.ActionStream
	SnapshotID         int64
	DeleteKind         dml.ActionKind
	TargetDataFilePath string
	FileSequence       int
}

func BuildDMLManifestPaths(ctx context.Context, req DMLManifestPathRequest) (DMLManifestPaths, error) {
	tableLocation := strings.TrimRight(strings.TrimSpace(req.TableLocation), "/")
	if tableLocation == "" {
		return DMLManifestPaths{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML manifest paths require table location", map[string]string{
			"table": req.Stream.Base.Table,
		}))
	}
	if req.SnapshotID <= 0 {
		return DMLManifestPaths{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML manifest paths require snapshot id", map[string]string{
			"table": req.Stream.Base.Table,
		}))
	}
	operation, err := normalizedDMLOperation(ctx, req.Stream.Operation, req.Stream.Base.Table)
	if err != nil {
		return DMLManifestPaths{}, err
	}
	idempotencyKey := firstNonEmpty(req.Stream.Base.StatementID, req.Stream.Base.IdempotencyKey)
	if idempotencyKey == "" {
		return DMLManifestPaths{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML manifest paths require statement id or idempotency key", map[string]string{
			"table": req.Stream.Base.Table,
		}))
	}
	base := joinDMLObjectPath(tableLocation, "metadata", "mo-dml", operation, "stmt-"+api.PathHash(idempotencyKey))
	paths := DMLManifestPaths{
		ManifestListPath: joinDMLObjectPath(base, "manifest-list-snap-"+strconv.FormatInt(req.SnapshotID, 10)+".avro"),
	}
	needsData, needsDelete := dmlManifestKinds(req.Stream.Actions)
	if needsData {
		paths.DataManifestPath = joinDMLObjectPath(base, "data-manifest.avro")
	}
	if needsDelete {
		paths.DeleteManifestPath = joinDMLObjectPath(base, "delete-manifest.avro")
	}
	return paths, nil
}

func BuildDMLDeleteFilePath(ctx context.Context, req DMLDeleteFilePathRequest) (string, error) {
	tableLocation := strings.TrimRight(strings.TrimSpace(req.TableLocation), "/")
	if tableLocation == "" {
		return "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete file path requires table location", map[string]string{
			"table": req.Stream.Base.Table,
		}))
	}
	if req.SnapshotID <= 0 {
		return "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete file path requires snapshot id", map[string]string{
			"table": req.Stream.Base.Table,
		}))
	}
	if req.FileSequence <= 0 {
		return "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete file path requires positive file sequence", map[string]string{
			"table": req.Stream.Base.Table,
		}))
	}
	dataPathHash := api.PathHash(strings.TrimSpace(req.TargetDataFilePath))
	if dataPathHash == "" {
		return "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete file path requires target data file path", map[string]string{
			"table": req.Stream.Base.Table,
		}))
	}
	kind, err := normalizedDMLDeleteKind(ctx, req.DeleteKind, req.Stream.Base.Table)
	if err != nil {
		return "", err
	}
	operation, err := normalizedDMLOperation(ctx, req.Stream.Operation, req.Stream.Base.Table)
	if err != nil {
		return "", err
	}
	idempotencyKey := firstNonEmpty(req.Stream.Base.StatementID, req.Stream.Base.IdempotencyKey)
	if idempotencyKey == "" {
		return "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML delete file path requires statement id or idempotency key", map[string]string{
			"table": req.Stream.Base.Table,
		}))
	}
	fileName := "delete-snap-" + strconv.FormatInt(req.SnapshotID, 10) +
		"-seq-" + strconv.Itoa(req.FileSequence) +
		"-df-" + dataPathHash + ".parquet"
	return joinDMLObjectPath(tableLocation, "data", "mo-dml", operation, "stmt-"+api.PathHash(idempotencyKey), "delete", kind, fileName), nil
}

func normalizedDMLOperation(ctx context.Context, operation dml.Operation, table string) (string, error) {
	switch operation {
	case dml.OperationDelete, dml.OperationUpdate, dml.OperationMerge, dml.OperationOverwrite:
		return string(operation), nil
	default:
		return "", api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML manifest paths require a supported operation", map[string]string{
			"operation": strings.TrimSpace(string(operation)),
			"table":     table,
		}))
	}
}

func normalizedDMLDeleteKind(ctx context.Context, kind dml.ActionKind, table string) (string, error) {
	switch kind {
	case dml.ActionAddEqualityDelete:
		return "equality", nil
	case dml.ActionAddPositionDelete:
		return "position", nil
	default:
		return "", api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML delete file path requires a delete action kind", map[string]string{
			"kind":  strings.TrimSpace(string(kind)),
			"table": table,
		}))
	}
}

func dmlManifestKinds(actions []dml.Action) (needsData, needsDelete bool) {
	for _, action := range actions {
		switch action.Kind {
		case dml.ActionAppendData, dml.ActionDeleteDataFile, dml.ActionRewriteDataFile:
			needsData = true
		case dml.ActionAddEqualityDelete, dml.ActionAddPositionDelete:
			needsDelete = true
		}
	}
	return needsData, needsDelete
}

func joinDMLObjectPath(base string, parts ...string) string {
	out := strings.TrimRight(strings.TrimSpace(base), "/")
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), "/")
		if part == "" {
			continue
		}
		if out == "" {
			out = part
		} else {
			out += "/" + part
		}
	}
	return out
}

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

package write

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type AppendSourceKind string

const (
	AppendSourceSQLInsert  AppendSourceKind = "sql_insert"
	AppendSourceCTAS       AppendSourceKind = "ctas_existing_mapping"
	AppendSourceSink       AppendSourceKind = "sink"
	AppendSourceMOIPublish AppendSourceKind = "moi_publish"
)

type AppendRequestSpec struct {
	api.CatalogRequest
	Namespace            api.Namespace
	Table                string
	TableLocation        string
	TargetRef            string
	TargetRefType        string
	AllowTagMove         bool
	CatalogCapabilities  api.CatalogCapabilities
	TableUUID            string
	BaseSnapshotID       int64
	BaseSchemaID         int
	BaseSpecID           int
	BaseSortOrderID      int
	BaseSchema           api.Schema
	BaseSpec             api.PartitionSpec
	KnownPartitionSpecs  []api.PartitionSpec
	WriterOwnerAccountID uint32
	DataFiles            []api.DataFile
	IdempotencyKey       string
	SourceKind           AppendSourceKind
	SourceBatch          string
	SourceQueryID        string
	WriterID             string
	StatementID          string
	ExistingMapping      bool
	CatalogCreateAllowed bool
	Summary              map[string]string
	PublishAuditHint     api.PublishAuditHint
}

func BuildAppendRequest(ctx context.Context, spec AppendRequestSpec) (api.AppendRequest, error) {
	if strings.TrimSpace(spec.Table) == "" || len(spec.Namespace) == 0 {
		return api.AppendRequest{}, api.NewError(api.ErrConfigInvalid, "Iceberg append request requires namespace and table", nil)
	}
	if strings.TrimSpace(spec.IdempotencyKey) == "" {
		return api.AppendRequest{}, api.NewError(api.ErrConfigInvalid, "Iceberg append request requires an idempotency key", nil)
	}
	if spec.TargetRef == "" {
		spec.TargetRef = "main"
	}
	if spec.SourceKind == "" {
		spec.SourceKind = AppendSourceSQLInsert
	}
	if spec.SourceKind == AppendSourceCTAS && !spec.ExistingMapping && !spec.CatalogCreateAllowed {
		return api.AppendRequest{}, api.NewError(api.ErrUnsupportedFeature, "Iceberg CTAS requires an existing mapping unless catalog create-table capability is enabled", map[string]string{"table": spec.Table})
	}
	summary := cloneStringMap(spec.Summary)
	if summary == nil {
		summary = make(map[string]string)
	}
	summary["source-kind"] = string(spec.SourceKind)
	if spec.SourceQueryID != "" {
		summary["source-query-id"] = spec.SourceQueryID
	}
	if spec.SourceBatch != "" {
		summary["source-batch"] = spec.SourceBatch
	}
	req := api.AppendRequest{
		CatalogRequest:       spec.CatalogRequest,
		Namespace:            append(api.Namespace(nil), spec.Namespace...),
		Table:                spec.Table,
		TableLocation:        spec.TableLocation,
		TargetRef:            spec.TargetRef,
		TargetRefType:        spec.TargetRefType,
		AllowTagMove:         spec.AllowTagMove,
		CatalogCapabilities:  spec.CatalogCapabilities,
		TableUUID:            spec.TableUUID,
		BaseSnapshotID:       spec.BaseSnapshotID,
		BaseSchemaID:         spec.BaseSchemaID,
		BaseSpecID:           spec.BaseSpecID,
		BaseSortOrderID:      spec.BaseSortOrderID,
		BaseSchema:           spec.BaseSchema,
		BaseSpec:             spec.BaseSpec,
		KnownPartitionSpecs:  clonePartitionSpecs(spec.KnownPartitionSpecs),
		WriterOwnerAccountID: spec.WriterOwnerAccountID,
		DataFiles:            cloneDataFiles(spec.DataFiles),
		IdempotencyKey:       spec.IdempotencyKey,
		SourceBatch:          spec.SourceBatch,
		SourceQueryID:        spec.SourceQueryID,
		WriterID:             spec.WriterID,
		StatementID:          spec.StatementID,
		Summary:              summary,
		PublishAuditHint:     spec.PublishAuditHint,
	}
	return NormalizeAppendTargetRef(req)
}

func BuildExistingMappingCTASAppendRequest(ctx context.Context, spec AppendRequestSpec) (api.AppendRequest, error) {
	spec.SourceKind = AppendSourceCTAS
	spec.ExistingMapping = true
	return BuildAppendRequest(ctx, spec)
}

func BuildSinkAppendRequest(ctx context.Context, spec AppendRequestSpec) (api.AppendRequest, error) {
	spec.SourceKind = AppendSourceSink
	return BuildAppendRequest(ctx, spec)
}

func BuildMOIPublishAppendRequest(ctx context.Context, spec AppendRequestSpec) (api.AppendRequest, error) {
	spec.SourceKind = AppendSourceMOIPublish
	return BuildAppendRequest(ctx, spec)
}

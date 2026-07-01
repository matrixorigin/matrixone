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

package api

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type MetadataFacade interface {
	AdapterName() string
	ParseTableMetadata(ctx context.Context, data []byte, metadataLocation string) (*TableMetadata, error)
	ReadManifestList(ctx context.Context, data []byte) ([]ManifestFile, error)
	ReadManifest(ctx context.Context, data []byte) ([]ManifestEntry, error)
	ResolveSnapshot(ctx context.Context, meta *TableMetadata, selector SnapshotSelector) (Snapshot, error)
	DetectUnsupportedP0(ctx context.Context, meta *TableMetadata, manifests []ManifestFile, files []DataFile) ([]UnsupportedFeature, error)
}

type SnapshotSelector struct {
	SnapshotID        int64
	HasSnapshotID     bool
	TimestampMS       int64
	HasTimestampMS    bool
	RefName           string
	AllowMainFallback bool
}

type ScanPlanner interface {
	PlanScan(ctx context.Context, req ScanPlanRequest) (*IcebergScanPlan, error)
}

type ScanPlanRequest struct {
	CatalogRequest
	Namespace                Namespace
	Table                    string
	Ref                      string
	Snapshot                 SnapshotSelector
	ProjectionIDs            []int
	ResidualSQL              string
	PrunePredicates          []PrunePredicate
	PlanningTimeout          string
	EnableRowGroupPlanning   bool
	EnableDeleteApply        bool
	DeleteMaxMemoryBytes     int64
	EnableDeleteSpill        bool
	ResidencyPolicies        []model.ResidencyPolicy
	CatalogValidator         CatalogRequestValidator
	ObjectResidencyValidator ObjectResidencyValidator
}

type CatalogRequestValidator func(context.Context, CatalogRequest) error

type ObjectResidencyValidator func(context.Context, ObjectResidencyRequest) error

type ObjectResidencyRequest struct {
	AccountID           uint32
	CatalogID           uint64
	CatalogURI          string
	Endpoint            string
	Region              string
	Bucket              string
	StorageLocation     string
	Principal           string
	CredentialID        string
	CredentialExpiresAt time.Time
}

type PruneOp string

const (
	PruneOpEQ  PruneOp = "eq"
	PruneOpLT  PruneOp = "lt"
	PruneOpLTE PruneOp = "lte"
	PruneOpGT  PruneOp = "gt"
	PruneOpGTE PruneOp = "gte"
)

type PrunePredicate struct {
	FieldID int
	Op      PruneOp
	Literal PruneLiteral
}

type PruneLiteral struct {
	Kind       IcebergTypeKind
	Bool       bool
	Int64      int64
	Float64    float64
	String     string
	Bytes      []byte
	IsNull     bool
	Normalized bool
}

type IcebergScanPlan struct {
	Snapshot                  SnapshotPlan
	DataTasks                 []DataFileTask
	DeleteTasks               []DeleteFileTask
	ColumnMapping             []IcebergColumnMapping
	ResidualFilter            ResidualFilter
	Profile                   PlanningProfile
	ObjectIORef               string
	ServerPredicateEquivalent bool
	DeleteMaxMemoryBytes      int64
	EnableDeleteSpill         bool
}

type SnapshotPlan struct {
	SnapshotID           int64
	SchemaID             int
	PartitionSpecIDs     []int
	MetadataLocation     string
	MetadataLocationHash string
	ManifestList         string
	ManifestListHash     string
	RefName              string
	PlanningMode         string
}

type DataFileTask struct {
	DataFile        DataFile
	ManifestPath    string
	ResidualFilter  ResidualFilter
	CredentialScope string
	RowGroups       []RowGroupSplit
}

type DeleteFileTask struct {
	DataFile        DataFile
	ManifestPath    string
	AppliesToPath   string
	CredentialScope string
	DeleteSchemaID  int
	SequenceNumber  int64
}

type IcebergColumnMapping struct {
	FieldID        int
	ColumnName     string
	MOType         MOType
	Required       bool
	Projected      bool
	ParquetFieldID int
	Hidden         bool
}

type RowGroupSplit struct {
	Ordinal         int32
	StartRowOrdinal int64
	RowCount        int64
	Bytes           int64
	LowerBounds     map[int][]byte
	UpperBounds     map[int][]byte
	NullValueCounts map[int]int64
	ValueCounts     map[int]int64
}

type ResidualFilter struct {
	ExpressionSQL string
	AlwaysTrue    bool
}

type PlanningProfile struct {
	MetadataBytes          int64
	ManifestListBytes      int64
	ManifestBytes          int64
	ManifestsSelected      int
	ManifestsPruned        int
	DataFilesSelected      int
	DataFilesPruned        int
	DataFileBytesSelected  int64
	DataFileBytesPruned    int64
	PlanningCacheHits      int
	PlanningCacheMiss      int
	PlanningMode           string
	RowGroupsSelected      int
	RowGroupsPruned        int
	DeleteFilesSelected    int
	DeleteFilesPruned      int
	ServerPlanningFallback bool
	DeleteRowsFiltered     int64
	DeleteMemoryBytes      int64
}

type FeatureDetector interface {
	DetectUnsupportedP0(ctx context.Context, meta *TableMetadata, manifests []ManifestFile, files []DataFile) ([]UnsupportedFeature, error)
}

type WriteBuilder interface {
	BuildAppend(ctx context.Context, req AppendRequest) (*CommitAttempt, error)
}

type Committer interface {
	CommitTable(ctx context.Context, req CommitRequest) (*CommitResult, error)
}

type MetricsReporter interface {
	ReportMetrics(ctx context.Context, req MetricsReportRequest) error
}

type MetricsReportKind string

const (
	MetricsReportScan  MetricsReportKind = "scan"
	MetricsReportWrite MetricsReportKind = "write"
)

type MetricsReportRequest struct {
	CatalogRequest
	Namespace            Namespace
	Table                string
	Ref                  string
	SnapshotID           int64
	QueryID              string
	StatementID          string
	Kind                 MetricsReportKind
	PlanningProfile      PlanningProfile
	CommitID             string
	MetadataLocationHash string
	Rows                 int64
	Files                int
	Extra                map[string]string
}

const CacheInvalidatorRuntimeKey = "iceberg.cache.invalidator"

type CacheInvalidationHandler interface {
	InvalidateIcebergCache(ctx context.Context, req CacheInvalidationRequest) (int, error)
}

type CacheInvalidationRequest struct {
	AccountID            uint32
	CatalogID            uint64
	Namespace            string
	Table                string
	SnapshotID           int64
	MetadataLocationHash string
	CommitID             string
}

type AppendRequest struct {
	CatalogRequest
	Namespace            Namespace
	Table                string
	TableLocation        string
	TargetRef            string
	TargetRefType        string
	AllowTagMove         bool
	CatalogCapabilities  CatalogCapabilities
	TableUUID            string
	BaseSnapshotID       int64
	BaseSchemaID         int
	BaseSpecID           int
	BaseSortOrderID      int
	BaseSchema           Schema
	BaseSpec             PartitionSpec
	KnownPartitionSpecs  []PartitionSpec
	WriterOwnerAccountID uint32
	DataFiles            []DataFile
	IdempotencyKey       string
	SourceBatch          string
	SourceQueryID        string
	WriterID             string
	StatementID          string
	Summary              map[string]string
	PublishAuditHint     PublishAuditHint
}

type CommitRequest struct {
	CatalogRequest
	Namespace      Namespace
	Table          string
	TargetRef      string
	Requirements   []CommitRequirement
	Updates        []CommitUpdate
	IdempotencyKey string
	Summary        map[string]string
}

type CommitAttempt struct {
	Requirements   []CommitRequirement
	Updates        []CommitUpdate
	DataFiles      []DataFile
	ManifestFiles  []ManifestFile
	Summary        map[string]string
	IdempotencyKey string
	BaseSnapshotID int64
	TargetRef      string
	TargetRefType  string
}

type CommitRequirement struct {
	Type        string
	Ref         string
	SnapshotID  int64
	TableUUID   string
	SchemaID    int
	SpecID      int
	SortOrderID int
}

type CommitUpdate struct {
	Type       string
	Payload    map[string]string
	FilePath   string
	DataFile   *DataFile
	Manifest   *ManifestFile
	Snapshot   *Snapshot
	Ref        string
	RefType    string
	SnapshotID int64
}

type CommitResult struct {
	SnapshotID           int64
	MetadataLocation     string
	MetadataLocationHash string
	CommitID             string
	Unknown              bool
	Verified             bool
}

type PublishAuditHint struct {
	JobID          string
	SourceDB       string
	SourceTable    string
	SourceBatch    string
	WatermarkStart string
	WatermarkEnd   string
	BusinessWindow string
}

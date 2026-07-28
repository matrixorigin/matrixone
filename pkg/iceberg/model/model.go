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

package model

import "time"

const (
	DefaultRefMain = "main"

	AuthModeNone         = "none"
	AuthModeCredential   = "credential"
	AuthModeRemoteSign   = "remote_sign"
	ReadModeAppendOnly   = "append_only"
	ReadModeMergeOnRead  = "merge_on_read"
	WriteModeReadOnly    = "read_only"
	WriteModeAppendOnly  = "append_only"
	WriteModeMergeOnRead = "merge_on_read"

	ResidencyScopeCluster   = "cluster"
	ResidencyScopeAccount   = "account"
	ResidencyWildcard       = "*"
	ResidencyPolicyEnabled  = "enabled"
	ResidencyPolicyDisabled = "disabled"
	ResidencyPolicyAudit    = "audit"

	PrincipalUnspecifiedID uint64 = 0
)

type Catalog struct {
	AccountID        uint32
	CatalogID        uint64
	Name             string
	Type             string
	URI              string
	Warehouse        string
	AuthMode         string
	TokenSecretRef   string
	CapabilitiesJSON string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	DisabledAt       *time.Time
	Version          uint64
}

type PrincipalMap struct {
	AccountID         uint32
	CatalogID         uint64
	MORoleID          uint64
	MOUserID          uint64
	ExternalPrincipal string
	ScopeJSON         string
	CreatedBy         uint64
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Version           uint64
}

type ResidencyPolicy struct {
	ScopeType         string
	AccountID         uint32
	CatalogID         uint64
	AllowedCatalogURI string
	AllowedEndpoint   string
	AllowedRegion     string
	AllowedBucket     string
	PolicyState       string
	CreatedBy         uint64
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Version           uint64
}

type TableMapping struct {
	AccountID                uint32
	DatabaseID               uint64
	TableID                  uint64
	CatalogID                uint64
	Namespace                string
	TableName                string
	DefaultRef               string
	ReadMode                 string
	WriteMode                string
	WriterOwnerAccountID     uint32
	CapabilitiesJSON         string
	LastSnapshotID           string
	LastMetadataLocationHash string
	CreatedAt                time.Time
	UpdatedAt                time.Time
	Version                  uint64
}

type RefCache struct {
	AccountID  uint32
	CatalogID  uint64
	Namespace  string
	TableName  string
	RefName    string
	RefType    string
	SnapshotID string
	LastSeenAt time.Time
	Source     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Version    uint64
}

type PublishJob struct {
	AccountID       uint32
	JobID           string
	SourceDB        string
	SourceTable     string
	TargetCatalogID uint64
	TargetNamespace string
	TargetTable     string
	SourceBatch     string
	WatermarkStart  string
	WatermarkEnd    string
	BusinessWindow  string
	SnapshotID      string
	CommitID        string
	RowCount        uint64
	FileCount       uint64
	Status          string
	ErrorCategory   string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	StatusUpdatedAt time.Time
	Version         uint64
}

type OrphanFile struct {
	AccountID         uint32
	JobID             string
	CatalogID         uint64
	Namespace         string
	TableName         string
	TableLocationHash string
	FilePath          string
	FilePathHash      string
	FilePathRedacted  string
	WrittenAt         time.Time
	ExpireAt          time.Time
	CleanupStatus     string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Version           uint64
}

type MaintenanceJob struct {
	JobID              string
	AccountID          uint32
	CatalogID          uint64
	Namespace          string
	TableName          string
	Operation          string
	TargetRef          string
	SnapshotBefore     string
	SnapshotAfter      string
	RewrittenFileCount uint64
	RemovedFileCount   uint64
	Status             string
	ErrorCategory      string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	StatusUpdatedAt    time.Time
	Version            uint64
}

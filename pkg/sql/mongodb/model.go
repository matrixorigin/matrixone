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

package mongodb

import "time"

const (
	TableConnections = "mo_mongodb_connections"
	TableMappings    = "mo_mongodb_tables"

	DiscoverySeeds = "seeds"
	DiscoverySRV   = "srv"

	ConversionStrict  = "strict"
	ConversionTryNull = "try_null"

	SchemaExplicit = "explicit"
)

// Connection is the non-secret, tenant-scoped configuration persisted in the
// MO catalog. Credentials and CA material are deliberately represented only by
// opaque secret references.
type Connection struct {
	AccountID           uint32
	ConnectionID        uint64
	Name                string
	DiscoveryMode       string
	Hosts               string
	SRVHost             string
	ReplicaSet          string
	AuthSource          string
	AuthMechanism       string
	CredentialSecretRef string
	TLSMode             string
	TLSCASecretRef      string
	ReadPreference      string
	ReadConcern         string
	MaxStalenessSeconds int64
	OptionsJSON         string
	Version             uint64
	Disabled            bool
}

// ColumnMapping maps one MO output column to a scalar BSON dotted path.
type ColumnMapping struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	TypeID      int32  `json:"type_id"`
	Width       int32  `json:"width,omitempty"`
	Scale       int32  `json:"scale,omitempty"`
	NotNullable bool   `json:"not_nullable,omitempty"`
	Conversion  string `json:"conversion,omitempty"`
}

// TableMapping is the catalog authority for one MongoDB external table.
type TableMapping struct {
	AccountID      uint32
	DatabaseID     uint64
	TableID        uint64
	MappingID      uint64
	ConnectionID   uint64
	Connection     string
	Database       string
	Collection     string
	SchemaMode     string
	Conversion     string
	SplitKey       string
	MaxParallelism int32
	Columns        []ColumnMapping
	OptionsJSON    string
	Version        uint64
}

// RuntimeConfig contains process-local limits. It is never serialized into a
// logical plan or pipeline message.
type RuntimeConfig struct {
	Enable                 bool
	EnablePerAccount       bool
	AllowedAccounts        map[uint32]struct{}
	AllowLoopback          bool
	AllowedHostSuffixes    []string
	AllowedCIDRs           []string
	ConnectTimeout         time.Duration
	ServerSelectionTimeout time.Duration
	SocketTimeout          time.Duration
	MaxPoolSize            uint64
	MinPoolSize            uint64
	MaxConnecting          uint64
	MaxCachedClients       int
	BatchRows              int32
	MaxBatchBytes          int64
	MaxValueBytes          int64
	MaxScanRows            int64
	MaxScanBytes           int64
	MaxConversionErrors    int64
	MaxConversionErrorRate float64
	MaxSourceConcurrency   int
}

func (c RuntimeConfig) EnabledFor(accountID uint32) bool {
	if !c.Enable {
		return false
	}
	if !c.EnablePerAccount || accountID == 0 {
		return true
	}
	_, ok := c.AllowedAccounts[accountID]
	return ok
}

func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		Enable:                 true,
		ConnectTimeout:         10 * time.Second,
		ServerSelectionTimeout: 10 * time.Second,
		SocketTimeout:          30 * time.Second,
		MaxPoolSize:            32,
		MaxConnecting:          2,
		MaxCachedClients:       64,
		BatchRows:              8192,
		MaxBatchBytes:          64 << 20,
		MaxValueBytes:          16 << 20,
		MaxScanRows:            50_000_000,
		MaxScanBytes:           32 << 30,
		MaxConversionErrors:    1_000,
		MaxConversionErrorRate: 0.10,
		MaxSourceConcurrency:   4,
	}
}

// Credentials is returned by a CN-local resolver. It must never be formatted,
// logged, stored in catalog metadata, or encoded into protobuf messages.
type Credentials struct {
	Username string
	Password string
	TLSCA    []byte
}

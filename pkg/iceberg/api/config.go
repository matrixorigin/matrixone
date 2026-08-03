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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moconfig "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

const (
	ConfigKeyEnable             = "iceberg.enable"
	ConfigKeyEnablePerAccount   = "iceberg.enable.per_account"
	ConfigKeyManifestCacheBytes = "iceberg.scan.manifest_cache_bytes"
	ConfigKeyManifestCacheTTL   = "iceberg.scan.manifest_cache_ttl"
	ConfigKeyManifestParallel   = "iceberg.scan.manifest_read_parallelism"
	ConfigKeyMaxManifestFiles   = "iceberg.scan.max_manifest_files"
	ConfigKeyMaxDataFiles       = "iceberg.scan.max_data_files"
	ConfigKeyPlanningMaxMemory  = "iceberg.scan.planning_max_memory"
	ConfigKeyServerPlanningMode = "iceberg.scan.server_planning"
	ConfigKeyWriteEnabled       = "iceberg.write.enable"
	ConfigKeyDeleteEnabled      = "iceberg.write.delete.enable"
	ConfigKeyDeleteMaxMemory    = "iceberg.delete.max_memory"
	ConfigKeyDMLMaxMemory       = "iceberg.write.dml_max_memory"
	ConfigKeyDeleteSpillEnabled = "iceberg.delete.spill.enabled"
	ConfigKeyDMLEnabled         = "iceberg.write.dml.enable"
	ConfigKeyMaintenanceEnabled = "iceberg.maintenance.enable"
	ConfigKeyRemoteSigning      = "iceberg.remote_signing.enable"
	ConfigKeyOrphanTTL          = "iceberg.write.orphan_ttl"
	ConfigKeyOrphanGCEnabled    = "iceberg.write.orphan_gc.enabled"
	ConfigKeyProtectedCNToCN    = "iceberg.security.protected_cn_to_cn"
)

type OrphanGCMode string

const (
	OrphanGCRecordOnly OrphanGCMode = "record_only"
	OrphanGCAutomatic  OrphanGCMode = "automatic"
)

type ServerPlanningMode string

const (
	ServerPlanningOff      ServerPlanningMode = moconfig.IcebergServerPlanningOff
	ServerPlanningAuto     ServerPlanningMode = moconfig.IcebergServerPlanningAuto
	ServerPlanningRequired ServerPlanningMode = moconfig.IcebergServerPlanningRequired
)

type Config struct {
	Enable           bool
	EnablePerAccount bool
	Scan             ScanPlanningConfig
	Write            WriteConfig
	Security         SecurityConfig
}

type ScanPlanningConfig struct {
	ManifestCacheBytes      int64
	ManifestCacheTTL        time.Duration
	ManifestReadParallelism int
	MaxManifestFiles        int
	MaxDataFiles            int
	PlanningMaxMemory       int64
	ServerPlanningMode      ServerPlanningMode
	PlanningTimeout         time.Duration
}

type WriteConfig struct {
	EnableWrite       bool
	EnableDelete      bool
	DeleteMaxMemory   int64
	DMLMaxMemory      int64
	EnableDeleteSpill bool
	EnableDML         bool
	EnableMaintenance bool
	EnableRemoteSign  bool
	OrphanTTL         time.Duration
	EnableOrphanGC    bool
}

type SecurityConfig struct {
	ProtectedCNToCN bool
}

type AccountConfig struct {
	AccountID uint32
	Enable    bool
}

func DefaultConfig() Config {
	var params moconfig.IcebergParameters
	params.SetDefaultValues()
	return FromParameters(params)
}

func NewConfigFromParameters(ctx context.Context, params moconfig.IcebergParameters) (Config, error) {
	params.SetDefaultValues()
	if err := params.Validate(ctx); err != nil {
		return Config{}, err
	}
	return FromParameters(params), nil
}

func FromParameters(params moconfig.IcebergParameters) Config {
	return Config{
		Enable:           params.Enable,
		EnablePerAccount: params.EnablePerAccount,
		Scan: ScanPlanningConfig{
			ManifestCacheBytes:      params.ManifestCacheBytes,
			ManifestCacheTTL:        params.ManifestCacheTTL.Duration,
			ManifestReadParallelism: params.ManifestReadParallelism,
			MaxManifestFiles:        params.MaxManifestFiles,
			MaxDataFiles:            params.MaxDataFiles,
			PlanningMaxMemory:       params.PlanningMaxMemory,
			ServerPlanningMode:      ServerPlanningMode(params.ServerPlanningMode),
			PlanningTimeout:         params.PlanningTimeout.Duration,
		},
		Write: WriteConfig{
			EnableWrite:       params.EnableWrite,
			EnableDelete:      params.EnableDelete,
			DeleteMaxMemory:   params.DeleteMaxMemory,
			DMLMaxMemory:      params.DMLMaxMemory,
			EnableDeleteSpill: params.EnableDeleteSpill,
			EnableDML:         params.EnableDML,
			EnableMaintenance: params.EnableMaintenance,
			EnableRemoteSign:  params.EnableRemoteSigning,
			OrphanTTL:         params.OrphanTTL.Duration,
			EnableOrphanGC:    params.EnableOrphanGC,
		},
		Security: SecurityConfig{
			ProtectedCNToCN: params.ProtectedCNToCN,
		},
	}
}

func (c Config) Validate(ctx context.Context) error {
	return c.toParameters().Validate(ctx)
}

func (c Config) EffectiveForAccount(account AccountConfig) Config {
	effective := c
	if c.EnablePerAccount {
		effective.Enable = c.Enable && account.Enable
	}
	return effective
}

func (c Config) OrphanGCMode() OrphanGCMode {
	if c.Write.EnableOrphanGC {
		return OrphanGCAutomatic
	}
	return OrphanGCRecordOnly
}

func WithPlanningTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeoutCause(ctx, timeout, moerr.CauseIcebergPlanning)
}

func (c Config) toParameters() moconfig.IcebergParameters {
	return moconfig.IcebergParameters{
		Enable:                  c.Enable,
		EnablePerAccount:        c.EnablePerAccount,
		ManifestCacheBytes:      c.Scan.ManifestCacheBytes,
		ManifestCacheTTL:        toml.Duration{Duration: c.Scan.ManifestCacheTTL},
		ManifestReadParallelism: c.Scan.ManifestReadParallelism,
		MaxManifestFiles:        c.Scan.MaxManifestFiles,
		MaxDataFiles:            c.Scan.MaxDataFiles,
		PlanningMaxMemory:       c.Scan.PlanningMaxMemory,
		ServerPlanningMode:      string(c.Scan.ServerPlanningMode),
		PlanningTimeout:         toml.Duration{Duration: c.Scan.PlanningTimeout},
		EnableWrite:             c.Write.EnableWrite,
		EnableDelete:            c.Write.EnableDelete,
		DeleteMaxMemory:         c.Write.DeleteMaxMemory,
		DMLMaxMemory:            c.Write.DMLMaxMemory,
		EnableDeleteSpill:       c.Write.EnableDeleteSpill,
		EnableDML:               c.Write.EnableDML,
		EnableMaintenance:       c.Write.EnableMaintenance,
		EnableRemoteSigning:     c.Write.EnableRemoteSign,
		ProtectedCNToCN:         c.Security.ProtectedCNToCN,
		OrphanTTL:               toml.Duration{Duration: c.Write.OrphanTTL},
		EnableOrphanGC:          c.Write.EnableOrphanGC,
	}
}

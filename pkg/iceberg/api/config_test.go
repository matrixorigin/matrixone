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
	"testing"
	"time"

	moconfig "github.com/matrixorigin/matrixone/pkg/config"
)

func TestDefaultConfigIsP0ReadOnly(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Enable {
		t.Fatalf("iceberg must be disabled by default")
	}
	if cfg.Write.EnableWrite || cfg.Write.EnableDelete || cfg.Write.EnableDML || cfg.Write.EnableMaintenance || cfg.Write.EnableRemoteSign || cfg.Write.EnableOrphanGC || cfg.Security.ProtectedCNToCN {
		t.Fatalf("P0 write/delete/dml/maintenance/remote-signing switches must default to false")
	}
	if cfg.Write.OrphanTTL != 24*time.Hour {
		t.Fatalf("orphan ttl should default to 24h, got %s", cfg.Write.OrphanTTL)
	}
	if cfg.OrphanGCMode() != OrphanGCRecordOnly {
		t.Fatalf("V1/P1 orphan GC should default to record-only, got %s", cfg.OrphanGCMode())
	}
	if cfg.Scan.ServerPlanningMode != ServerPlanningAuto {
		t.Fatalf("server planning should default to auto, got %s", cfg.Scan.ServerPlanningMode)
	}
	if cfg.Scan.ManifestCacheTTL != 5*time.Minute {
		t.Fatalf("manifest cache ttl should default to 5m, got %s", cfg.Scan.ManifestCacheTTL)
	}
	if cfg.Scan.MaxManifestFiles != 100000 {
		t.Fatalf("max manifest files should default to 100000, got %d", cfg.Scan.MaxManifestFiles)
	}
	if err := cfg.Validate(context.Background()); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}
}

func TestConfigValidationRejectsBadPlanningValues(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Scan.ManifestReadParallelism = 0
	if err := cfg.Validate(context.Background()); err == nil {
		t.Fatalf("expected invalid parallelism")
	}
	cfg = DefaultConfig()
	cfg.Scan.ServerPlanningMode = "surprise"
	if err := cfg.Validate(context.Background()); err == nil {
		t.Fatalf("expected invalid server planning mode")
	}
	cfg = DefaultConfig()
	cfg.Write.OrphanTTL = -time.Second
	if err := cfg.Validate(context.Background()); err == nil {
		t.Fatalf("expected invalid orphan ttl")
	}
	cfg = DefaultConfig()
	cfg.Write.EnableOrphanGC = true
	if cfg.OrphanGCMode() != OrphanGCAutomatic {
		t.Fatalf("orphan gc mode should reflect explicit enable, got %s", cfg.OrphanGCMode())
	}
}

func TestNewConfigFromParametersUsesLiveConfigValidation(t *testing.T) {
	params := moconfig.IcebergParameters{
		Enable:                  true,
		EnablePerAccount:        true,
		ManifestReadParallelism: -1,
	}
	if _, err := NewConfigFromParameters(context.Background(), params); err == nil {
		t.Fatalf("expected live config validation to reject negative parallelism")
	}
	params.ManifestReadParallelism = 4
	params.ProtectedCNToCN = true
	cfg, err := NewConfigFromParameters(context.Background(), params)
	if err != nil {
		t.Fatalf("unexpected config error: %v", err)
	}
	if cfg.Scan.ManifestReadParallelism != 4 || cfg.Scan.ServerPlanningMode != ServerPlanningAuto {
		t.Fatalf("unexpected bridged config: %+v", cfg)
	}
	if !cfg.Security.ProtectedCNToCN {
		t.Fatalf("expected protected CN-to-CN flag to bridge into api config")
	}
	if cfg.Write.OrphanTTL != 24*time.Hour || cfg.OrphanGCMode() != OrphanGCRecordOnly {
		t.Fatalf("unexpected orphan gc defaults after bridge: %+v", cfg.Write)
	}
}

func TestEffectiveForAccount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enable = true
	cfg.EnablePerAccount = true
	if cfg.EffectiveForAccount(AccountConfig{AccountID: 7, Enable: false}).Enable {
		t.Fatalf("per-account disabled account should turn iceberg off")
	}
	if !cfg.EffectiveForAccount(AccountConfig{AccountID: 7, Enable: true}).Enable {
		t.Fatalf("per-account enabled account should keep iceberg on")
	}
	cfg.EnablePerAccount = false
	if !cfg.EffectiveForAccount(AccountConfig{AccountID: 7, Enable: false}).Enable {
		t.Fatalf("server-only mode should ignore account switch")
	}
}

func TestWithPlanningTimeoutUsesCauseAwareContext(t *testing.T) {
	ctx, cancel := WithPlanningTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if context.Cause(ctx) == nil {
		t.Fatalf("expected timeout cause")
	}
}

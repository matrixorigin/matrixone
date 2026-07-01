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

package config

import (
	"testing"
	"time"
)

func TestIcebergParametersDefaultValues(t *testing.T) {
	var fp FrontendParameters
	fp.SetDefaultValues()
	if fp.Iceberg.Enable {
		t.Fatalf("iceberg server switch must default off")
	}
	if fp.Iceberg.EnableWrite || fp.Iceberg.EnableDelete || fp.Iceberg.EnableDML || fp.Iceberg.EnableMaintenance || fp.Iceberg.EnableRemoteSigning || fp.Iceberg.EnableOrphanGC || fp.Iceberg.ProtectedCNToCN {
		t.Fatalf("P0 non-read switches must default off")
	}
	if fp.Iceberg.ManifestReadParallelism != 8 {
		t.Fatalf("unexpected manifest parallelism: %d", fp.Iceberg.ManifestReadParallelism)
	}
	if fp.Iceberg.ManifestCacheTTL.Duration != 5*time.Minute {
		t.Fatalf("unexpected manifest cache ttl: %s", fp.Iceberg.ManifestCacheTTL.Duration)
	}
	if fp.Iceberg.MaxManifestFiles != 100000 {
		t.Fatalf("unexpected max manifest files: %d", fp.Iceberg.MaxManifestFiles)
	}
	if fp.Iceberg.ServerPlanningMode != IcebergServerPlanningAuto {
		t.Fatalf("unexpected planning mode: %s", fp.Iceberg.ServerPlanningMode)
	}
	if fp.Iceberg.OrphanTTL.Duration != 24*time.Hour {
		t.Fatalf("unexpected orphan ttl: %s", fp.Iceberg.OrphanTTL.Duration)
	}
	if err := fp.Iceberg.Validate(t.Context()); err != nil {
		t.Fatalf("default iceberg config should validate: %v", err)
	}
}

func TestIcebergParametersValidation(t *testing.T) {
	var params IcebergParameters
	params.SetDefaultValues()
	params.ManifestReadParallelism = -1
	if err := params.Validate(t.Context()); err == nil {
		t.Fatalf("expected negative manifest parallelism to fail validation")
	}
	params.SetDefaultValues()
	params.ServerPlanningMode = "local"
	if err := params.Validate(t.Context()); err == nil {
		t.Fatalf("expected non-frozen server planning mode to fail validation")
	}
	params.SetDefaultValues()
	params.OrphanTTL.Duration = -time.Second
	if err := params.Validate(t.Context()); err == nil {
		t.Fatalf("expected negative orphan ttl to fail validation")
	}
}

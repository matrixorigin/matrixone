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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestEnsureFeatureEnabledUsesGlobalGate(t *testing.T) {
	cfg := api.DefaultConfig()
	cfg.Enable = true
	if err := EnsureFeatureEnabled(context.Background(), cfg, 42, "Iceberg scan"); err != nil {
		t.Fatalf("expected enabled global gate: %v", err)
	}
	cfg.Enable = false
	if err := EnsureFeatureEnabled(context.Background(), cfg, 42, "Iceberg scan"); err == nil {
		t.Fatalf("expected disabled global gate to reject")
	}
}

func TestAccountConfigForFeatureGateFailsClosedForPerAccountMode(t *testing.T) {
	cfg := api.DefaultConfig()
	cfg.Enable = true
	cfg.EnablePerAccount = true
	if EffectiveConfigForAccount(cfg, 42).Enable {
		t.Fatalf("per-account mode must fail closed until account enablement is wired")
	}
}

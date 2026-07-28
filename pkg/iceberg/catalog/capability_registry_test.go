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

package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestCapabilityRegistrySupportsAndRequire(t *testing.T) {
	registry := NewCapabilityRegistry(api.CatalogCapabilities{
		CredentialVending:  true,
		RemoteSigning:      true,
		ServerSidePlanning: false,
		BranchTag:          true,
		Commit:             false,
		CreateTable:        true,
		MetricsReport:      true,
	})
	for _, cap := range []Capability{CapabilityCredentialVending, CapabilityRemoteSigning, CapabilityBranchTag, CapabilityCreateTable, CapabilityMetricsReport} {
		if !registry.Supports(cap) {
			t.Fatalf("expected %s to be supported", cap)
		}
	}
	if registry.Supports(CapabilityCommit) {
		t.Fatalf("commit should not be supported")
	}
	err := registry.Require(context.Background(), CapabilityCommit, "append")
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_UNSUPPORTED_FEATURE") || !strings.Contains(err.Error(), "capability=commit") || !strings.Contains(err.Error(), "operation=append") {
		t.Fatalf("expected unsupported capability error, got %v", err)
	}
	if err := registry.Require(context.Background(), CapabilityRemoteSigning, "scan"); err != nil {
		t.Fatalf("supported capability should not error: %v", err)
	}
}

func TestCapabilityRegistryFromResponsesAndSnapshot(t *testing.T) {
	configRegistry := CapabilityRegistryFromConfig(&api.ConfigResponse{Capabilities: api.CatalogCapabilities{ServerSidePlanning: true}})
	if !configRegistry.Supports(CapabilityServerSidePlanning) {
		t.Fatalf("expected server planning from config response")
	}
	loadRegistry := CapabilityRegistryFromLoadTable(&api.LoadTableResponse{Capabilities: api.CatalogCapabilities{CredentialVending: true}})
	if !loadRegistry.Supports(CapabilityCredentialVending) {
		t.Fatalf("expected credential vending from load table response")
	}
	snapshot := loadRegistry.Snapshot()
	if !snapshot[CapabilityCredentialVending] || snapshot[CapabilityCommit] {
		t.Fatalf("unexpected capability snapshot: %+v", snapshot)
	}
	missing := loadRegistry.Missing(CapabilityCredentialVending, CapabilityRemoteSigning, CapabilityCommit, CapabilityCreateTable)
	names := strings.Join(CapabilityNames(missing), ",")
	if names != "commit,create_table,remote_signing" {
		t.Fatalf("unexpected missing capabilities: %s", names)
	}
}

func TestNormalizeCapabilityName(t *testing.T) {
	if NormalizeCapabilityName("server-side-planning") != CapabilityServerSidePlanning {
		t.Fatalf("expected dash form to normalize")
	}
	if NormalizeCapabilityName("remote.signing") != CapabilityRemoteSigning {
		t.Fatalf("expected dot form to normalize")
	}
	if NormalizeCapabilityName("create-table") != CapabilityCreateTable {
		t.Fatalf("expected create-table form to normalize")
	}
	if NormalizeCapabilityName("metrics-report") != CapabilityMetricsReport {
		t.Fatalf("expected metrics-report form to normalize")
	}
}

func TestParseCapabilitiesJSON(t *testing.T) {
	caps, err := ParseCapabilitiesJSON(`{
		"server-side-planning": true,
		"branch_tag": "enabled",
		"metrics.report": 1,
		"remote_signing": false
	}`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !caps.ServerSidePlanning || !caps.BranchTag || !caps.MetricsReport {
		t.Fatalf("expected enabled capabilities: %+v", caps)
	}
	if caps.RemoteSigning {
		t.Fatalf("expected explicit false capability: %+v", caps)
	}
}

func TestParseCapabilitiesJSONList(t *testing.T) {
	caps, err := ParseCapabilitiesJSON(`["credential-vending","create_table"]`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !caps.CredentialVending || !caps.CreateTable {
		t.Fatalf("expected list capabilities: %+v", caps)
	}
}

func TestParseCapabilitiesJSONRejectsInvalidShape(t *testing.T) {
	if _, err := ParseCapabilitiesJSON(`{"commit":"maybe"}`); err == nil {
		t.Fatalf("expected invalid boolean value")
	}
	if _, err := ParseCapabilitiesJSON(`true`); err == nil {
		t.Fatalf("expected invalid json shape")
	}
}

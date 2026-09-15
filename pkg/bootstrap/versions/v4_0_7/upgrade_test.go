// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v4_0_7

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

func TestPythonFunctionRevisionUpgradeContract(t *testing.T) {
	metadata := Handler.Metadata()
	if metadata.Version != "4.0.7" || metadata.MinUpgradeVersion != "4.0.6" {
		t.Fatalf("unexpected version metadata: %+v", metadata)
	}
	if metadata.UpgradeCluster != versions.Yes || metadata.UpgradeTenant != versions.Yes {
		t.Fatalf("4.0.7 must run both cluster and tenant upgrades: %+v", metadata)
	}
	if metadata.VersionOffset != uint32(len(tenantUpgEntries)) {
		t.Fatalf("version offset %d does not match tenant entries %d", metadata.VersionOffset, len(tenantUpgEntries))
	}
	if metadata.RequiredProtocolVersion != defines.MORPCVersion61 {
		t.Fatalf("unexpected 4.0.7 protocol gate: %v", metadata.RequiredProtocolVersion)
	}

	if len(tenantUpgEntries) != 9 {
		t.Fatalf("expected six UDF identity columns, index replacement, and one revision table, got %d entries", len(tenantUpgEntries))
	}
	for _, name := range []string{
		"active_revision", "namespace_version", "canonical_input_descriptor",
		"return_descriptor", "signature_key_schema_version", "signature_fingerprint",
	} {
		var found bool
		for _, entry := range tenantUpgEntries {
			if entry.TableName == "mo_user_defined_function" && strings.Contains(strings.ToLower(entry.UpgSql), "add column "+name) {
				found = true
				if entry.UpgType != versions.ADD_COLUMN || entry.Schema != catalog.MO_CATALOG {
					t.Fatalf("unexpected upgrade entry for %s: %+v", name, entry)
				}
				break
			}
		}
		if !found {
			t.Fatalf("missing mo_user_defined_function.%s upgrade", name)
		}
	}

	var addIndex, dropIndex, revision *versions.UpgradeEntry
	for i := range tenantUpgEntries {
		entry := &tenantUpgEntries[i]
		lower := strings.ToLower(entry.UpgSql)
		switch {
		case strings.Contains(lower, "name_db_arg_types_descriptor"):
			addIndex = entry
		case strings.Contains(lower, "drop index name_db_arg_types"):
			dropIndex = entry
		case entry.TableName == "mo_function_revisions":
			revision = entry
		}
	}
	if addIndex == nil || addIndex.UpgType != versions.ADD_INDEX || addIndex.RequiredProtocolVersion != defines.MORPCVersion48 {
		t.Fatalf("missing current Python signature index upgrade: %+v", addIndex)
	}
	if !strings.Contains(strings.ToLower(addIndex.UpgSql), "canonical_input_descriptor") {
		t.Fatalf("current Python signature index omits exact descriptor: %s", addIndex.UpgSql)
	}
	if dropIndex == nil || dropIndex.UpgType != versions.DROP_INDEX || dropIndex.RequiredProtocolVersion != defines.MORPCVersion48 {
		t.Fatalf("missing legacy signature index removal: %+v", dropIndex)
	}
	if revision == nil {
		t.Fatal("missing Python revision table upgrade")
	}
	if revision.Schema != catalog.MO_CATALOG || revision.TableName != "mo_function_revisions" {
		t.Fatalf("unexpected revision catalog identity: %+v", revision)
	}
	if revision.UpgType != versions.CREATE_NEW_TABLE || revision.RequiredProtocolVersion != defines.MORPCVersion48 {
		t.Fatalf("unexpected revision catalog upgrade: %+v", revision)
	}
	lowerDDL := strings.ToLower(revision.UpgSql)
	for _, column := range []string{
		"function_id", "revision", "namespace_version", "arg_types",
		"definition_schema_version", "abi_contract", "adapter_version",
		"artifact_digest", "environment_digest", "sdk_version", "null_policy",
		"volatility", "definition_fingerprint", "primary key(function_id, revision)",
	} {
		if !strings.Contains(lowerDDL, column) {
			t.Fatalf("revision catalog DDL missing %q: %s", column, revision.UpgSql)
		}
	}
}

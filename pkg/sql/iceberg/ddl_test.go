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
	"strings"
	"testing"
)

func TestResidencyPolicyDDLKeysCatalogURI(t *testing.T) {
	normalized := strings.Join(strings.Fields(ResidencyPolicyDDL), " ")
	for _, want := range []string{
		"allowed_catalog_uri varchar(2048) not null",
		"primary key(scope_type, account_id, catalog_id, allowed_catalog_uri, allowed_endpoint, allowed_region, allowed_bucket)",
	} {
		if !strings.Contains(normalized, want) {
			t.Fatalf("residency policy DDL must include %q in storage identity:\n%s", want, normalized)
		}
	}
}

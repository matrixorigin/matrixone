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

// BranchMetadataLifecycleGateSQL serializes whole-table restore with lineage
// GC before either operation locks mo_branch_metadata, mo_snapshots, or mo_pitr.
// The mo_tables row is preserved by restore, so the gate remains stable while
// mo_branch_metadata itself is replaced.
const BranchMetadataLifecycleGateSQL = "select rel_id from mo_catalog.mo_tables " +
	"where account_id=0 and reldatabase='mo_catalog' and relname='mo_branch_metadata' for update"

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

package iscp

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	iscppkg "github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestHooksRegistered confirms init() registered fulltext2's ISCP Hooks with
// the iscp framework and that the value satisfies the iscp.Hooks interface
// (mirrors the `var _ iscppkg.Hooks = Hooks{}` assertion in iscp.go).
func TestHooksRegistered(t *testing.T) {
	var _ iscppkg.Hooks = Hooks{}

	h, ok := iscppkg.GetHooks(catalog.MoIndexFullText2Algo.ToString())
	require.True(t, ok)
	require.NotNil(t, h)
	require.True(t, iscppkg.HasHooks(catalog.MoIndexFullText2Algo.ToString()))
}

// TestNewSqlWriterErrors drives Hooks.NewSqlWriter through the two error guards
// reachable with plain inputs (no live txn/engine required):
//   - the fulltext2 storage/metadata hidden tables are missing, and
//   - the index has no source column (empty Parts).
//
// The success path and the Run hook need a live CDC consumer pipeline / typed
// tabledef pk resolution, so they are exercised by the fulltext2 BVT cases, not
// here.
func TestNewSqlWriterErrors(t *testing.T) {
	// Missing storage/metadata hidden tables → guarded error before any tabledef
	// dereference, so a nil tabledef is safe here.
	_, err := Hooks{}.NewSqlWriter(
		iscppkg.JobID{},
		&iscppkg.ConsumerInfo{DBName: "db"},
		nil,
		[]*plan.IndexDef{{
			IndexTableName:     "not_a_hidden_table",
			IndexAlgoTableType: "other",
			Parts:              []string{"body"},
		}},
	)
	require.Error(t, err)

	// Storage + metadata present but the index has no source column (empty Parts)
	// → distinct guarded error, again before any tabledef dereference.
	_, err = Hooks{}.NewSqlWriter(
		iscppkg.JobID{},
		&iscppkg.ConsumerInfo{DBName: "db"},
		nil,
		[]*plan.IndexDef{
			{
				IndexTableName:     "storage_tbl",
				IndexAlgoTableType: catalog.FullText2Index_TblType_Storage,
				Parts:              nil,
			},
			{
				IndexTableName:     "meta_tbl",
				IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata,
			},
		},
	)
	require.Error(t, err)
}

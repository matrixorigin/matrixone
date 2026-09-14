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

package compile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// The ALTER ... COPY replica is created from regenerated DDL, which cannot express relkind, so
// the create statement has to carry the original's kind. This asserts the carrying itself:
// delete the WithKeepRelKind call and this fails, which the plan-side tests do not catch --
// they only prove the value is available and that the generated DDL lacks it.
func TestAlterCopyCreateOptionsCarriesRelKind(t *testing.T) {
	for _, kind := range []string{
		catalog.Hnsw_TblType_Metadata,
		catalog.Cagra_TblType_Metadata,
		catalog.Ivfpq_TblType_Metadata,
		catalog.FullText2Index_TblType_Metadata,
		catalog.SystemIndexRel,
		catalog.SystemOrdinaryRel,
	} {
		t.Run(kind, func(t *testing.T) {
			qry := &plan.AlterTable{TableDef: &plan.TableDef{TableType: kind}}

			got, ok := alterCopyCreateOptions(qry).KeepRelKind()
			require.True(t, ok, "the replica create must carry a kind")
			require.Equal(t, kind, got)
		})
	}
}

// The empty kind is a real value (a generic hidden table carries it), so it must be carried
// as present rather than silently dropped -- otherwise buildCreateTable would derive "r" from
// an ordinary-looking replica name and promote the table.
func TestAlterCopyCreateOptionsCarriesEmptyRelKind(t *testing.T) {
	qry := &plan.AlterTable{TableDef: &plan.TableDef{TableType: ""}}

	got, ok := alterCopyCreateOptions(qry).KeepRelKind()
	require.True(t, ok, "empty is carried as present, not treated as unset")
	require.Equal(t, "", got)
}

// The logical id is carried only when there is one, and the two carried values coexist.
func TestAlterCopyCreateOptionsCarriesLogicalId(t *testing.T) {
	withID := &plan.AlterTable{TableDef: &plan.TableDef{TableType: "r", LogicalId: 77}}
	opts := alterCopyCreateOptions(withID)
	require.Equal(t, uint64(77), opts.KeepLogicalId())
	kind, ok := opts.KeepRelKind()
	require.True(t, ok)
	require.Equal(t, "r", kind)

	none := &plan.AlterTable{TableDef: &plan.TableDef{TableType: "r"}}
	require.Equal(t, uint64(0), alterCopyCreateOptions(none).KeepLogicalId())
}

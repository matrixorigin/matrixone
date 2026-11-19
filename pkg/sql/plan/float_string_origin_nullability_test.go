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

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// An explicit binary-to-text cast may synthesize NULL at runtime for invalid
// UTF-8 input, so the cast expression binds nullable. COALESCE of that cast
// with a NOT NULL literal still binds NotNullable=true at the root: the root
// nullability comes from the NOT NULL argument, not from the cast descendant.
// Origin identity includes nullability at every compared node including the
// root, so sameLegacyFloatOrigin must reject a candidate that is identical to
// the stored expression except for a forged root Typ.NotNullable.
func TestLegacyFloatOriginRejectsForgedRootNullability(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	compiler := &mock.ctxt
	stmt, err := mysql.ParseOne(t.Context(), "select coalesce(cast(x'61' as char), 'fallback')", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false, nil)
	require.NoError(t, err)
	q := built.GetQuery()
	require.NotNil(t, q)
	var root *planpb.Expr
	for _, n := range q.Nodes {
		if n != nil && len(n.ProjectList) > 0 {
			root = n.ProjectList[0]
		}
	}
	require.NotNil(t, root)
	require.Equal(t, "coalesce", root.GetF().GetFunc().GetObjName())
	require.True(t, root.Typ.NotNullable, "NOTNULL fallback argument keeps coalesce root NOT NULL")
	forged := proto.Clone(root).(*planpb.Expr)
	forged.Typ.NotNullable = false
	require.False(t, sameLegacyFloatOrigin(root, forged), "forged root nullability is not derived from the cast descendant")
}

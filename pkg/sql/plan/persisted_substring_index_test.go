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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPersistedSubstringIndexKeepsLegacyOverload(t *testing.T) {
	proc := testutil.NewProcess(t)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select substring_index('a,b', ',', a)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr

	for _, typ := range []planpb.Type{
		{Id: int32(types.T_decimal64), Width: 18, Scale: 1},
		{Id: int32(types.T_decimal128), Width: 38, Scale: 1},
	} {
		binder := NewGeneratedColBinder(proc.Ctx, []string{"a"}, []planpb.Type{typ})
		projection, err := binder.BindExpr(ast, 0, false)
		require.NoError(t, err)
		_, transientIndex := function.DecodeOverloadID(projection.GetF().GetFunc().GetObj())
		require.GreaterOrEqual(t, transientIndex, int32(3))

		persisted := DeepCopyExpr(projection)
		require.NoError(t, preservePersistedFormatCompatibility(proc.Ctx, persisted))
		count := persisted.GetF().GetArgs()[2]
		require.Equal(t, int32(types.T_float64), count.Typ.Id)
		_, persistedIndex := function.DecodeOverloadID(persisted.GetF().GetFunc().GetObj())
		require.Equal(t, int32(0), persistedIndex)
	}
}

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

type nativePADLegacyUDFContext struct{ *MockCompilerContext }

func (c *nativePADLegacyUDFContext) ResolveUdf(name string, _ []*pb.Expr) (*function.Udf, error) {
	body := "'A' collate utf8mb4_general_ci"
	switch name {
	case "legacy_nested":
		body = "legacy_scalar()"
	case "legacy_query":
		body = "select 'A' collate utf8mb4_general_ci"
	case "legacy_bad":
		body = "("
	}
	return &function.Udf{Body: body, Language: string(tree.SQL)}, nil
}

func TestNativePADLegacySQLUDFBinding(t *testing.T) {
	parent := WithCollationBindingRevision(t.Context(), 1)
	binder := NewDefaultBinder(parent, nil, nil, Type{}, nil)
	for _, body := range []string{"'A' collate utf8mb4_general_ci", "("} {
		expr, err := bindFuncExprImplUdf(&binder.baseBinder, "legacy_scalar", &function.Udf{
			Body: body, Language: string(tree.SQL),
		}, nil, nil, 0)
		if body == "(" {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, uint32(3), expr.Typ.Charset)
			require.Zero(t, expr.Typ.CollationVersion, "unversioned stored SQL must not inherit its caller's generation")
		}
		require.Same(t, parent, binder.GetContext())
	}

	base, _ := nativePADDDLContext(t)
	ctx := &nativePADLegacyUDFContext{MockCompilerContext: base}
	ctx.SetContext(parent)
	stmt, err := parsers.ParseOne(parent, dialect.MYSQL,
		"select legacy_nested(), legacy_query(), 'A' collate utf8mb4_general_ci", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.Same(t, parent, ctx.GetContext())
	query := built.GetQuery()
	outputs := query.Nodes[query.Steps[0]].ProjectList
	require.Len(t, outputs, 3)
	require.Zero(t, outputs[0].Typ.CollationVersion)
	require.Zero(t, outputs[1].Typ.CollationVersion)
	require.Equal(t, uint32(1), outputs[2].Typ.CollationVersion, "the outer binder must regain native explicit binding")
	bad, err := parsers.ParseOne(parent, dialect.MYSQL, "select legacy_bad()", 1)
	require.NoError(t, err)
	defer bad.Free()
	_, err = BuildPlan(ctx, bad, false)
	require.Error(t, err)
	require.Same(t, parent, ctx.GetContext(), "an invalid body must restore the compiler context too")
}

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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type variableStringDomainContext struct {
	MockCompilerContext
	domain types.RuntimeStringDomain
	err    error
}

func (c *variableStringDomainContext) ResolveVariableStringDomain(string, bool, bool) (types.RuntimeStringDomain, error) {
	return c.domain, c.err
}

func TestBindUserVariableCapturesEffectiveStringDomain(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.Type
		domain types.RuntimeStringDomain
	}{
		{"inherit text", types.T_text.ToType(), types.RuntimeStringInherit},
		{"inherit binary", types.T_blob.ToType(), types.RuntimeStringInherit},
		{"binary value on text type", types.T_text.ToType(), types.RuntimeStringBinary},
		{"text value on binary type", types.T_blob.ToType(), types.RuntimeStringText},
		{"text value on binary charset", types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetBinary), types.RuntimeStringText},
		{"keep text collation", types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetUTF8MB4Bin), types.RuntimeStringBinary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assigned := makePlan2Type(&tc.typ)
			ctx := &variableStringDomainContext{MockCompilerContext: NewMockOptimizer(false).ctxt, domain: tc.domain}
			ctx.ResolveVariableTypeFunc = func(string, bool, bool) (Type, error) { return assigned, nil }
			binder := &baseBinder{builder: &QueryBuilder{compCtx: ctx}}
			first, err := binder.baseBindVar(&tree.VarExpr{Name: "s"}, 0, true)
			require.NoError(t, err)
			require.Equal(t, assigned, first.Typ, "the static type and effective row domain are independent")
			require.Equal(t, uint32(tc.domain)+1, first.GetV().BoundStringDomain)
			require.Equal(t, makePlan2Type(&tc.typ), assigned, "binding must not rewrite the assignment")

			ctx.domain = types.RuntimeStringBinary
			if tc.domain == types.RuntimeStringBinary {
				ctx.domain = types.RuntimeStringText
			}
			second, err := binder.baseBindVar(&tree.VarExpr{Name: "s"}, 0, true)
			require.NoError(t, err)
			require.Equal(t, first.Typ, second.Typ)
			require.NotEqual(t, first.GetV().BoundStringDomain, second.GetV().BoundStringDomain)
			require.Equal(t, uint32(tc.domain)+1, first.GetV().BoundStringDomain)

			copy := DeepCopyExpr(first)
			require.NotSame(t, first.GetV(), copy.GetV())
			payload, err := copy.Marshal()
			require.NoError(t, err)
			var restored Expr
			require.NoError(t, restored.Unmarshal(payload))
			require.Equal(t, assigned, restored.Typ)
			require.Equal(t, first.GetV(), restored.GetV())
			copy.GetV().BoundStringDomain = second.GetV().BoundStringDomain
			require.Equal(t, uint32(tc.domain)+1, first.GetV().BoundStringDomain)
		})
	}
}

func TestBindUserVariableStringDomainErrorsAndNonStringControls(t *testing.T) {
	ctx := &variableStringDomainContext{MockCompilerContext: NewMockOptimizer(false).ctxt}
	assigned := makeSimplePlan2Type(types.T_text)
	ctx.ResolveVariableTypeFunc = func(string, bool, bool) (Type, error) { return assigned, nil }
	binder := &baseBinder{builder: &QueryBuilder{compCtx: ctx}}
	ctx.err = moerr.NewInternalErrorNoCtx("variable domain resolver failed")
	_, err := binder.baseBindVar(&tree.VarExpr{Name: "s"}, 0, true)
	require.ErrorIs(t, err, ctx.err)
	ctx.err = nil
	ctx.domain = types.RuntimeStringDomain(255)
	_, err = binder.baseBindVar(&tree.VarExpr{Name: "s"}, 0, true)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	ctx.err = moerr.NewInternalErrorNoCtx("non-string domain lookup must not happen")

	for _, oid := range []types.T{types.T_int64, types.T_bit, types.T_json} {
		assigned = makeSimplePlan2Type(oid)
		expr, err := binder.baseBindVar(&tree.VarExpr{Name: "s"}, 0, true)
		require.NoError(t, err)
		require.Equal(t, assigned, expr.Typ)
		require.Zero(t, expr.GetV().BoundStringDomain)
	}
	expr, err := binder.baseBindVar(&tree.VarExpr{Name: "version", System: true}, 0, true)
	require.NoError(t, err)
	require.Equal(t, makeSimplePlan2Type(types.T_text), expr.Typ)
	require.Zero(t, expr.GetV().BoundStringDomain)

	// Optional contexts still create a frozen INHERIT binding, not a legacy
	// expression whose domain would be read again on every execution.
	binder.builder.compCtx = &ctx.MockCompilerContext
	assigned = makeSimplePlan2Type(types.T_text)
	expr, err = binder.baseBindVar(&tree.VarExpr{Name: "s"}, 0, true)
	require.NoError(t, err)
	require.Equal(t, assigned, expr.Typ)
	require.Equal(t, uint32(1), expr.GetV().BoundStringDomain)
}

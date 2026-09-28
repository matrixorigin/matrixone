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
		name    string
		typ     types.Type
		domain  types.RuntimeStringDomain
		charset uint8
	}{
		{"inherit text", types.T_text.ToType(), types.RuntimeStringInherit, types.CharsetUTF8},
		{"inherit binary", types.T_blob.ToType(), types.RuntimeStringInherit, types.CharsetBinary},
		{"binary value on text type", types.T_text.ToType(), types.RuntimeStringBinary, types.CharsetBinary},
		{"text value on binary type", types.T_blob.ToType(), types.RuntimeStringText, types.CharsetUTF8},
		{"keep text collation", types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetUTF8MB4Bin), types.RuntimeStringText, types.CharsetUTF8MB4Bin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assigned := makePlan2Type(&tc.typ)
			ctx := &variableStringDomainContext{MockCompilerContext: NewMockOptimizer(false).ctxt, domain: tc.domain}
			ctx.ResolveVariableTypeFunc = func(string, bool, bool) (Type, error) { return assigned, nil }
			binder := &baseBinder{builder: &QueryBuilder{compCtx: ctx}}
			first, err := binder.baseBindVar(&tree.VarExpr{Name: "s"}, 0, true)
			require.NoError(t, err)
			require.Equal(t, uint32(tc.charset), first.Typ.Charset)
			require.Equal(t, assigned.Id, first.Typ.Id)
			require.Equal(t, assigned.Width, first.Typ.Width)
			require.Equal(t, makePlan2Type(&tc.typ), assigned, "binding must not rewrite the assignment")

			// A new statement binds the new value, without changing the old plan.
			ctx.domain = types.RuntimeStringBinary
			if tc.charset == types.CharsetBinary {
				ctx.domain = types.RuntimeStringText
			}
			second, err := binder.baseBindVar(&tree.VarExpr{Name: "s"}, 0, true)
			require.NoError(t, err)
			require.NotEqual(t, first.Typ.Charset, second.Typ.Charset)
			require.Equal(t, uint32(tc.charset), first.Typ.Charset)
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
		require.NoError(t, err, "non-string variables must not use the string-domain resolver")
		require.Equal(t, assigned, expr.Typ)
	}
	expr, err := binder.baseBindVar(&tree.VarExpr{Name: "version", System: true}, 0, true)
	require.NoError(t, err)
	require.Equal(t, makeSimplePlan2Type(types.T_text), expr.Typ)
}

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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func newStringNumValFn(s string) *tree.FuncExpr {
	nv := tree.NewNumVal[string](s, s, false, tree.P_char)
	return &tree.FuncExpr{Exprs: tree.Exprs{nv}}
}

func newNonNumValFn() *tree.FuncExpr {
	// UnresolvedName is not a NumVal — triggers the error branch.
	un := tree.NewUnresolvedName(tree.NewCStr("col", 0))
	return &tree.FuncExpr{Exprs: tree.Exprs{un}}
}

func TestGetCagraParams_OK(t *testing.T) {
	var b *QueryBuilder // GetContext on nil QueryBuilder returns context.TODO()
	out, err := b.getCagraParams(newStringNumValFn(`{"m":"32"}`))
	require.NoError(t, err)
	require.Equal(t, `{"m":"32"}`, out)
}

func TestGetCagraParams_Error(t *testing.T) {
	var b *QueryBuilder
	_, err := b.getCagraParams(newNonNumValFn())
	require.Error(t, err)
}

func TestGetIvfpqParams_OK(t *testing.T) {
	var b *QueryBuilder
	out, err := b.getIvfpqParams(newStringNumValFn(`{"lists":"4"}`))
	require.NoError(t, err)
	require.Equal(t, `{"lists":"4"}`, out)
}

func TestGetIvfpqParams_Error(t *testing.T) {
	var b *QueryBuilder
	_, err := b.getIvfpqParams(newNonNumValFn())
	require.Error(t, err)
}

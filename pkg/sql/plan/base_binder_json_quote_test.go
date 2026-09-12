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
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestJsonQuotePreparedParameterMetadataDoesNotCastRuntimeValue(t *testing.T) {
	prepared, err := runOneStmt(
		NewMockOptimizer(false), t,
		"prepare json_quote_stmt from 'select json_quote(?) as result'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().GetPlan()
	quoted := findPlanFunctionExpr(preparePlan, "json_quote")
	require.NotNil(t, quoted)
	require.Equal(t, int32(types.T_text), quoted.Typ.Id)
	require.Equal(t, int32(393200), quoted.Typ.Width)
	require.NotNil(t, quoted.GetF().Args[0].GetP(),
		"metadata inference must preserve the direct parameter marker")

	value := strings.Repeat("a", types.MaxVarcharLen/utf8.UTFMax+1)
	filled, err := FillValuesOfParamsInPlan(context.Background(), preparePlan, []any{value})
	require.NoError(t, err)
	quoted = findPlanFunctionExpr(filled, "json_quote")
	require.NotNil(t, quoted)
	require.Equal(t, int32(types.T_text), quoted.Typ.Id)
	require.Equal(t, int32(types.MaxLongTextLen), quoted.Typ.Width)
	require.Equal(t, value, quoted.GetF().Args[0].GetLit().GetSval())
}

func TestJsonQuoteStaticNullHasZeroCharacterBound(t *testing.T) {
	query, err := runOneStmt(NewMockOptimizer(false), t, "select json_quote(null) as result")
	require.NoError(t, err)
	quoted := findPlanFunctionExpr(query, "json_quote")
	require.NotNil(t, quoted)
	require.Equal(t, int32(types.T_varchar), quoted.Typ.Id)
	require.Equal(t, int32(2), quoted.Typ.Width)
	require.Equal(t, uint32(types.CharsetUTF8MB4Bin), quoted.Typ.Charset)
	require.True(t, quoted.GetF().Args[0].GetLit().GetIsnull())
}

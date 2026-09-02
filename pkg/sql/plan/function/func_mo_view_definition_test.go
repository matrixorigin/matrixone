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

package function

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestViewDefinitionFunctionRegistration(t *testing.T) {
	registered := allSupportedFunctions[MO_VIEW_DEFINITION]
	require.Equal(t, MO_VIEW_DEFINITION, registered.functionId)
	require.Len(t, registered.Overloads, 1)
	require.Equal(t, types.T_text, registered.Overloads[0].retType(nil).Oid)
	require.NotNil(t, registered.Overloads[0].newOp())

	function, err := GetFunctionByName(
		context.Background(), "mo_view_definition", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	functionID, _ := DecodeOverloadID(function.GetEncodedOverloadID())
	require.Equal(t, int32(MO_VIEW_DEFINITION), functionID)
	require.Equal(t, types.T_text, function.GetReturnType().Oid)
}

func TestViewDefinitionFromPersistedData(t *testing.T) {
	tests := []struct {
		name      string
		persisted string
		want      string
		ok        bool
	}{
		{
			name:      "current frozen definition is returned unchanged",
			persisted: `{"Stmt":"create view v as select 0","definition":"select ` + "`frozen`" + ` from ` + "`t`" + `"}`,
			want:      "select `frozen` from `t`",
			ok:        true,
		},
		{
			name:      "legacy block comment before view is structurally opaque",
			persisted: `{"Stmt":"create /* migration view fake as */ view v as select 1"}`,
			want:      "select 1",
			ok:        true,
		},
		{
			name:      "legacy executable wrapper preserves quoted terminator",
			persisted: `{"Stmt":"/*!50001 CREATE VIEW v AS SELECT 'x*/y' AS s */;"}`,
			want:      "x*/y",
			ok:        true,
		},
		{
			name:      "legacy quoted definer cannot supply view boundary",
			persisted: `{"Stmt":"CREATE DEFINER=' view fake as select 0'@'%' VIEW v AS SELECT 1"}`,
			want:      "select 1",
			ok:        true,
		},
		{
			name:      "legacy check option is outside definition",
			persisted: `{"Stmt":"CREATE VIEW v AS SELECT 1 WITH CASCADED CHECK OPTION"}`,
			want:      "select 1",
			ok:        true,
		},
		{
			name:      "legacy saved parser options are honored",
			persisted: `{"Stmt":"CREATE VIEW v AS SELECT 1","sql_mode":"PIPES_AS_CONCAT","lower_case_table_names":1}`,
			want:      "select 1",
			ok:        true,
		},
		{
			name:      "malformed JSON remains null",
			persisted: `{`,
		},
		{
			name:      "missing statement remains null",
			persisted: `{}`,
		},
		{
			name:      "malformed persisted row remains null",
			persisted: `{"Stmt":"CREATE VIEW"}`,
		},
		{
			name:      "non view statement remains null",
			persisted: `{"Stmt":"SELECT 1"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition, ok := viewDefinitionFromPersistedData(context.Background(), test.persisted)
			require.Equal(t, test.ok, ok)
			if !ok {
				require.Empty(t, definition)
				return
			}
			require.Contains(t, strings.ToLower(definition), test.want)
			require.NotContains(t, strings.ToLower(definition), "create view")
			require.NotContains(t, strings.ToLower(definition), "check option")
		})
	}
}

func TestBuiltInViewDefinition(t *testing.T) {
	proc := testutil.NewProcess(t)
	current := `{"Stmt":"create view v as select 0","definition":"select frozen from t"}`
	legacy := `{"Stmt":"create /* migration view fake as */ view v as select 1"}`

	t.Run("evaluates valid and invalid persisted rows", func(t *testing.T) {
		input := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendStringList(input,
			[]string{current, legacy, `{"Stmt":"CREATE VIEW"}`},
			[]bool{false, false, false}, proc.Mp()))
		result := vector.NewFunctionResultWrapper(types.T_text.ToType(), proc.Mp())
		require.NoError(t, result.PreExtendAndReset(input.Length()))
		require.NoError(t, builtInViewDefinition(
			[]*vector.Vector{input}, result, proc, input.Length(), nil))

		values := vector.GenerateFunctionStrParameter(result.GetResultVector())
		value, isNull := values.GetStrValue(0)
		require.False(t, isNull)
		require.Equal(t, "select frozen from t", string(value))
		value, isNull = values.GetStrValue(1)
		require.False(t, isNull)
		require.Equal(t, "select 1", strings.ToLower(string(value)))
		_, isNull = values.GetStrValue(2)
		require.True(t, isNull)
	})

	t.Run("preserves null inputs and selection mask", func(t *testing.T) {
		input := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendStringList(input,
			[]string{current, legacy}, []bool{false, false}, proc.Mp()))
		result := vector.NewFunctionResultWrapper(types.T_text.ToType(), proc.Mp())
		require.NoError(t, result.PreExtendAndReset(input.Length()))
		require.NoError(t, builtInViewDefinition([]*vector.Vector{input}, result,
			proc, input.Length(), &FunctionSelectList{
				AnyNull:    true,
				SelectList: []bool{true, false},
			}))

		values := vector.GenerateFunctionStrParameter(result.GetResultVector())
		value, isNull := values.GetStrValue(0)
		require.False(t, isNull)
		require.Equal(t, "select frozen from t", string(value))
		_, isNull = values.GetStrValue(1)
		require.True(t, isNull)

		nullInput := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
		nullResult := vector.NewFunctionResultWrapper(types.T_text.ToType(), proc.Mp())
		require.NoError(t, nullResult.PreExtendAndReset(nullInput.Length()))
		require.NoError(t, builtInViewDefinition(
			[]*vector.Vector{nullInput}, nullResult, proc, nullInput.Length(), nil))
		_, isNull = vector.GenerateFunctionStrParameter(
			nullResult.GetResultVector()).GetStrValue(0)
		require.True(t, isNull)
	})
}

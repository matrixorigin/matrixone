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
)

func TestViewDefinitionFunctionRegistration(t *testing.T) {
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
			name:      "malformed persisted row remains null",
			persisted: `{"Stmt":"CREATE VIEW"}`,
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

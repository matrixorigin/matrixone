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

package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestUseDatabaseCatalogNamePreservesCreatedName(t *testing.T) {
	for _, tc := range []struct {
		mode int64
		want string
	}{
		{mode: 0, want: "QaMode2Base"},
		{mode: 1, want: "qamode2base"},
		{mode: 2, want: "QaMode2Base"},
	} {
		stmt, err := mysql.ParseOne(context.Background(), "USE `QaMode2Base`", tc.mode)
		require.NoError(t, err)
		ses := &Session{feSessionImpl: feSessionImpl{
			sesSysVars: &SystemVariables{mp: map[string]interface{}{"lower_case_table_names": tc.mode}},
		}}
		ctx := defines.AttachMode2NameResolution(context.Background(), tc.mode == 2)
		require.Equal(t, tc.want, useDatabaseCatalogName(ctx, stmt.(*tree.Use), ses))
	}
}

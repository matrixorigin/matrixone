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

package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedDatabaseMigrationWireRoundTrip(t *testing.T) {
	for _, database := range []string{"a", "", "db_中文`name"} {
		t.Run(database, func(t *testing.T) {
			original := &MigrateConnToRequest{
				ConnID: 17, DB: "current_b",
				PrepareStmts: []*PrepareStmt{{
					Name: "p", SQL: "create table snap as select * from source",
					Database: database, ParamTypes: []byte{1, 2},
				}},
			}
			wire, err := original.Marshal()
			require.NoError(t, err)
			require.Len(t, wire, original.ProtoSize())
			var decoded MigrateConnToRequest
			require.NoError(t, decoded.Unmarshal(wire))
			require.Equal(t, original, &decoded)
			require.Equal(t, database, decoded.PrepareStmts[0].GetDatabase(), "空库不能回退为连接当前库")
			if database != "" {
				binding, err := original.PrepareStmts[0].Marshal()
				require.NoError(t, err)
				var truncated PrepareStmt
				require.Error(t, truncated.Unmarshal(binding[:len(binding)-1]), "截断绑定不能静默退回当前库")
			}
		})
	}
}

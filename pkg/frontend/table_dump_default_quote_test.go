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

package frontend

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// These SQL bytes pin the pre-backport 4.2 policy, not main's newer schema
// hash or charset metadata. Changing executable SHOW CREATE must not invalidate
// archives already produced by 4.2 DUMP TABLE.
func TestTableDumpDefaultHashesPreserve42(t *testing.T) {
	for _, tc := range []struct{ name, origin, legacy string }{
		{"apostrophe", "'it''s'", "'it''''s'"},
		{"backtick", "'x`y'", "'x``y'"},
		{"empty", "''", "''"},
		{"literal_null", "'NULL'", "'NULL'"},
		{"sql_null", "null", "NULL"},
		{"backslash", `'a\\b'`, `'a\\b'`},
		{"expression", " (concat('a','b')) ", "(concat('a','b'))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := &plan.TableDef{Name: "source", Cols: []*plan.ColDef{{
				Name: "a", Typ: plan.Type{Id: int32(types.T_varchar), Width: 32},
				Default: &plan.Default{NullAbility: true, OriginString: tc.origin},
			}}}
			before := proto.Clone(def)
			got, err := tableSchemaHash(def)
			require.NoError(t, err)
			legacySQL := "CREATE TABLE `__table_dump_target__` (\n  `a` varchar(32) DEFAULT " + tc.legacy + "\n)"
			require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(legacySQL))), got)
			require.True(t, proto.Equal(before, def), "hashing must not mutate catalog metadata")
			def.Cols[0].Default.OriginString = "'different'"
			changed, err := tableSchemaHash(def)
			require.NoError(t, err)
			require.NotEqual(t, got, changed, "a real default change must remain incompatible")
		})
	}
}

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

package versions

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

// The shared COLUMNS upgrade entry converges the pre-change identity of the
// tinyint(1) revision (issue #31749) to the current definition exactly once:
// a tenant whose live view still holds the pre-change identity is not ready
// (the CREATE OR REPLACE authoring of the current definition runs), while
// the current identity - in both its initialized and its persisted
// authoring form - reads ready, so the second check is a no-op.
func TestInformationSchemaColumnsUpgradeEntryConvergesPreBoolIdentity(t *testing.T) {
	entry := InformationSchemaColumnsUpgradeEntry()
	require.Equal(t, sysview.InformationDBConst, entry.Schema)
	require.Equal(t, "COLUMNS", entry.TableName)
	require.EqualValues(t, defines.MORPCVersion109, entry.RequiredProtocolVersion)
	require.Equal(t, strings.Replace(sysview.InformationSchemaColumnsDDL,
		"CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1), entry.UpgSql)

	for _, tc := range []struct {
		name   string
		stored string
		ready  bool
	}{
		{"pre-change tinyint(1) identity upgrades", sysview.InformationSchemaColumnsPreBoolTinyIntOneDDL(), false},
		{"current identity after the replace is a no-op", sysview.InformationSchemaColumnsDDL, true},
		{"persisted authoring form of the current identity", strings.Replace(sysview.InformationSchemaColumnsDDL,
			"CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := CheckViewDefinition
			CheckViewDefinition = func(_ executor.TxnExecutor, _ uint32, schema, name string) (bool, string, error) {
				require.Equal(t, sysview.InformationDBConst, schema)
				return name == "columns", tc.stored, nil
			}
			t.Cleanup(func() { CheckViewDefinition = old })
			ready, err := entry.CheckFunc(nil, 7)
			require.NoError(t, err)
			if tc.ready {
				require.True(t, ready)
			} else {
				require.False(t, ready)
			}
		})
	}
}

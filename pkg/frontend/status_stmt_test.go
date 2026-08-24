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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestCapturePersistentDropTableTargets(t *testing.T) {
	ses := &Session{
		tempTables:    make(map[string]string),
		tempTablesRev: make(map[string]string),
	}
	ses.AddTempTable("db1", "shadowed", "__mo_temp_shadowed")

	prefix := tree.ObjectNamePrefix{SchemaName: tree.Identifier("db1"), ExplicitSchema: true}
	shadowed := tree.NewTableName(tree.Identifier("shadowed"), prefix, nil)
	persistent := tree.NewTableName(tree.Identifier("persistent"), prefix, nil)

	t.Run("ordinary drop resolves shadowed target as temporary", func(t *testing.T) {
		st := &tree.DropTable{Names: tree.TableNames{shadowed}}
		require.Empty(t, capturePersistentDropTableTargets(ses, st))
	})

	t.Run("explicit temporary drop has no persistent targets", func(t *testing.T) {
		st := &tree.DropTable{Temporary: true, Names: tree.TableNames{shadowed}}
		require.Empty(t, capturePersistentDropTableTargets(ses, st))
	})

	t.Run("mixed drop keeps only permanent targets", func(t *testing.T) {
		st := &tree.DropTable{Names: tree.TableNames{shadowed, persistent}}
		targets := capturePersistentDropTableTargets(ses, st)
		require.Equal(t, tree.TableNames{persistent}, targets)

		// The classification is captured before execution and remains valid after
		// dropTableSingle removes the temporary alias.
		ses.RemoveTempTable("db1", "shadowed")
		require.Equal(t, tree.TableNames{persistent}, targets)
	})
}

func TestExecCtxCloseClearsPersistentDropTableTargets(t *testing.T) {
	execCtx := &ExecCtx{
		persistentDropTableTargets: tree.TableNames{
			tree.NewTableName(tree.Identifier("t"), tree.ObjectNamePrefix{}, nil),
		},
	}
	execCtx.Close()
	require.Nil(t, execCtx.persistentDropTableTargets)
}

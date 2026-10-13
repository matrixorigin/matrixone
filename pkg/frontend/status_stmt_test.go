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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestCapturePersistentDropTableTargets(t *testing.T) {
	ses := &Session{
		tempTables:    make(map[string]string),
		tempTablesRev: make(map[string]string),
	}
	ses.AddTempTable("db1", "shadowed", "__mo_temp_shadowed")
	collect := func(st *tree.DropTable, db string) tree.TableNames {
		targets, err := capturePersistentDropTableTargets(context.Background(), ses, st, db)
		require.NoError(t, err)
		return targets
	}

	prefix := tree.ObjectNamePrefix{SchemaName: tree.Identifier("db1"), ExplicitSchema: true}
	shadowed := tree.NewTableName(tree.Identifier("shadowed"), prefix, nil)
	persistent := tree.NewTableName(tree.Identifier("persistent"), prefix, nil)

	t.Run("ordinary drop resolves shadowed target as temporary", func(t *testing.T) {
		st := &tree.DropTable{Names: tree.TableNames{shadowed}}
		require.Empty(t, collect(st, ""))
	})

	t.Run("explicit temporary drop has no persistent targets", func(t *testing.T) {
		st := &tree.DropTable{Temporary: true, Names: tree.TableNames{shadowed}}
		require.Empty(t, collect(st, ""))
	})

	t.Run("prepared default database overrides execute-time temporary alias", func(t *testing.T) {
		unqualified := tree.NewTableName(tree.Identifier("shadowed"), tree.ObjectNamePrefix{}, nil)
		st := &tree.DropTable{Names: tree.TableNames{unqualified}}
		require.Equal(t, tree.TableNames{unqualified}, collect(st, "db2"))
	})

	t.Run("mixed drop keeps only permanent targets", func(t *testing.T) {
		st := &tree.DropTable{Names: tree.TableNames{shadowed, persistent}}
		targets := collect(st, "")
		require.Equal(t, tree.TableNames{persistent}, targets)

		// The classification is captured before execution and remains valid after
		// dropTableSingle removes the temporary alias.
		ses.RemoveTempTable("db1", "shadowed")
		require.Equal(t, tree.TableNames{persistent}, targets)
	})
}

func TestMode2PersistentDropTargetUsesCapturedTemporaryAlias(t *testing.T) {
	ses := &Session{
		tempTables:          make(map[string]string),
		tempTablesRev:       make(map[string]string),
		tempTableIdentities: make(map[string]tempTableIdentity),
	}
	ses.AddTempTable("QaDB", "TempMix", "physical_one")
	alias := tree.NewTableName(tree.Identifier("tempmix"), tree.ObjectNamePrefix{
		SchemaName: tree.Identifier("qadb"), ExplicitSchema: true,
	}, nil)
	stmt := &tree.DropTable{Names: tree.TableNames{alias}}
	mode2 := defines.AttachMode2NameResolution(context.Background(), true)
	targets, err := capturePersistentDropTableTargets(mode2, ses, stmt, "")
	require.NoError(t, err)
	require.Empty(t, targets)
	objectLifecycle, err := requiresPessimisticObjectLifecycleTxn(mode2, ses, stmt, "")
	require.NoError(t, err)
	require.False(t, objectLifecycle)
	alterLifecycle, err := requiresPessimisticLifecycleModeTxn(mode2, ses, &tree.AlterTable{Table: alias}, "")
	require.NoError(t, err)
	require.False(t, alterLifecycle)
	legacy := defines.AttachMode2NameResolution(context.Background(), false)
	targets, err = capturePersistentDropTableTargets(legacy, ses, stmt, "")
	require.NoError(t, err)
	require.Equal(t, tree.TableNames{alias}, targets)

	ses.AddTempTable("QaDB", "TEMPMIX", "physical_two")
	_, err = capturePersistentDropTableTargets(mode2, ses, stmt, "")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrAmbiguousIdentifier))
	_, err = requiresPessimisticObjectLifecycleTxn(mode2, ses, stmt, "")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrAmbiguousIdentifier))
	_, err = requiresPessimisticLifecycleModeTxn(mode2, ses, &tree.AlterTable{Table: alias}, "")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrAmbiguousIdentifier))
}

func TestExecCtxCloseClearsPersistentDropTableTargets(t *testing.T) {
	execCtx := &ExecCtx{
		effectiveTxnDefaultDatabase: "prepare_db",
		persistentDropTableTargets: tree.TableNames{
			tree.NewTableName(tree.Identifier("t"), tree.ObjectNamePrefix{}, nil),
		},
	}
	execCtx.Close()
	require.Empty(t, execCtx.effectiveTxnDefaultDatabase)
	require.Nil(t, execCtx.persistentDropTableTargets)
}

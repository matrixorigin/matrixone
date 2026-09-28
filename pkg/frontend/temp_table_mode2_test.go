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
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestResolveMode2TemporaryTableAliasAndLegacyCollision(t *testing.T) {
	ses := &Session{
		tempTables:          make(map[string]string),
		tempTablesRev:       make(map[string]string),
		tempTableIdentities: make(map[string]tempTableIdentity),
		feSessionImpl: feSessionImpl{sesSysVars: &SystemVariables{mp: map[string]interface{}{
			"lower_case_table_names": int64(0),
		}}},
	}
	ses.AddTempTable("QaDB", "MixT", "physical_one")
	ses.AddTempTable("QaDB", "mixt", "physical_two")
	ses.sesSysVars.Set("lower_case_table_names", int64(2))
	legacy := defines.AttachMode2NameResolution(context.Background(), false)
	_, found, err := ses.ResolveTempTable(legacy, "QaDB", "MIXT")
	require.NoError(t, err)
	require.False(t, found, "the statement's captured pre-upgrade gate keeps exact lookup")
	mode2 := defines.AttachMode2NameResolution(context.Background(), true)
	_, _, err = ses.ResolveTempTable(mode2, "QaDB", "MIXT")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrAmbiguousIdentifier))
	ses.RemoveTempTable("QaDB", "mixt")
	physical, found, err := ses.ResolveTempTable(mode2, "QaDB", "mixt")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "physical_one", physical)
	physical, found, err = ses.ResolveTempTable(mode2, "qadb", "MIXT")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "physical_one", physical)
	ses.RemoveTempTable("QaDB", "MixT")
	_, found, err = ses.ResolveTempTable(mode2, "QaDB", "mixt")
	require.NoError(t, err)
	require.False(t, found)
}

func TestMode2TemporaryAliasIndexFollowsPublishAndRollback(t *testing.T) {
	ses := &Session{
		tempTables:          make(map[string]string),
		tempTablesRev:       make(map[string]string),
		tempTableIdentities: make(map[string]tempTableIdentity),
	}
	mode2 := defines.AttachMode2NameResolution(context.Background(), true)
	ses.PublishTemporaryTable("QaDB", "MixT", "physical_one")
	physical, found, err := ses.ResolveTempTable(mode2, "qadb", "mixt")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "physical_one", physical)

	ses.addTempTable("QaDB", "Other", "physical_two", "txn", "stmt")
	ses.rollbackTempTableStatement("txn", "stmt")
	_, found, err = ses.ResolveTempTable(mode2, "qadb", "other")
	require.NoError(t, err)
	require.False(t, found)

	ses.PublishTemporaryTable("QaDB", "MixT", "physical_replacement")
	physical, found, err = ses.ResolveTempTable(mode2, "QADB", "MIXT")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "physical_replacement", physical)
	ses.RetireTemporaryTable("QaDB", "MixT", "physical_replacement", nil)
	_, found, err = ses.ResolveTempTable(mode2, "qadb", "mixt")
	require.NoError(t, err)
	require.False(t, found)
}

func TestMode2TemporaryAliasConcurrentMutationAndResolution(t *testing.T) {
	ses := &Session{
		tempTables:          make(map[string]string),
		tempTablesRev:       make(map[string]string),
		tempTableIdentities: make(map[string]tempTableIdentity),
	}
	mode2 := defines.AttachMode2NameResolution(context.Background(), true)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 500; i++ {
			ses.AddTempTable("QaDB", "MixT", "physical_one")
			ses.RemoveTempTable("QaDB", "MixT")
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 500; i++ {
			physical, found, err := ses.ResolveTempTable(mode2, "qadb", "mixt")
			if err != nil || (found && physical != "physical_one") {
				t.Errorf("inconsistent concurrent alias lookup: physical=%q found=%v err=%v", physical, found, err)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
}

func TestInstallMigratedIdentifierModeKeepsGlobalValue(t *testing.T) {
	globals := &SystemVariables{mp: map[string]interface{}{"lower_case_table_names": int64(1)}}
	ses := &Session{feSessionImpl: feSessionImpl{gSysVars: globals, sesSysVars: globals}}
	require.NoError(t, ses.installMigratedIdentifierMode(2))
	value, err := ses.GetSessionSysVar("lower_case_table_names")
	require.NoError(t, err)
	require.Equal(t, int64(2), value)
	require.Equal(t, int64(1), globals.Get("lower_case_table_names"))
	require.Error(t, ses.installMigratedIdentifierMode(3))
}

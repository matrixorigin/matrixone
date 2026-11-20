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

package catalog

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestTemporaryIndexTableNameClassification(t *testing.T) {
	sessionID := uuid.MustParse("018f1f76-7b9d-7f35-b2d9-9b8d7774bde8")
	indexID := "0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd"

	uniqueName := UniqueIndexTableNamePrefix + indexID
	secondaryName := SecondaryIndexTableNamePrefix + indexID
	realUniqueName := defines.GenTempTableName(sessionID, "db_with_underscores", uniqueName)
	realSecondaryName := defines.GenTempTableName(sessionID, "db_with_underscores", secondaryName)

	require.True(t, IsUniqueIndexTable(realUniqueName))
	require.False(t, IsSecondaryIndexTable(realUniqueName))
	require.True(t, IsSecondaryIndexTable(realSecondaryName))
	require.False(t, IsUniqueIndexTable(realSecondaryName))

	// An internal-looking database name must not turn an ordinary temporary
	// table into an index table.
	ordinaryName := defines.GenTempTableName(sessionID, uniqueName, "ordinary_table")
	require.False(t, IsUniqueIndexTable(ordinaryName))
	require.False(t, IsSecondaryIndexTable(ordinaryName))
}

func TestMoTablesLogicalIDIndexPrimaryColumnType(t *testing.T) {
	defs := GenMoTablesLogicalIdIndexTableDefs()
	sourceType := MoTablesTypes[MO_TABLES_CPKEY_IDX]

	for _, def := range defs {
		attrDef, ok := def.(*engine.AttributeDef)
		if ok && attrDef.Attr.Name == IndexTablePrimaryColName {
			require.Equal(t, sourceType, attrDef.Attr.Type)
			return
		}
	}

	t.Fatal("logical ID index primary column definition not found")
}

func TestBuildMoDatabaseBatchQuery(t *testing.T) {
	require.Equal(t, MoDatabaseBatchQuery, BuildMoDatabaseBatchQuery(nil))
	require.Equal(
		t,
		MoDatabaseBatchQuery+" and "+SystemDBAttr_AccID+" in (0,10)",
		BuildMoDatabaseBatchQuery([]uint32{0, 10}),
	)
}

func TestBuildMoTablesBatchQuery(t *testing.T) {
	require.Equal(t, MoTablesBatchQuery, BuildMoTablesBatchQuery(nil))
	require.Equal(
		t,
		MoTablesBatchQuery+" and "+SystemRelAttr_AccID+" in (1)",
		BuildMoTablesBatchQuery([]uint32{1}),
	)
}

func TestBuildMoColumnsBatchQuery(t *testing.T) {
	require.Equal(t, MoColumnsBatchQuery, BuildMoColumnsBatchQuery(nil))
	require.Equal(
		t,
		strings.Replace(MoColumnsBatchQuery, " order by ", " and "+SystemColAttr_AccID+" in (1,2) order by ", 1),
		BuildMoColumnsBatchQuery([]uint32{1, 2}),
	)
}

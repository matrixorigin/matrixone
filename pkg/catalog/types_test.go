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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
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

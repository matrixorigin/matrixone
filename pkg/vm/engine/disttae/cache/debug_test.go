// Copyright 2024 Matrix Origin
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

package cache

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestDebugAccountContents(t *testing.T) {
	cc := NewCatalog()
	ts := timestamp.Timestamp{PhysicalTime: 100}

	cc.databases.data.Set(&DatabaseItem{
		AccountId: 1,
		Name:      "db1",
		Ts:        ts,
		Id:        10,
		Typ:       "USER",
	})
	cc.databases.data.Set(&DatabaseItem{
		AccountId: 1,
		Name:      "db2",
		Ts:        ts,
		Id:        20,
		Typ:       "USER",
	})
	cc.databases.data.Set(&DatabaseItem{
		AccountId: 2,
		Name:      "dbx",
		Ts:        ts,
		Id:        30,
		Typ:       "USER",
	})

	cc.tables.data.Set(&TableItem{
		AccountId:    1,
		DatabaseId:   10,
		Name:         "t1",
		Ts:           ts,
		Id:           11,
		DatabaseName: "db1",
		Kind:         "TABLE",
		Defs:         make([]engine.TableDef, 2),
		LogicalId:    1011,
	})
	cc.tables.data.Set(&TableItem{
		AccountId:    1,
		DatabaseId:   10,
		Name:         "t2",
		Ts:           ts,
		Id:           12,
		DatabaseName: "db1",
		Kind:         "TABLE",
		Defs:         make([]engine.TableDef, 3),
		LogicalId:    1012,
	})
	cc.tables.data.Set(&TableItem{
		AccountId:    1,
		DatabaseId:   20,
		Name:         "t3",
		Ts:           ts,
		Id:           21,
		DatabaseName: "db2",
		Kind:         "VIEW",
		Defs:         make([]engine.TableDef, 1),
		LogicalId:    2021,
	})
	cc.tables.data.Set(&TableItem{
		AccountId:    2,
		DatabaseId:   30,
		Name:         "other",
		Ts:           ts,
		Id:           31,
		DatabaseName: "dbx",
		Kind:         "TABLE",
	})

	got := cc.DebugAccountContents(1, ts, "db1", 10)
	require.Equal(t, ts, got.SnapshotTS)
	require.Equal(t, 1, got.VisibleDatabaseCount)
	require.Equal(t, 2, got.VisibleTableCount)
	require.Equal(t, 1, got.ReturnedDatabaseCount)
	require.Equal(t, 2, got.ReturnedTableCount)
	require.Len(t, got.Databases, 1)
	require.Equal(t, uint64(10), got.Databases[0].ID)
	require.Equal(t, "db1", got.Databases[0].Name)
	require.Equal(t, 2, got.Databases[0].TableCount)
	require.Len(t, got.Tables, 2)
	require.Equal(t, "db1", got.Tables[0].DatabaseName)
	require.Equal(t, 2, got.Tables[0].DefinitionCount)
	require.Equal(t, uint64(1011), got.Tables[0].LogicalID)

	got = cc.DebugAccountContents(1, ts, "20", 1)
	require.Equal(t, 1, got.VisibleDatabaseCount)
	require.Equal(t, 1, got.VisibleTableCount)
	require.Equal(t, 1, got.ReturnedDatabaseCount)
	require.Equal(t, 1, got.ReturnedTableCount)
	require.Equal(t, "db2", got.Databases[0].Name)
	require.Equal(t, "t3", got.Tables[0].Name)
	require.Equal(t, "VIEW", got.Tables[0].Kind)

	empty := cc.DebugAccountContents(99, ts, "", 10)
	require.NotNil(t, empty.Databases)
	require.NotNil(t, empty.Tables)
	require.Empty(t, empty.Databases)
	require.Empty(t, empty.Tables)
}

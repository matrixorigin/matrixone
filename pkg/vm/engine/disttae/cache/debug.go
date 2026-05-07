// Copyright 2021 - 2023 Matrix Origin
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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type DebugCatalogSummary struct {
	Start          timestamp.Timestamp `json:"start"`
	End            timestamp.Timestamp `json:"end"`
	DatabaseItems  int                 `json:"database_items"`
	TableItems     int                 `json:"table_items"`
	CanServeLatest bool                `json:"can_serve_latest"`
}

type DebugCatalogDatabaseItem struct {
	ID         uint64 `json:"id"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	TableCount int    `json:"table_count"`
}

type DebugCatalogTableItem struct {
	ID              uint64 `json:"id"`
	Name            string `json:"name"`
	DatabaseID      uint64 `json:"database_id"`
	DatabaseName    string `json:"database_name"`
	Kind            string `json:"kind"`
	DefinitionCount int    `json:"definition_count"`
	ColumnCount     int    `json:"column_count"`
	CreateSQL       string `json:"create_sql,omitempty"`
	LogicalID       uint64 `json:"logical_id"`
}

type DebugCatalogAccountContents struct {
	SnapshotTS            timestamp.Timestamp        `json:"snapshot_ts"`
	VisibleDatabaseCount  int                        `json:"visible_database_count"`
	ReturnedDatabaseCount int                        `json:"returned_database_count"`
	VisibleTableCount     int                        `json:"visible_table_count"`
	ReturnedTableCount    int                        `json:"returned_table_count"`
	Databases             []DebugCatalogDatabaseItem `json:"databases"`
	Tables                []DebugCatalogTableItem    `json:"tables"`
}

func (cc *CatalogCache) DebugSummary() DebugCatalogSummary {
	if cc == nil {
		return DebugCatalogSummary{}
	}

	cc.mu.Lock()
	start, end := cc.mu.start, cc.mu.end
	cc.mu.Unlock()

	return DebugCatalogSummary{
		Start:          start.ToTimestamp(),
		End:            end.ToTimestamp(),
		DatabaseItems:  cc.databases.data.Len(),
		TableItems:     cc.tables.data.Len(),
		CanServeLatest: cc.CanServe(end),
	}
}

func (cc *CatalogCache) DebugAccountContents(
	accountID uint32,
	ts timestamp.Timestamp,
	dbFilter string,
	limit int,
) DebugCatalogAccountContents {
	contents := DebugCatalogAccountContents{
		SnapshotTS: ts,
		Databases:  make([]DebugCatalogDatabaseItem, 0),
		Tables:     make([]DebugCatalogTableItem, 0),
	}
	if cc == nil {
		return contents
	}
	if limit <= 0 {
		limit = 100
	}

	key := &DatabaseItem{
		AccountId: accountID,
	}
	seen := make(map[string]struct{})
	cc.databases.data.Ascend(key, func(item *DatabaseItem) bool {
		if item.AccountId != accountID {
			return false
		}
		if item.Ts.Greater(ts) {
			return true
		}
		if _, ok := seen[item.Name]; ok {
			return true
		}
		seen[item.Name] = struct{}{}
		if item.deleted || !matchesDebugDatabaseFilter(dbFilter, item) {
			return true
		}

		tableNames, tableIDs := cc.Tables(accountID, item.Id, ts)
		contents.VisibleDatabaseCount++
		contents.VisibleTableCount += len(tableNames)

		if len(contents.Databases) < limit {
			contents.Databases = append(contents.Databases, DebugCatalogDatabaseItem{
				ID:         item.Id,
				Name:       item.Name,
				Type:       item.Typ,
				TableCount: len(tableNames),
			})
		}

		for i, tableName := range tableNames {
			if len(contents.Tables) >= limit {
				continue
			}

			var tableID uint64
			if i < len(tableIDs) {
				tableID = tableIDs[i]
			}
			tableItem := cc.GetTableByIdAndTime(accountID, item.Id, tableID, ts)
			debugItem := DebugCatalogTableItem{
				ID:           tableID,
				Name:         tableName,
				DatabaseID:   item.Id,
				DatabaseName: item.Name,
			}
			if tableItem != nil {
				debugItem.ID = tableItem.Id
				debugItem.Name = tableItem.Name
				debugItem.Kind = tableItem.Kind
				debugItem.DefinitionCount = len(tableItem.Defs)
				debugItem.CreateSQL = tableItem.CreateSql
				debugItem.LogicalID = tableItem.LogicalId
				if tableItem.TableDef != nil {
					debugItem.ColumnCount = len(tableItem.TableDef.Cols)
				}
			}
			contents.Tables = append(contents.Tables, debugItem)
		}

		return true
	})

	contents.ReturnedDatabaseCount = len(contents.Databases)
	contents.ReturnedTableCount = len(contents.Tables)
	return contents
}

func matchesDebugDatabaseFilter(filter string, item *DatabaseItem) bool {
	if filter == "" || item == nil {
		return true
	}
	return filter == item.Name || filter == strconv.FormatUint(item.Id, 10)
}

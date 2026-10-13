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

package disttae

import (
	"strings"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

// isSessionTemporaryCatalogItem reports whether item is a session temporary
// table that may be exposed by an old-snapshot catalog fallback. Temporary
// roots retain their existing name-and-kind rule. Temporary index tables also
// need current, reciprocal catalog ownership metadata.
func isSessionTemporaryCatalogItem(
	catalogCache *cache.CatalogCache,
	item *cache.TableItem,
	accountID uint32,
	databaseID uint64,
	databaseName string,
) bool {
	if item == nil {
		return false
	}
	if item.Kind == catalog.SystemTemporaryTable {
		return defines.IsTempTableName(item.Name)
	}
	if catalogCache == nil || item.Kind != catalog.SystemIndexRel ||
		item.AccountId != accountID || item.DatabaseId != databaseID ||
		item.DatabaseName != databaseName {
		return false
	}

	childSession, ok := parseSessionTemporaryTableName(item.Name, databaseName)
	if !ok || item.ExtraInfo == nil {
		return false
	}
	parentID := item.ExtraInfo.ParentTableID
	if parentID == 0 || parentID == item.Id {
		return false
	}

	// GetTableById returns only the latest live incarnation and treats a
	// tombstone as authoritative, so an older parent cannot validate the child.
	parent := catalogCache.GetTableById(accountID, databaseID, parentID)
	if parent == nil || parent.Id != parentID ||
		parent.AccountId != accountID || parent.DatabaseId != databaseID ||
		parent.DatabaseName != databaseName ||
		parent.Kind != catalog.SystemTemporaryTable {
		return false
	}
	parentSession, ok := parseSessionTemporaryTableName(parent.Name, databaseName)
	if !ok || parentSession != childSession || parent.ExtraInfo == nil {
		return false
	}

	// A child must be listed once by its parent. Requiring unique membership
	// fails closed on incomplete or internally inconsistent catalog metadata.
	children := 0
	for _, childID := range parent.ExtraInfo.IndexTables {
		if childID == item.Id {
			children++
			if children > 1 {
				return false
			}
		}
	}
	return children == 1
}

// parseSessionTemporaryTableName parses the fixed generated components while
// leaving the alias intact; aliases may themselves contain underscores.
func parseSessionTemporaryTableName(name, databaseName string) (uuid.UUID, bool) {
	if !defines.IsTempTableName(name) || databaseName == "" {
		return uuid.Nil, false
	}
	remainder := strings.TrimPrefix(name, defines.TempTableNamePrefix)
	const sessionIDLength = 32
	if len(remainder) <= sessionIDLength+1+len(databaseName)+1 ||
		remainder[sessionIDLength] != '_' {
		return uuid.Nil, false
	}
	sessionText := remainder[:sessionIDLength]
	sessionID, err := uuid.Parse(sessionText)
	if err != nil || strings.ReplaceAll(sessionID.String(), "-", "") != sessionText {
		return uuid.Nil, false
	}
	databasePrefix := databaseName + "_"
	if !strings.HasPrefix(remainder[sessionIDLength+1:], databasePrefix) {
		return uuid.Nil, false
	}
	alias := remainder[sessionIDLength+1+len(databasePrefix):]
	if alias == "" {
		return uuid.Nil, false
	}
	return sessionID, true
}

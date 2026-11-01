// Copyright 2022 Matrix Origin
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
	"maps"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// columnUpdateLeavesProjectionUnchanged recognizes column-only catalog updates
// that change no cached attribute (for example a derived unsigned flag). Raw
// catalog replay still applies the update. The caller holds tableChange; never
// publish a synthetic table version or assemble a schema from a partial update.
// DDL table and column events are delivered together, tables first. This is not
// recovery for missing DDL events or corrupted catalogs.
func (cc *CatalogCache) columnUpdateLeavesProjectionUnchanged(key *TableItem, columns Columns) bool {
	var previous *TableItem
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.AccountId == key.AccountId && item.DatabaseId == key.DatabaseId && item.Name == key.Name &&
			!item.deleted && item.Id == key.Id && item.Id != 0 && item.Ts.Less(key.Ts) {
			previous = item
		}
		return false // A tombstone or different incarnation must not expose older rows.
	})
	if previous == nil || previous.TableDef == nil || len(columns) == 0 {
		return false
	}
	attributes := make([]*engine.AttributeDef, 0, len(previous.TableDef.Cols))
	defs := make([]engine.TableDef, 0, len(previous.TableDef.Cols))
	primary, primarySeqnum, cluster := -1, -1, -1
	names := make(map[string]struct{}, len(previous.TableDef.Cols))
	for _, def := range previous.Defs {
		if attribute, ok := def.(*engine.AttributeDef); ok {
			if attribute == nil || attribute.Attr.ID != uint64(len(attributes)+1) {
				return false
			}
			index := len(attributes)
			name := strings.ToLower(attribute.Attr.Name)
			if _, exists := names[name]; exists {
				return false
			}
			names[name] = struct{}{}
			if attribute.Attr.Primary {
				primary, primarySeqnum = index, int(attribute.Attr.Seqnum)
			}
			if attribute.Attr.ClusterBy {
				cluster = index
			}
			attributes = append(attributes, attribute)
			defs = append(defs, attribute)
		}
	}
	if len(attributes) == 0 || len(attributes) != len(previous.TableDef.Cols) ||
		previous.PrimaryIdx != primary || previous.PrimarySeqnum != primarySeqnum || previous.ClusterByIdx != cluster {
		return false
	}
	last := attributes[len(attributes)-1].Attr
	if last.Name != catalog.Row_ID || !last.IsHidden || last.Type.Oid != types.T_Rowid {
		return false
	}
	// Reuse the column projection without decoding unrelated table metadata.
	projected, _ := getTableDef(&TableItem{Name: previous.Name}, defs)
	if !maps.Equal(previous.TableDef.Name2ColIndex, projected.Name2ColIndex) ||
		!proto.Equal(previous.TableDef.ClusterBy, projected.ClusterBy) {
		return false
	}
	for i, column := range projected.Cols {
		if !proto.Equal(previous.TableDef.Cols[i], column) {
			return false
		}
	}
	seen := make(map[int32]struct{}, len(columns))
	for _, column := range columns {
		if column.Num <= 0 || int(column.Num) > len(attributes) {
			return false
		}
		if _, ok := seen[column.Num]; ok {
			return false
		}
		seen[column.Num] = struct{}{}
		for _, flag := range []int8{column.IsHidden, column.IsUnsigned, column.IsAutoIncrement, column.HasDef, column.HasUpdate, column.HasGenerated, column.IsClusterBy} {
			if flag != 0 && flag != 1 {
				return false
			}
		}
		if column.ConstraintType != catalog.SystemColPKConstraint && column.ConstraintType != catalog.SystemColNoConstraint &&
			!(column.ConstraintType == "" && column.Name == catalog.Row_ID) {
			return false
		}
		incoming, err := decodeColumnAttribute(column)
		if err != nil || (column.IsUnsigned == 1 && !incoming.Attr.Type.IsUInt()) ||
			!equalColumnAttributes(attributes[column.Num-1].Attr, incoming.Attr) {
			return false
		}
	}
	return true
}

func equalColumnAttributes(a, b engine.Attribute) bool {
	if !proto.Equal(a.Default, b.Default) || !proto.Equal(a.OnUpdate, b.OnUpdate) || !proto.Equal(a.GeneratedCol, b.GeneratedCol) {
		return false
	}
	a.Default, b.Default = nil, nil
	a.OnUpdate, b.OnUpdate = nil, nil
	a.GeneratedCol, b.GeneratedCol = nil, nil
	return a == b
}

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
	if previous == nil || (previous.Defs == nil && previous.schema == nil) || len(columns) == 0 {
		return false
	}
	attributes, valid := validatedColumnAttributes(previous)
	if !valid {
		return false
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
			!equalColumnAttributes(attributes[column.Num-1], incoming.Attr) {
			return false
		}
	}
	return true
}

// validatedColumnAttributes validates the canonical schema before comparing updates.
// A contiguous schema ending in the hidden row ID establishes completeness.
func validatedColumnAttributes(previous *TableItem) ([]engine.Attribute, bool) {
	var attributes []engine.Attribute
	if previous.schema != nil {
		schema := previous.schema
		attributes = make([]engine.Attribute, len(schema.planner.Cols))
		remaining := schema.residual
		for i := range attributes {
			attributes[i] = schema.attribute(i, &remaining)
		}
	} else {
		attributes = make([]engine.Attribute, 0, len(previous.Defs))
		for _, def := range previous.Defs {
			if attribute, ok := def.(*engine.AttributeDef); ok {
				if attribute == nil {
					return nil, false
				}
				attributes = append(attributes, attribute.Attr)
			}
		}
	}
	primary, primarySeqnum, cluster := -1, -1, -1
	names := make(map[string]struct{}, len(attributes))
	for i := range attributes {
		attr := &attributes[i]
		if attr.ID != uint64(i+1) {
			return nil, false
		}
		name := strings.ToLower(attr.Name)
		if _, exists := names[name]; exists {
			return nil, false
		}
		names[name] = struct{}{}
		if attr.Primary {
			primary, primarySeqnum = i, int(attr.Seqnum)
		}
		if attr.ClusterBy {
			cluster = i
		}
	}
	if len(attributes) == 0 ||
		previous.PrimaryIdx != primary || previous.PrimarySeqnum != primarySeqnum || previous.ClusterByIdx != cluster {
		return nil, false
	}
	last := &attributes[len(attributes)-1]
	if last.Name != catalog.Row_ID || !last.IsHidden || last.Type.Oid != types.T_Rowid {
		return nil, false
	}
	return attributes, true
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

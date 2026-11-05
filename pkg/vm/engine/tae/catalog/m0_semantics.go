// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// EvolutionColumnSemantic requires the stable ID from the transaction catalog.
// TN ordinal/width cannot manufacture column identity. The caller must hydrate
// generated/default metadata from the authoritative catalog before this call.
func EvolutionColumnSemantic(col *ColDef, columnID uint64) ([]byte, error) {
	return evolutionColumnSemantic(col, columnID, nil)
}

func evolutionColumnSemantic(col *ColDef, columnID uint64, keys []uint64) ([]byte, error) {
	if col == nil || col.IsPhyAddr() {
		return nil, moerr.NewInternalErrorNoCtx("M0: invalid stored evolution column")
	}
	attr, err := AttrFromColDef(col)
	if err != nil {
		return nil, err
	}
	typ := vector.TypeToProtoType(col.Type)
	typ.Enumvalues = col.EnumValues
	typ.AutoIncr = col.AutoIncrement
	return m0read.EncodeColumnSemantic(m0read.ColumnSemantic{ID: columnID, Seqnum: uint32(col.SeqNum), Name: col.Name, Type: typ,
		Nullable: col.NullAbility, Hidden: col.Hidden, Primary: col.Primary, ClusterBy: col.ClusterBy, SortKey: col.SortKey,
		Default: attr.Default, OnUpdate: attr.OnUpdate, Generated: attr.GeneratedCol, KeyColumns: keys})
}

// EvolutionSchemaColumnSemantic uses catalog-owned constraint order and the
// contract's stable identities, rather than deriving authority from width.
func EvolutionSchemaColumnSemantic(schema *Schema, col *ColDef, id uint64) ([]byte, error) {
	var keys []uint64
	if col != nil && col.Hidden && col.Primary {
		if schema == nil || schema.Extra == nil || schema.Extra.M0Evolution == nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: key contract unavailable")
		}
		var constraints engine.ConstraintDef
		if err := constraints.UnmarshalBinary(schema.Constraint); err != nil {
			return nil, err
		}
		pk := constraints.GetPrimaryKeyDef()
		if pk == nil || pk.Pkey == nil || pk.Pkey.PkeyColName != col.Name || len(pk.Pkey.Names) < 2 {
			return nil, moerr.NewInternalErrorNoCtx("M0: TN composite key semantics unavailable")
		}
		seen := make(map[uint64]struct{}, len(pk.Pkey.Names))
		for _, name := range pk.Pkey.Names {
			pos, ok := schema.NameMap[name]
			if !ok {
				return nil, moerr.NewInternalErrorNoCtx("M0: TN key column unavailable")
			}
			key := schema.ColDefs[pos]
			c := schema.Extra.M0Evolution
			if key.Hidden || int(key.SeqNum) >= len(c.Columns) {
				return nil, moerr.NewInternalErrorNoCtx("M0: TN key identity unavailable")
			}
			keyID := c.Columns[key.SeqNum].ColumnId
			if _, ok := seen[keyID]; ok {
				return nil, moerr.NewInternalErrorNoCtx("M0: duplicate TN key identity")
			}
			seen[keyID] = struct{}{}
			keys = append(keys, keyID)
		}
	}
	return evolutionColumnSemantic(col, id, keys)
}

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

package cache

import (
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// candidateSchema owns the immutable planner and sparse engine residuals.
type candidateSchema struct {
	planner    *plan.TableDef
	constraint schemaConstraint
	residual   []candidateResidual
}
type candidateResidual struct {
	index int
	typ   types.Type
	alg   compress.T
	rowID bool
}

func candidateType(p plan.Type) (types.Type, bool) {
	oid := types.T(p.Id)
	if int32(oid) != p.Id {
		return types.Type{}, false
	}
	t, ok := oid.TryToType()
	if !ok {
		return types.Type{}, false
	}
	t.Width, t.Scale, t.Charset, t.CollationVersion = p.Width, p.Scale, uint8(p.Charset), uint8(p.CollationVersion)
	return t, true
}

func newCandidateSchema(item *TableItem) *candidateSchema {
	if item == nil || item.Defs == nil || item.deleted {
		return nil
	}
	p, _ := getTableDef(item, item.Defs)
	if p == nil {
		return nil
	}
	s := &candidateSchema{planner: p, constraint: newSchemaConstraint(item.Constraint)}
	// Rebuild all wrappers so no retained pointer keeps getTableDef's dense
	// Default allocation alive. Only the two schema-local zero wrappers alias.
	var zero [2]*plan.Default
	i := 0
	for _, d := range item.Defs {
		if a, ok := d.(*engine.AttributeDef); ok {
			c, original := p.Cols[i], a.Attr.Default
			if plan.CatalogZeroDefault(original) {
				n := 0
				if original.NullAbility {
					n = 1
				}
				if zero[n] == nil {
					zero[n] = &plan.Default{NullAbility: original.NullAbility}
				}
				c.Default = zero[n]
			} else {
				c.Default = plan.CopyDefaultInto(new(plan.Default), original)
				if c.Default != nil {
					c.Default.XXX_sizecache = original.XXX_sizecache
				}
			}
			typ, ok := candidateType(c.Typ)
			if !ok || a.Attr.Type != typ || a.Attr.Alg != compress.Lz4 || a.Attr.IsRowId {
				s.residual = append(s.residual, candidateResidual{index: i, typ: a.Attr.Type, alg: a.Attr.Alg, rowID: a.Attr.IsRowId})
			}
			i++
		}
	}
	return s
}

// attribute borrows expression metadata for read-only comparison. The ordered
// residual cursor permits one pass for either all columns or a subset.
func (s *candidateSchema) attribute(index int, remaining *[]candidateResidual) engine.Attribute {
	col := s.planner.Cols[index]
	attr := engine.Attribute{
		ID: col.ColId, Name: col.OriginName, Comment: col.Comment,
		Seqnum: uint16(col.Seqnum), IsHidden: col.Hidden, Primary: col.Primary,
		ClusterBy: col.ClusterBy, AutoIncrement: col.Typ.AutoIncr, EnumVlaues: col.Typ.Enumvalues,
		NumericAttributes: uint8(col.Typ.NumericAttributes),
		Default:           col.Default, OnUpdate: col.OnUpdate, GeneratedCol: col.GeneratedCol,
		Alg: compress.Lz4,
	}
	for len(*remaining) > 0 && (*remaining)[0].index < index {
		*remaining = (*remaining)[1:]
	}
	if len(*remaining) > 0 && (*remaining)[0].index == index {
		r := (*remaining)[0]
		attr.Type, attr.Alg, attr.IsRowId = r.typ, r.alg, r.rowID
		*remaining = (*remaining)[1:]
	} else {
		attr.Type, _ = candidateType(col.Typ)
	}
	return attr
}

func (s *candidateSchema) attributes(primaryOnly bool) []engine.AttributeDef {
	count := len(s.planner.Cols)
	if primaryOnly {
		count = 0
		for _, col := range s.planner.Cols {
			if col.Primary {
				count++
			}
		}
	}
	defs := make([]engine.AttributeDef, 0, count)
	remaining := s.residual
	for i, col := range s.planner.Cols {
		if primaryOnly && !col.Primary {
			continue
		}
		attr := s.attribute(i, &remaining)
		attr.Default = plan.CopyDefaultInto(new(plan.Default), attr.Default)
		attr.OnUpdate = cloneOnUpdate(attr.OnUpdate)
		attr.GeneratedCol = cloneGeneratedCol(attr.GeneratedCol)
		defs = append(defs, engine.AttributeDef{Attr: attr})
	}
	return defs
}

func (s *candidateSchema) engineDefs(item *TableItem) ([]engine.TableDef, error) {
	var constraint *engine.ConstraintDef
	if len(s.constraint.raw) != 0 {
		var err error
		constraint, err = s.constraint.decode()
		if err != nil {
			return nil, err
		}
	}
	defs := engineDefsHeader(item, constraint, len(s.planner.Cols))
	attrs := s.attributes(false)
	for i := range attrs {
		defs = append(defs, &attrs[i])
	}
	return defs, nil
}

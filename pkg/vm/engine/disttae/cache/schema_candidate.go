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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
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

func (s *candidateSchema) engineParts() ([]types.Type, []compress.T, []bool) {
	ts := make([]types.Type, len(s.planner.Cols))
	algs := make([]compress.T, len(ts))
	rows := make([]bool, len(ts))
	for i, c := range s.planner.Cols {
		ts[i], _ = candidateType(c.Typ)
		algs[i] = compress.Lz4
	}
	for _, r := range s.residual {
		ts[r.index], algs[r.index], rows[r.index] = r.typ, r.alg, r.rowID
	}
	return ts, algs, rows
}

// engineDefs reconstructs a private view from the candidate's planner and
// sparse engine-only fields.
func (s *candidateSchema) expanded() *prototypeSchema {
	ts, algs, rows := s.engineParts()
	template := *s.planner
	template.Cols = nil
	if template.Pkey != nil && template.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		copyKey := *template.Pkey
		copyKey.CompPkeyCol = nil
		template.Pkey = &copyKey
	}
	if template.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(template.ClusterBy.Name) {
		copyKey := *template.ClusterBy
		copyKey.CompCbkeyCol = nil
		template.ClusterBy = &copyKey
	}
	p := &prototypeSchema{planner: &template, constraint: s.constraint, columns: make([]catalogColumn, len(s.planner.Cols))}
	for i, c := range s.planner.Cols {
		p.columns[i] = catalogColumn{typ: ts[i], plannerType: c.Typ, name: c.Name, originName: c.OriginName, comment: c.Comment, colID: c.ColId, seqnum: c.Seqnum, alg: algs[i], defaultValue: c.Default, onUpdate: c.OnUpdate, generated: c.GeneratedCol, rowID: rows[i], hidden: c.Hidden, primary: c.Primary, clusterBy: c.ClusterBy}
	}
	return p
}

func (s *candidateSchema) engineDefs(item *TableItem) ([]engine.TableDef, error) {
	return s.expanded().engineDefs(item)
}

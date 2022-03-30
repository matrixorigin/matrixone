// Copyright 2021 Matrix Origin
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

package plan

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transformer"
)

// rough estimate
func (s *Scope) Rows() int64 {
	var rows int64

	if rel, ok := s.Op.(*Relation); ok {
		return rel.Rows
	}
	for _, chp := range s.Children {
		rows += chp.Rows()
	}
	return rows
}

func (s *Scope) prune(attrsMap map[string]uint64, fvars []string) {
	switch op := s.Op.(type) {
	case *Join:
		for name, ref := range attrsMap {
			if attr, ok := s.Result.AttrsMap[name]; ok {
				attr.Ref = int(ref)
			}
		}
		for i := 0; i < len(s.Result.Attrs); i++ {
			if attr := s.Result.AttrsMap[s.Result.Attrs[i]]; attr.Ref <= 0 {
				s.Result.Attrs = append(s.Result.Attrs[:i], s.Result.Attrs[i+1:]...)
				i--
				continue
			}
		}
		ms := s.classifying()
		for i := 0; i < len(s.Children); i++ {
			s.Children[i].prune(ms[i], fvars)
		}
	case *Order:
		for _, f := range op.Fs {
			attrsMap[f.Attr]++
		}
		s.Children[0].prune(attrsMap, fvars)
	case *Dedup:
		s.Children[0].prune(attrsMap, fvars)
	case *Limit:
		s.Children[0].prune(attrsMap, fvars)
	case *Offset:
		s.Children[0].prune(attrsMap, fvars)
	case *Restrict:
		attrs := op.E.Attributes()
		for _, attr := range attrs {
			attrsMap[attr]++
		}
		s.Children[0].prune(attrsMap, fvars)
	case *Projection:
		for i := 0; i < len(op.As); i++ {
			if ref, ok := attrsMap[op.As[i]]; ok {
				op.Rs[i] += ref
			}
		}
		mp := make(map[string]uint64)
		for i := 0; i < len(op.Rs); i++ {
			if op.Rs[i] == 0 {
				op.As = append(op.As[:i], op.As[i+1:]...)
				op.Es = append(op.Es[:i], op.Es[i+1:]...)
				op.Rs = append(op.Rs[:i], op.Rs[i+1:]...)
				i--
				continue
			}
			attrs := op.Es[i].Attributes()
			for _, attr := range attrs {
				mp[attr]++
			}
		}
		s.Children[0].prune(mp, fvars)
	case *ResultProjection:
		for i := 0; i < len(op.As); i++ {
			if ref, ok := attrsMap[op.As[i]]; ok {
				op.Rs[i] += ref
			}
		}
		mp := make(map[string]uint64)
		for i := 0; i < len(op.Rs); i++ {
			if op.Rs[i] == 0 {
				op.As = append(op.As[:i], op.As[i+1:]...)
				op.Es = append(op.Es[:i], op.Es[i+1:]...)
				op.Rs = append(op.Rs[:i], op.Rs[i+1:]...)
				i--
				continue
			}
			attrs := op.Es[i].Attributes()
			for _, attr := range attrs {
				mp[attr]++
			}
		}
		s.Children[0].prune(mp, fvars)
	case *Relation:
		{ // fill boundvars
			if len(fvars) > 0 || len(op.BoundVars) > 0 {
				op.Flg = true
				mp := make(map[string]uint64)
				for k, v := range attrsMap {
					mp[k] = v
				}
				for _, fvar := range op.FreeVars {
					delete(mp, fvar)
				}
				for _, bvar := range op.BoundVars {
					delete(mp, bvar.Alias)
				}
				for k, _ := range mp {
					op.BoundVars = append(op.BoundVars, &Aggregation{
						Ref:   1,
						Op:    transformer.Min,
						Name:  k,
						Alias: k,
						Type:  op.Attrs[k].Type.Oid,
					})
				}
				for i, bvar := range op.BoundVars {
					if ref, ok := attrsMap[bvar.Alias]; ok {
						op.BoundVars[i].Ref = int(ref)
					}
				}
			}
		}
		for _, bvar := range op.BoundVars {
			attrsMap[bvar.Name]++
		}
		if op.Cond != nil {
			attrs := op.Cond.Attributes()
			for _, attr := range attrs {
				attrsMap[attr]++
			}
		}
		for i := 0; i < len(op.Proj.As); i++ {
			if ref, ok := attrsMap[op.Proj.As[i]]; ok {
				op.Proj.Rs[i] += ref
			}
		}
		mp := make(map[string]uint64)
		for i := 0; i < len(op.Proj.Rs); i++ {
			if op.Proj.Rs[i] == 0 {
				op.Proj.As = append(op.Proj.As[:i], op.Proj.As[i+1:]...)
				op.Proj.Es = append(op.Proj.Es[:i], op.Proj.Es[i+1:]...)
				op.Proj.Rs = append(op.Proj.Rs[:i], op.Proj.Rs[i+1:]...)
				i--
				continue
			}
			attrs := op.Proj.Es[i].Attributes()
			for _, attr := range attrs {
				mp[attr]++
			}
		}
		for _, e := range op.Proj.Es {
			attrs := e.Attributes()
			for i := range attrs {
				if ref, ok := mp[attrs[i]]; ok {
					attr := op.Attrs[attrs[i]]
					attr.Ref = int(ref)
				}
			}
		}
		for k, attr := range op.Attrs {
			if attr.Ref == 0 {
				delete(op.Attrs, k)
			}
		}
	case *DerivedRelation:
		{ // fill boundvars
			if len(fvars) > 0 || len(op.BoundVars) > 0 {
				op.Flg = true
				mp := make(map[string]uint64)
				for k, v := range attrsMap {
					mp[k] = v
				}
				for _, fvar := range op.FreeVars {
					delete(mp, fvar)
				}
				for _, bvar := range op.BoundVars {
					delete(mp, bvar.Alias)
				}
				for k, _ := range mp {
					op.BoundVars = append(op.BoundVars, &Aggregation{
						Ref:   1,
						Op:    transformer.Min,
						Name:  k,
						Alias: k,
						Type:  s.Result.AttrsMap[k].Type.Oid,
					})
				}
			}
		}
		s.Children[0].reset()
		for i := 0; i < len(s.Result.Attrs); i++ {
			attr := s.Result.Attrs[i]
			if ref, ok := attrsMap[attr]; !ok || ref <= 0 {
				s.Result.Attrs = append(s.Result.Attrs[:i], s.Result.Attrs[i+1:]...)
				delete(s.Result.AttrsMap, attr)
				i--
				continue
			}
		}
		for _, bvar := range op.BoundVars {
			attrsMap[bvar.Name]++
		}
		if op.Cond != nil {
			attrs := op.Cond.Attributes()
			for _, attr := range attrs {
				attrsMap[attr]++
			}
		}
		for i := 0; i < len(op.Proj.As); i++ {
			if ref, ok := attrsMap[op.Proj.As[i]]; ok {
				op.Proj.Rs[i] += ref
			}
		}
		mp := make(map[string]uint64)
		for i := 0; i < len(op.Proj.Rs); i++ {
			if op.Proj.Rs[i] == 0 {
				op.Proj.As = append(op.Proj.As[:i], op.Proj.As[i+1:]...)
				op.Proj.Es = append(op.Proj.Es[:i], op.Proj.Es[i+1:]...)
				op.Proj.Rs = append(op.Proj.Rs[:i], op.Proj.Rs[i+1:]...)
				i--
				continue
			}
			attrs := op.Proj.Es[i].Attributes()
			for _, attr := range attrs {
				mp[attr]++
			}
		}
		s.Children[0].prune(mp, nil)
	case *Untransform:
		for _, fvar := range op.FreeVars {
			attrsMap[fvar]++
		}
		s.Children[0].prune(attrsMap, op.FreeVars)
	case *Rename:
		for i := 0; i < len(op.As); i++ {
			if ref, ok := attrsMap[op.As[i]]; ok {
				op.Rs[i] += ref
			}
		}
		mp := make(map[string]uint64)
		for i := 0; i < len(op.Rs); i++ {
			if op.Rs[i] == 0 {
				op.As = append(op.As[:i], op.As[i+1:]...)
				op.Es = append(op.Es[:i], op.Es[i+1:]...)
				op.Rs = append(op.Rs[:i], op.Rs[i+1:]...)
				i--
				continue
			}
			attrs := op.Es[i].Attributes()
			for _, attr := range attrs {
				mp[attr]++
			}
		}
		s.Children[0].prune(mp, fvars)
	}
}

func (s *Scope) getAggregations() []*Aggregation {
	switch op := s.Op.(type) {
	case *Join:
		var aggs []*Aggregation

		for i := range s.Children {
			aggs = append(aggs, s.Children[i].getAggregations()...)
		}
		return aggs
	case *Order:
		return s.Children[0].getAggregations()
	case *Dedup:
		return s.Children[0].getAggregations()
	case *Limit:
		return s.Children[0].getAggregations()
	case *Offset:
		return s.Children[0].getAggregations()
	case *Restrict:
		return s.Children[0].getAggregations()
	case *Projection:
		return s.Children[0].getAggregations()
	case *ResultProjection:
		return s.Children[0].getAggregations()
	case *Relation:
		return op.BoundVars
	case *DerivedRelation:
		return op.BoundVars
	case *Untransform:
		return s.Children[0].getAggregations()
	case *Rename:
		return s.Children[0].getAggregations()
	}
	return nil
}

func (s *Scope) pruneProjection(aggs []*Aggregation) {
	switch op := s.Op.(type) {
	case *Order:
		s.Children[0].pruneProjection(aggs)
	case *Dedup:
		s.Children[0].pruneProjection(aggs)
	case *Limit:
		s.Children[0].pruneProjection(aggs)
	case *Offset:
		s.Children[0].pruneProjection(aggs)
	case *Restrict:
		s.Children[0].pruneProjection(aggs)
	case *Projection:
		if _, ok := s.Children[0].Op.(*Untransform); ok {
			mp := make(map[string]int)
			for i := range op.Es {
				if e, ok := op.Es[i].(*extend.Attribute); ok {
					mp[e.Name] = 0
				}
			}
			for _, agg := range aggs {
				if _, ok := mp[agg.Alias]; !ok {
					op.Rs = append(op.Rs, 1)
					op.As = append(op.As, agg.Alias)
					op.Es = append(op.Es, &extend.Attribute{
						Type: agg.Type,
						Name: agg.Alias,
					})
				}
			}
		}
		s.Children[0].pruneProjection(aggs)
	case *ResultProjection:
		s.Children[0].pruneProjection(aggs)
	case *Relation:
	case *DerivedRelation:
	case *Untransform:
		s.Children[0].pruneProjection(aggs)
	case *Rename:
		s.Children[0].pruneProjection(aggs)
	}
}

func (s *Scope) reset() {
	switch op := s.Op.(type) {
	case *Join:
		for i := 0; i < len(s.Children); i++ {
			s.Children[i].reset()
		}
	case *Order:
		s.Children[0].reset()
	case *Dedup:
		s.Children[0].reset()
	case *Limit:
		s.Children[0].reset()
	case *Offset:
		s.Children[0].reset()
	case *Restrict:
		s.Children[0].reset()
	case *Projection:
		for i := 0; i < len(op.Rs); i++ {
			op.Rs[i] = 0
		}
		s.Children[0].reset()
	case *ResultProjection:
		for i := 0; i < len(op.Rs); i++ {
			op.Rs[i] = 0
		}
		s.Children[0].reset()
	case *Relation:
		for i := 0; i < len(op.Proj.Rs); i++ {
			op.Proj.Rs[i] = 0
		}
		for _, attr := range op.Attrs {
			attr.Ref = 0
		}
	case *DerivedRelation:
		for i := 0; i < len(op.Proj.Rs); i++ {
			op.Proj.Rs[i] = 0
		}
	case *Untransform:
		s.Children[0].reset()
	case *Rename:
		s.Children[0].reset()
	}
}

func (s *Scope) classifying() []map[string]uint64 {
	ms := make([]map[string]uint64, len(s.Children))
	for i := range s.Children {
		ms[i] = make(map[string]uint64)
	}
	for _, name := range s.Result.Attrs {
		ref := s.Result.AttrsMap[name].Ref
		if ref <= 0 {
			continue
		}
		for i := range s.Children {
			if _, ok := s.Children[i].Result.AttrsMap[name]; ok {
				ms[i][name] = uint64(ref)
			}
		}
	}
	op := s.Op.(*Join)
	for _, vs := range op.Vars {
		for _, v := range vs {
			name := strconv.Itoa(v)
			for i := range s.Children {
				if _, ok := s.Children[i].Result.AttrsMap[name]; ok {
					ms[i][name]++
				}
			}
		}
	}
	return ms
}

func (s *Scope) registerRelations(qry *Query) {
	switch op := s.Op.(type) {
	case *Join:
		for i := 0; i < len(s.Children); i++ {
			s.Children[i].registerRelations(qry)
		}
	case *Order:
		s.Children[0].registerRelations(qry)
	case *Dedup:
		s.Children[0].registerRelations(qry)
	case Limit:
		s.Children[0].registerRelations(qry)
	case Offset:
		s.Children[0].registerRelations(qry)
	case *Restrict:
		s.Children[0].registerRelations(qry)
	case *Projection:
		s.Children[0].registerRelations(qry)
	case *ResultProjection:
		s.Children[0].registerRelations(qry)
	case *Relation:
		if mp, ok := qry.Rels[op.Schema]; ok {
			mp[op.Name] = s
		} else {
			mp := make(map[string]*Scope)
			mp[op.Name] = s
			qry.Rels[op.Schema] = mp
		}
	case *Untransform:
		s.Children[0].registerRelations(qry)
	case *Rename:
		if _, ok := s.Children[0].Op.(*Relation); ok {
			qry.RenameRels[s.Name] = s.Children[0]
		} else {
			s.Children[0].registerRelations(qry)
		}
	case *DerivedRelation:
		qry.RenameRels[s.Name] = s
	}
}

func printScopes(prefix []byte, ss []*Scope, buf *bytes.Buffer) {
	for _, s := range ss {
		if len(s.Children) > 0 {
			printScopes(append(prefix, "    "...), s.Children, buf)
		}
		switch op := s.Op.(type) {
		case *Join:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printJoin(op, s)))
		case *Order:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printOrder(op)))
		case *Dedup:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printDedup(op)))
		case *Limit:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printLimit(op)))
		case *Offset:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printOffset(op)))
		case *Rename:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printRename(op)))
		case *Restrict:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printRestrict(op)))
		case *Projection:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printProjection(op)))
		case *ResultProjection:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printResultProjection(op)))
		case *Relation:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printRelation(op)))
		case *Untransform:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printUntransform(op)))
		case *DerivedRelation:
			buf.WriteString(fmt.Sprintf("%s%v\n", prefix, printDerivedRelation(op, s)))
		}
	}
}

func printJoin(op *Join, s *Scope) string {
	return fmt.Sprintf("⨝(%v[%v])", op.Type, op.Vars)
}

func printDedup(op *Dedup) string {
	return fmt.Sprintf("δ()")
}

func printOrder(op *Order) string {
	var buf bytes.Buffer

	buf.WriteString("τ([")
	for i, f := range op.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString(fmt.Sprintf("])"))
	return buf.String()
}

func printRestrict(op *Restrict) string {
	return fmt.Sprintf("σ(%s)", op.E)
}

func printRelation(op *Relation) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("%v.%v: [", op.Schema, op.Name))
	{
		cnt := 0
		for k, v := range op.Attrs {
			if cnt > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("%s:%v", k, v.Ref))
			cnt++
		}
		buf.WriteString("]")
	}
	if len(op.Proj.Es) > 0 {
		buf.WriteString(" -> π(")
		for i, e := range op.Proj.Es {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(fmt.Sprintf("%s -> %s: %v", e, op.Proj.As[i], op.Proj.Rs[i]))
		}
		buf.WriteString(")")
	}
	if op.Cond != nil {
		buf.WriteString(fmt.Sprintf(" -> σ(%s)", op.Cond))
	}
	if op.Flg {
		if len(op.BoundVars) > 0 || len(op.FreeVars) > 0 {
			buf.WriteString(" -> ∏([")
			for i, fvar := range op.FreeVars {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(fvar)
			}
			buf.WriteString("], [")
			for i, bvar := range op.BoundVars {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(fmt.Sprintf("%s(%s) -> %s: %v", transformer.TransformerNames[bvar.Op], bvar.Name, bvar.Alias, bvar.Ref))
			}
			buf.WriteString("])")
		}
	} else if len(op.FreeVars) > 0 {
		buf.WriteString(fmt.Sprintf(" -> θ(%v)", op.FreeVars))
	}
	return buf.String()
}

func printDerivedRelation(op *DerivedRelation, s *Scope) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("derived relation: [%v] [", s.Result.Attrs))
	if len(op.Proj.Es) > 0 {
		buf.WriteString(" -> π(")
		for i, e := range op.Proj.Es {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(fmt.Sprintf("%s -> %s: %v", e, op.Proj.As[i], op.Proj.Rs[i]))
		}
		buf.WriteString(")")
	}
	if op.Cond != nil {
		buf.WriteString(fmt.Sprintf(" -> σ(%s)", op.Cond))
	}
	if op.Flg {
		if len(op.BoundVars) > 0 || len(op.FreeVars) > 0 {
			buf.WriteString(" -> ∏([")
			for i, fvar := range op.FreeVars {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(fvar)
			}
			buf.WriteString("], [")
			for i, bvar := range op.BoundVars {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(fmt.Sprintf("%s(%s) -> %s: %v", transformer.TransformerNames[bvar.Op], bvar.Name, bvar.Alias, bvar.Ref))
			}
			buf.WriteString("])")
		}
	} else if len(op.FreeVars) > 0 {
		buf.WriteString(fmt.Sprintf(" -> θ(%v)", op.FreeVars))
	}
	buf.WriteString("]")
	return buf.String()
}

func printLimit(op *Limit) string {
	return fmt.Sprintf("limit(%v)", op.Limit)
}

func printOffset(op *Offset) string {
	return fmt.Sprintf("offset(%v)", op.Offset)
}

func printRename(op *Rename) string {
	var buf bytes.Buffer

	buf.WriteString("π(")
	for i, e := range op.Es {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s[%T] -> %s: %v", e, e, op.As[i], op.Rs[i]))
	}
	buf.WriteString(")")
	return buf.String()
}

func printProjection(op *Projection) string {
	var buf bytes.Buffer

	buf.WriteString("π(")
	for i, e := range op.Es {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s[%T] -> %s: %v", e, e, op.As[i], op.Rs[i]))
	}
	buf.WriteString(")")
	return buf.String()
}

func printResultProjection(op *ResultProjection) string {
	var buf bytes.Buffer

	buf.WriteString("π(")
	for i, e := range op.Es {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s[%T] -> %s: %v", e, e, op.As[i], op.Rs[i]))
	}
	buf.WriteString(")")
	return buf.String()
}

func printUntransform(op *Untransform) string {
	var buf bytes.Buffer

	buf.WriteString("∐ ([")
	for i, fvar := range op.FreeVars {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fvar)
	}
	buf.WriteString("])")
	return buf.String()
}

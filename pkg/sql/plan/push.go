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
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

// check if it is conjunctive aggregation query.
func isCAQ(flg bool, s *Scope) bool {
	switch s.Op.(type) {
	case *Order:
		return isCAQ(flg, s.Children[0])
	case *Dedup:
		return isCAQ(flg, s.Children[0])
	case *Limit:
		return isCAQ(flg, s.Children[0])
	case *Offset:
		return isCAQ(flg, s.Children[0])
	case *Restrict:
		return isCAQ(flg, s.Children[0])
	case *Projection:
		return isCAQ(flg, s.Children[0])
	case *ResultProjection:
		return isCAQ(flg, s.Children[0])
	case *Rename:
		return isCAQ(flg, s.Children[0])
	case *Untransform:
		return isCAQ(true, s.Children[0])
	case *Join:
		if flg {
			return true
		}
		return false
	}
	return false
}

func pushDownUntransform(s *Scope, fvars []string) {
	switch op := s.Op.(type) {
	case *Order:
		pushDownUntransform(s.Children[0], fvars)
	case *Dedup:
		pushDownUntransform(s.Children[0], fvars)
	case *Limit:
		pushDownUntransform(s.Children[0], fvars)
	case *Offset:
		pushDownUntransform(s.Children[0], fvars)
	case *Restrict:
		pushDownUntransform(s.Children[0], fvars)
	case *Projection:
		if uop, ok := s.Children[0].Op.(*Untransform); ok {
			pushDownUntransform(s.Children[0].Children[0], uop.FreeVars)
			s.Children[0] = s.Children[0].Children[0]
			return
		}
		if _, ok := s.Children[0].Op.(*Join); ok {
			child := s.Children[0]
			rs := &Scope{
				Name:     child.Name,
				Children: []*Scope{child},
			}
			rfvars := make([]string, len(fvars))
			{
				mp := make(map[string]string)
				for i, e := range op.Es {
					if attr, ok := e.(*extend.Attribute); ok {
						mp[op.As[i]] = attr.Name
					}
				}
				for i, fvar := range fvars {
					rfvars[i] = mp[fvar]
				}
			}
			{ // construct result
				rs.Result.AttrsMap = make(map[string]*Attribute)
				for _, attr := range child.Result.Attrs {
					rs.Result.Attrs = append(s.Result.Attrs, attr)
					rs.Result.AttrsMap[attr] = &Attribute{
						Name: attr,
						Type: child.Result.AttrsMap[attr].Type,
					}
				}
			}
			rs.Op = &Untransform{rfvars}
			s.Children = []*Scope{rs}
			for i := range child.Children {
				pushDownUntransform(child.Children[i], rfvars)
			}
			return
		}
		if rel, ok := s.Children[0].Op.(*Relation); ok {
			mp := make(map[string]int)
			{
				for i, bvar := range rel.BoundVars {
					if bvar.Name == bvar.Alias {
						mp[bvar.Name] = i
					}
				}
			}
			for i := 0; i < len(op.Es); i++ {
				if e, ok := op.Es[i].(*extend.Attribute); ok {
					if j, ok := mp[e.Name]; ok {
						rel.BoundVars[j].Alias = op.As[i]
						op.Rs = append(op.Rs[:i], op.Rs[i+1:]...)
						op.As = append(op.As[:i], op.As[i+1:]...)
						op.Es = append(op.Es[:i], op.Es[i+1:]...)
						continue
					}
				}
			}
			return
		}
		pushDownUntransform(s.Children[0], fvars)
	case *ResultProjection:
		pushDownUntransform(s.Children[0], fvars)
	case *Rename:
		pushDownUntransform(s.Children[0], fvars)
	}
}

func pushDownRestrict(flg bool, e extend.Extend, qry *Query) extend.Extend {
	if pushDownRestrictExtend(e, qry) {
		return nil
	}
	if es := extend.AndExtends(e, nil); len(es) > 0 {
		for i := 0; i < len(es); i++ {
			if pushDownRestrictExtend(es[i], qry) {
				es = append(es[:i], es[i+1:]...)
				i--
			}
		}
		e = extendsToAndExtend(es)
	}
	return e
}

func pushDownAggregation(aggs []*Aggregation, qry *Query) error {
	for i := 0; i < len(aggs); i++ {
		s, e, ok := pushDownProjection(aggs[i].E, qry)
		if !ok {
			return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("attributes involved in the aggregation by must belong to the same relation"))
		}
		aggs[i].Name = e.String()
		switch rel := s.Op.(type) {
		case *Relation:
			rel.BoundVars = append(rel.BoundVars, aggs[i])
		case *DerivedRelation:
			rel.BoundVars = append(rel.BoundVars, aggs[i])
			{ // construct result
				s.Result.Attrs = append(s.Result.Attrs, aggs[i].Alias)
				s.Result.AttrsMap[aggs[i].Alias] = &Attribute{
					Name: aggs[i].Alias,
					Type: aggs[i].Type.ToType(),
				}
			}
		}
	}
	return nil
}

func pushDownFreeVariables(es []extend.Extend, qry *Query) error {
	for i := 0; i < len(es); i++ {
		s, e, ok := pushDownProjection(es[i], qry)
		if !ok {
			return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("attributes involved in the group by must belong to the same relation"))
		}
		es[i] = &extend.Attribute{
			Name: es[i].String(),
			Type: es[i].ReturnType(),
		}
		switch rel := s.Op.(type) {
		case *Relation:
			rel.FreeVars = append(rel.FreeVars, e.String())
		case *DerivedRelation:
			rel.FreeVars = append(rel.FreeVars, e.String())
		}
	}
	return nil
}

func (s *Scope) pushDownJoinAttribute(attrs []string) {
	switch op := s.Op.(type) {
	case *Join:
		jattrs := make([][]string, len(s.Children))
		for _, vs := range op.Vars {
			for _, v := range vs {
				name := strconv.Itoa(v)
				for i := range s.Children {
					if _, ok := s.Children[i].Result.AttrsMap[name]; ok {
						jattrs[i] = append(jattrs[i], name)
					}
				}
			}
		}
		for i := 0; i < len(s.Children); i++ {
			s.Children[i].pushDownJoinAttribute(jattrs[i])
		}
	case *Order:
		s.Children[0].pushDownJoinAttribute(attrs)
	case *Dedup:
		s.Children[0].pushDownJoinAttribute(attrs)
	case *Limit:
		s.Children[0].pushDownJoinAttribute(attrs)
	case *Offset:
		s.Children[0].pushDownJoinAttribute(attrs)
	case *Restrict:
		s.Children[0].pushDownJoinAttribute(attrs)
	case *Projection:
		if len(attrs) > 0 {
			mp := make(map[string]int)
			for i := range op.As {
				mp[op.As[i]] = i
			}
			pattrs := make([]string, len(attrs))
			for i, attr := range attrs {
				pattrs[i] = op.Es[mp[attr]].String()
			}
			s.Children[0].pushDownJoinAttribute(pattrs)
		} else {
			s.Children[0].pushDownJoinAttribute(attrs)
		}
	case *Relation:
		mp := make(map[string]int)
		{
			for _, fvar := range op.FreeVars {
				mp[fvar]++
			}
		}
		for _, attr := range attrs {
			if _, ok := mp[attr]; !ok {
				mp[attr]++
				op.FreeVars = append(op.FreeVars, attr)
			}
		}
	case *DerivedRelation:
		mp := make(map[string]int)
		{
			for _, fvar := range op.FreeVars {
				mp[fvar]++
			}
		}
		for _, attr := range attrs {
			if _, ok := mp[attr]; !ok {
				mp[attr]++
				op.FreeVars = append(op.FreeVars, attr)
			}
		}
	case *Untransform:
		s.Children[0].pushDownJoinAttribute(attrs)
	case *Rename:
		s.Children[0].pushDownJoinAttribute(attrs)
	}
}

func pushDownProjection(e extend.Extend, qry *Query) (*Scope, extend.Extend, bool) {
	var s *Scope

	attrs := e.Attributes()
	switch len(attrs) {
	case 0:
		return nil, nil, false
	case 1:
		var name string

		s = findScopeWithAttribute(attrs[0], &name, qry.Scope)
	default:
		var name string

		s = findScopeWithAttribute(attrs[0], &name, qry.Scope)
		if s == nil {
			return nil, nil, false
		}
		for i := 1; i < len(attrs); i++ {
			if t := findScopeWithAttribute(attrs[i], &name, qry.Scope); s != t {
				return nil, nil, false
			}
		}
	}
	if rel, ok := s.Op.(*DerivedRelation); ok {
		pe := pushDownProjectionExtend(e, qry)
		alias := pe.String()
		{ // check projection exist or not
			for i := 0; i < len(rel.Proj.Es); i++ {
				if rel.Proj.As[i] == alias {
					return s, pe, true
				}
			}
		}
		rel.Proj.Rs = append(rel.Proj.Rs, 0)
		rel.Proj.Es = append(rel.Proj.Es, pe)
		rel.Proj.As = append(rel.Proj.As, alias)
		{ // construct result
			s.Result.Attrs = append(s.Result.Attrs, alias)
			s.Result.AttrsMap[alias] = &Attribute{
				Name: alias,
				Type: e.ReturnType().ToType(),
			}
		}
		return s, pe, true
	}
	rel := s.Op.(*Relation)
	pe := pushDownProjectionExtend(e, qry)
	alias := pe.String()
	{ // check projection exist or not
		for i := 0; i < len(rel.Proj.Es); i++ {
			if rel.Proj.As[i] == alias {
				return s, pe, true
			}
		}
	}
	rel.Proj.Rs = append(rel.Proj.Rs, 0)
	rel.Proj.Es = append(rel.Proj.Es, pe)
	rel.Proj.As = append(rel.Proj.As, alias)
	{ // construct result
		s.Result.Attrs = append(s.Result.Attrs, alias)
		s.Result.AttrsMap[alias] = &Attribute{
			Name: alias,
			Type: e.ReturnType().ToType(),
		}
	}
	return s, pe, true
}

func pushDownProjectionExtend(e extend.Extend, qry *Query) extend.Extend {
	switch v := e.(type) {
	case *extend.Attribute:
		var name string

		findScopeWithAttribute(v.Name, &name, qry.Scope)
		return &extend.Attribute{
			Name: name,
			Type: v.Type,
		}
	case *extend.UnaryExtend:
		v.E = pushDownProjectionExtend(v.E, qry)
		return v
	case *extend.ParenExtend:
		v.E = pushDownProjectionExtend(v.E, qry)
		return v
	case *extend.ValueExtend:
		return v
	case *extend.BinaryExtend:
		v.Left = pushDownProjectionExtend(v.Left, qry)
		v.Right = pushDownProjectionExtend(v.Right, qry)
	case *extend.MultiExtend:
		for i := 0; i < len(v.Args); i++ {
			v.Args[i] = pushDownProjectionExtend(v.Args[i], qry)
		}
		return v
	}
	return e
}

func pushDownRestrictExtend(e extend.Extend, qry *Query) bool {
	var s *Scope

	attrs := e.ExtendAttributes()
	aliases := make([]string, len(attrs))
	if s = findScopeWithAttribute(attrs[0].Name, &aliases[0], qry.Scope); s == nil {
		return false
	}
	for i := 1; i < len(attrs); i++ {
		t := findScopeWithAttribute(attrs[i].Name, &aliases[i], qry.Scope)
		if t != s {
			return false
		}
	}
	for i, attr := range attrs {
		attr.Name = aliases[i]
	}
	switch rel := s.Op.(type) {
	case *Relation:
		if rel.Cond != nil {
			rel.Cond = &extend.BinaryExtend{
				Op:    overload.And,
				Right: e,
				Left:  rel.Cond,
			}
		} else {
			rel.Cond = e
		}
	case *DerivedRelation:
		if rel.Cond != nil {
			rel.Cond = &extend.BinaryExtend{
				Op:    overload.And,
				Right: e,
				Left:  rel.Cond,
			}
		} else {
			rel.Cond = e
		}
	}
	return true
}

func findScopeWithAttribute(name string, alias *string, s *Scope) *Scope {
	switch op := s.Op.(type) {
	case *Join:
		for i := 0; i < len(s.Children); i++ {
			if rp := findScopeWithAttribute(name, alias, s.Children[i]); rp != nil {
				return rp
			}
		}
	case *Relation:
		if _, ok := s.Result.AttrsMap[name]; ok {
			*alias = name
			return s
		}
	case *DerivedRelation:
		if _, ok := s.Result.AttrsMap[name]; ok {
			*alias = name
			return s
		}
	case *Rename:
		for i := range op.Es {
			if op.As[i] == name {
				if e, ok := op.Es[i].(*extend.Attribute); ok {
					return findScopeWithAttribute(e.Name, alias, s.Children[0])
				}
			}
		}
	case *Projection:
		for i := range op.Es {
			if op.As[i] == name {
				if e, ok := op.Es[i].(*extend.Attribute); ok {
					return findScopeWithAttribute(e.Name, alias, s.Children[0])
				}
			}
		}
	}
	return nil
}

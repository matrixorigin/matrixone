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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (b *build) buildCrossJoin(ss *ScopeSet) (*Scope, error) {
	s := new(Scope)
	s.Op = &Join{Type: CROSS}
	{ // construct result
		s.Result.AttrsMap = make(map[string]*Attribute)
		mp, mq := make(map[string]int), make(map[string]string)
		for i := range ss.Scopes {
			rp := &ss.Scopes[i].Result
			for _, attr := range rp.Attrs {
				if j, ok := mp[attr]; ok { // remove ambiguously column name
					if j >= 0 {
						mp[attr] = -1
						if len(mq[attr]) > 0 {
							name := mq[attr] + "." + attr
							s.Result.Attrs[j] = name
							attrp := s.Result.AttrsMap[attr]
							attrp.Name = name
							delete(s.Result.AttrsMap, attr)
							s.Result.AttrsMap[name] = attrp
						}
					}
					name := ss.Scopes[i].Name + "." + attr
					if len(ss.Scopes[i].Name) == 0 {
						name = attr
					}
					s.Result.Attrs = append(s.Result.Attrs, name)
					s.Result.AttrsMap[name] = &Attribute{
						Name: name,
						Type: rp.AttrsMap[attr].Type,
					}
				} else {
					mq[attr] = ss.Scopes[i].Name
					mp[attr] = len(s.Result.Attrs)
					s.Result.Attrs = append(s.Result.Attrs, attr)
					s.Result.AttrsMap[attr] = &Attribute{
						Name: attr,
						Type: rp.AttrsMap[attr].Type,
					}
				}
			}
		}
	}
	s.Children = append(s.Children, ss.Scopes...)
	return s, nil
}

func (b *build) buildQualifiedJoin(joinType int, ss *ScopeSet) (*Scope, error) {
	var ts []types.Type
	var attrs, as []string

	nss := make([]*Scope, len(ss.Scopes))
	{
		ap := new(int) // alias generator
		b.initAliasGenerator(ap, ss.Conds)
		tm := make([]*Scope, 0, len(ss.Scopes))
		for i, s := range ss.Scopes {
			tm = append(tm[:0], ss.Scopes[:i]...)
			tm = append(tm, ss.Scopes[i+1:]...)
			as0 := make([]string, 0, len(s.Result.Attrs))
			ts0 := make([]types.Type, 0, len(s.Result.Attrs))
			for j := range s.Result.Attrs {
				alias, attr := b.generateAlias(i, ap, s.Result.Attrs[j], s, tm, ss.Conds)
				{
					as = append(as, attr)
					attrs = append(attrs, alias)
					ts = append(ts, s.Result.AttrsMap[s.Result.Attrs[j]].Type)
				}
				as0 = append(as0, alias)
				ts0 = append(ts0, s.Result.AttrsMap[s.Result.Attrs[j]].Type)
			}
			nss[i] = b.buildRename(s, s.Result.Attrs, as0, ts0)
		}
	}
	ss.Scopes = nss
	s, err := b.buildHyperGraph(joinType, ss)
	if err != nil {
		return nil, err
	}
	return b.buildRename(s, attrs, as, ts), nil
}

func (b *build) buildNaturalJoin(joinType int, ss *ScopeSet) (*Scope, error) {
	var attrs []string
	var ts []types.Type

	{
		mp := make(map[string]int)
		for i, s := range ss.Scopes {
			as := make([]string, 0, len(s.Result.Attrs))
			ts0 := make([]types.Type, 0, len(s.Result.Attrs))
			for _, attr := range s.Result.Attrs {
				if j, ok := mp[attr]; ok {
					as = append(as, strconv.Itoa(j))
				} else {
					mp[attr] = len(attrs)
					as = append(as, strconv.Itoa(len(attrs)))
					attrs = append(attrs, attr)
				}
				ts = append(ts, s.Result.AttrsMap[attr].Type)
				ts0 = append(ts0, s.Result.AttrsMap[attr].Type)
			}
			ss.Scopes[i] = b.buildRename(s, s.Result.Attrs, as, ts0)
		}
	}
	s, err := b.buildHyperGraph(joinType, ss)
	if err != nil {
		return nil, err
	}
	as := make([]string, len(attrs))
	{ // construct alias list
		for i := range attrs {
			as[i] = strconv.Itoa(i)
		}
	}
	return b.buildRename(s, as, attrs, ts), nil
}

func (b *build) buildHyperGraph(joinType int, ss *ScopeSet) (*Scope, error) {
	gp := new(Graph)
	{ // build hyper graph
		for _, s := range ss.Scopes {
			ep := &Edge{}
			for _, attr := range s.Result.Attrs {
				v, _ := strconv.Atoi(attr)
				ep.Vs = append(ep.Vs, v)
			}
			gp.Es = append(gp.Es, ep)
		}
	}
	if err := b.checkHyperGraph(gp, ss); err != nil {
		return nil, err
	}
	ness := []*EdgeSet{}
	{
		ess := buildEdgeSet(gp.Es)
		ness = b.decomposition(buildVertexSet(gp.Es), b.initVertexSet(gp.Es, ss.Scopes, ess), ess, ness, ss.Scopes)
	}
	return b.buildJoinTree(joinType, new(Scope), ness[0].I1, ness, ss.Scopes), nil
}

func (b *build) decomposition(vp, nvp *VertexSet, ess, ness []*EdgeSet, ss []*Scope) []*EdgeSet {
	var j int
	var w int

	j = -1
	for i, es := range ess {
		if nvp.Contains(es.I1) && !nvp.Contains(es.I2) {
			if j < 0 || es.W > w || ss[ess[j].I2].Rows() > ss[ess[i].I2].Rows() {
				j = i
				w = es.W
			}
		}
	}
	if j >= 0 {
		ness = append(ness, ess[j])
		nvp.Is = append(nvp.Is, ess[j].I2)
		nvp.Es = append(nvp.Es, ess[j].E2)
	}
	if len(vp.Es) != len(nvp.Es) {
		ness = b.decomposition(vp, nvp, ess, ness, ss)
	}
	return ness
}

func (b *build) buildJoinTree(joinType int, root *Scope, i int, ess []*EdgeSet, ss []*Scope) *Scope {
	op := &Join{Type: joinType}
	root.Children = append(root.Children, ss[i])
	for j := 0; j < len(ess); j++ {
		if ess[j].I1 != i {
			continue
		}
		es := ess[j]
		op.Vars = append(op.Vars, connectedVertexs(es.E1.Vs, es.E2.Vs))
		ess = append(ess[:j], ess[j+1:]...)
		b.buildPath(joinType, root, es, ess, ss)
		j--
	}
	{ // construct result
		root.Result.AttrsMap = make(map[string]*Attribute)
		for i := range root.Children {
			rp := &root.Children[i].Result
			for _, attr := range rp.Attrs {
				if _, ok := root.Result.AttrsMap[attr]; !ok {
					root.Result.Attrs = append(root.Result.Attrs, attr)
					root.Result.AttrsMap[attr] = &Attribute{
						Name: attr,
						Type: rp.AttrsMap[attr].Type,
					}
				}
			}
		}
	}
	{
		vars := make([][]int, len(root.Children))
		for i, s := range root.Children {
			if i == 0 {
				continue
			}
			for _, attr := range s.Result.Attrs {
				v, _ := strconv.Atoi(attr)
				for j := range op.Vars {
					for k := range op.Vars[j] {
						if v == op.Vars[j][k] {
							vars[i] = append(vars[i], v)
							goto End
						}
					}
				}
			End:
			}
		}
		op.Vars = vars
	}
	root.Op = op
	return root
}

func (b *build) buildPath(joinType int, root *Scope, es *EdgeSet, ess []*EdgeSet, ss []*Scope) {
	if isEndEdgeSet(es, ess) {
		root.Children = append(root.Children, ss[es.I2])
	} else {
		tm := make([]*EdgeSet, 0, len(ess))
		for j := range ess {
			if es.I2 == ess[j].I1 {
				tm = append(tm[:0], ess...)
				root.Children = append(root.Children, b.buildJoinTree(joinType, new(Scope), ess[j].I1, tm, ss))
			}
		}
	}
}

func (b *build) initVertexSet(es []*Edge, ss []*Scope, ess []*EdgeSet) *VertexSet {
	var j int
	var cnt int
	var rows int64

	vp := new(VertexSet)
	for i := range es {
		if j < 0 || getEdgesCount(i, ess) > cnt || ss[i].Rows() > rows {
			j = i
			rows = ss[i].Rows()
			cnt = getEdgesCount(i, ess)
		}
	}
	vp.Is = append(vp.Is, j)
	vp.Es = append(vp.Es, es[j])
	return vp
}

func (b *build) checkHyperGraph(gp *Graph, ss *ScopeSet) error {
	if existIndependentEdge(gp) {
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("isolated relation not support now"))
	}
	if !isEmptyGraph(pruneGraph(cloneGraph(gp))) { // check whether is a cyclic graph
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("cyclic join not support now"))
	}
	return nil
}

func (b *build) buildJoinCond(expr tree.Expr, ss *ScopeSet) error {
	switch e := expr.(type) {
	case *tree.AndExpr:
		if err := b.buildJoinCond(e.Left, ss); err != nil {
			return err
		}
		return b.buildJoinCond(e.Right, ss)
	case *tree.ComparisonExpr:
		if e.Op != tree.EQUAL {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", tree.String(expr, dialect.MYSQL)))
		}
		left, ok := e.Left.(*tree.UnresolvedName)
		if !ok {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", tree.String(expr, dialect.MYSQL)))
		}
		right, ok := e.Right.(*tree.UnresolvedName)
		if !ok {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", tree.String(expr, dialect.MYSQL)))
		}
		li, lname, err := b.getJoinAttribute(left, ss)
		if err != nil {
			return err
		}
		ri, rname, err := b.getJoinAttribute(right, ss)
		if err != nil {
			return err
		}
		if ss.Scopes[li].Result.AttrsMap[lname].Type.Oid != ss.Scopes[ri].Result.AttrsMap[rname].Type.Oid {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v': type mismatch", tree.String(expr, dialect.MYSQL)))
		}
		ss.Conds = append(ss.Conds, &JoinCondition{
			R:     li,
			S:     ri,
			Rattr: lname,
			Sattr: rname,
		})
		return nil
	case *tree.ParenExpr:
		return b.buildJoinCond(e.Expr, ss)
	}
	return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", tree.String(expr, dialect.MYSQL)))
}

func (b *build) getJoinAttribute(expr *tree.UnresolvedName, ss *ScopeSet) (int, string, error) {
	switch expr.NumParts {
	case 1:
		i, err := b.findAttribute(expr.Parts[0], ss)
		if err != nil {
			return -1, "", err
		}
		return i, expr.Parts[0], nil
	case 2:
		i, err := b.findAttributeWithSchema(expr.Parts[1], expr.Parts[0], ss)
		if err != nil {
			if i, err = b.findAttribute(expr.Parts[1]+"."+expr.Parts[0], ss); err == nil {
				return i, expr.Parts[1] + "." + expr.Parts[0], nil
			}
			if i, err = b.findAttribute(expr.Parts[0], ss); err != nil {
				return -1, "", err
			}
			return i, expr.Parts[0], nil
		}
		return i, expr.Parts[0], nil
	default:
		return -1, "", errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Unsupport column '%s' in on clause", tree.String(expr, dialect.MYSQL)))
	}
}

func (b *build) findAttribute(name string, ss *ScopeSet) (int, error) {
	var j int
	var cnt int

	for i, s := range ss.Scopes {
		if _, ok := s.Result.AttrsMap[name]; ok {
			cnt++
			j = i
		}
	}
	switch cnt {
	case 0:
		return -1, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Unknown column '%s'", name))
	case 1:
		return j, nil
	default:
		return -1, errors.New(errno.DuplicateColumn, fmt.Sprintf("Column '%s' in on clause is ambiguous", name))
	}
}

func (b *build) findAttributeWithSchema(schema, name string, ss *ScopeSet) (int, error) {
	var j int
	var cnt int

	for i, s := range ss.Scopes {
		if s.Name != schema {
			continue
		}
		if _, ok := s.Result.AttrsMap[name]; ok {
			cnt++
			j = i
		}
	}
	switch cnt {
	case 0:
		return -1, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Unknown column '%s' in 'from clause'", name))
	case 1:
		return j, nil
	default:
		return -1, errors.New(errno.DuplicateColumn, fmt.Sprintf("Column '%s' in on clause is ambiguous", name))
	}
}

func (b *build) initAliasGenerator(ap *int, conds []*JoinCondition) {
	var alias int

	for _, cond := range conds {
		cond.Alias = -1
	}
	for i, cond := range conds {
		if cond.Alias >= 0 {
			continue
		}
		cond.Alias = alias
		alias++
		for j := i + 1; j < len(conds); j++ {
			switch {
			case cond.R == conds[j].R && cond.Rattr == conds[j].Rattr:
				conds[j].Alias = cond.Alias
			case cond.R == conds[j].S && cond.Rattr == conds[j].Sattr:
				conds[j].Alias = cond.Alias
			case cond.S == conds[j].S && cond.Sattr == conds[j].Sattr:
				conds[j].Alias = cond.Alias
			case cond.S == conds[j].R && cond.Sattr == conds[j].Rattr:
				conds[j].Alias = cond.Alias
			}
		}
	}
	*ap = alias
}

func (b *build) generateAlias(i int, ap *int, attr string, s *Scope, ss []*Scope, conds []*JoinCondition) (string, string) {
	for _, cond := range conds {
		if (cond.R == i && cond.Rattr == attr) || (cond.S == i && cond.Sattr == attr) {
			for k := range ss {
				if _, ok := ss[k].Result.AttrsMap[attr]; ok {
					return strconv.Itoa(cond.Alias), traceAttribute(s, attr)
				}
			}
			return strconv.Itoa(cond.Alias), attr
		}
	}
	for j := range ss {
		if _, ok := ss[j].Result.AttrsMap[attr]; ok {
			(*ap)++
			return strconv.Itoa(*ap), traceAttribute(s, attr)
		}
	}
	(*ap)++
	return strconv.Itoa(*ap), attr
}

func traceAttribute(s *Scope, attr string) string {
	if len(s.Name) != 0 {
		return s.Name + "." + attr
	}
	for i := range s.Children {
		if _, ok := s.Children[i].Result.AttrsMap[attr]; ok {
			return traceAttribute(s.Children[i], attr)
		}
	}
	return attr
}

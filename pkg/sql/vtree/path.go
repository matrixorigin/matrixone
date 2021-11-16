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

package vtree

import (
	"matrixone/pkg/sql/ftree"
	"matrixone/pkg/sql/util"
)

func buildPath(isRoot bool, fns []*ftree.FNode, fvars []string, fvarsMap map[string]uint8) ([]*node, []string) {
	p := new(path)
	rn := pathRelationName(fns)
	p.freeVarsMap = make(map[string]uint8)
	for _, fv := range fvars {
		tbl, name := util.SplitTableAndColumn(fv)
		if len(tbl) > 0 && tbl == rn {
			p.freeVarsMap[name] = 0
			p.freeVars = append(p.freeVars, name)
		} else {
			p.freeVarsMap[fv] = 0
			p.freeVars = append(p.freeVars, fv)
		}
	}
	if fv, ok := fns[0].Root.(*ftree.Variable); ok && !isRoot {
		if _, ok := p.freeVarsMap[fv.Name]; !ok {
			p.freeVarsMap[fv.Name] = 0
			p.freeVars = append(p.freeVars, fv.Name)
		}
	}
	return p.buildViews(fns, fvars, fvarsMap)
}

func (p *path) buildViews(fns []*ftree.FNode, fvars []string, fvarsMap map[string]uint8) ([]*node, []string) {
	var n *node
	var ns []*node
	var rns []string
	var tms []string
	var children []*node

	fn := fns[0]
	if len(fn.Children) > 0 {
		name := fn.Root.(*ftree.Variable).Name
		if _, ok := p.freeVarsMap[name]; !ok {
			p.freeVarsMap[name] = 0
			p.freeVars = append(p.freeVars, name)
		}
		children, tms = buildPath(false, fn.Children, fvars, fvarsMap)
		rns = append(tms, rns...)
	}
	if len(fns) > 1 {
		ns, tms = p.buildViews(fns[1:], fvars, fvarsMap)
		rns = append(tms, rns...)
	}
	if frel, ok := fn.Root.(*ftree.Relation); ok {
		n, tms = p.buildViewByRelation(frel, fvars, fvarsMap)
		rns = append(rns, tms...)
		n.children = children
	} else {
		if n = p.buildViewByVariable(fn.Root.(*ftree.Variable), rns, fvars, fvarsMap); n == nil {
			return ns, rns
		}
		n.children = children
	}
	return append([]*node{n}, ns...), rns
}

func (p *path) buildViewByRelation(frel *ftree.Relation, fvars []string, fvarsMap map[string]uint8) (*node, []string) {
	n := new(node)
	n.content = frel
	n.rns = []string{frel.Rel.Alias}
	n.freeVarsMap = make(map[string]uint8)
	for i := range frel.Vars {
		if _, ok := p.freeVarsMap[frel.Vars[i]]; ok {
			n.freeVarsMap[frel.Vars[i]] = 0
			n.freeVars = append(n.freeVars, frel.Vars[i])
		} else {
			n.boundVars = append(n.boundVars, frel.Vars[i])
		}
	}
	return n, n.rns
}

func (p *path) buildViewByVariable(fvar *ftree.Variable, rns []string, fvars []string, fvarsMap map[string]uint8) *node {
	if _, ok := p.freeVarsMap[fvar.Name]; ok {
		n := new(node)
		n.content = fvar
		n.rns = append(n.rns, rns...)
		n.freeVarsMap = make(map[string]uint8)
		n.freeVarsMap[fvar.Name] = 0
		n.freeVars = append(n.freeVars, fvar.Name)
		return n
	}
	return nil
}

func pathRelationName(fns []*ftree.FNode) string {
	fn := fns[len(fns)-1]
	return fn.Root.(*ftree.Relation).Rel.Alias
}

// prune redundant node
func pruneNodes(depth int, ns []*node) []*node {
	for i := 0; i < len(ns); i++ {
		if (depth == 0 || i > 0) && i != len(ns)-1 && len(ns[i].children) == 0 {
			ns = append(ns[:i], ns[i+1:]...)
			i--
			continue
		}
		if len(ns[i].children) > 0 {
			ns[i].children = pruneNodes(depth+1, ns[i].children)
		}
	}
	return ns
}

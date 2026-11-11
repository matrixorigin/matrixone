// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"cmp"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// A column in a finalized scan schema, independent of projected output slots.
type localDomainColumn struct {
	scan int32
	col  int32
	typ  int32
}

type localDomainProof struct {
	query   *planpb.Query
	tree    *deepRuntimeFilterTree
	parents map[localDomainColumn]localDomainColumn
	builds  map[localDomainColumn]bool
	work    int
}

func localDomainInteger(typ int32) bool {
	switch types.T(typ) {
	case types.T_int16, types.T_int32, types.T_int64, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	}
	return false
}

func (p *localDomainProof) root(col localDomainColumn) localDomainColumn {
	parent, ok := p.parents[col]
	if !ok {
		p.parents[col] = col
		return col
	}
	if parent != col {
		p.parents[col] = p.root(parent)
	}
	return p.parents[col]
}

func (p *localDomainProof) output(nodeID, pos int32, depth int) (localDomainColumn, bool) {
	p.work++
	if p.work > 4096 {
		return localDomainColumn{}, false
	}
	if depth >= 128 || nodeID < 0 || int(nodeID) >= len(p.query.Nodes) {
		return localDomainColumn{}, false
	}
	n := p.query.Nodes[nodeID]
	if n == nil || n.Limit != nil || n.Offset != nil || pos < 0 || int(pos) >= len(n.ProjectList) {
		return localDomainColumn{}, false
	}
	e := n.ProjectList[pos]
	if e == nil || e.GetCol() == nil || !localDomainInteger(e.Typ.Id) {
		return localDomainColumn{}, false
	}
	c := e.GetCol()
	if n.NodeType == planpb.Node_TABLE_SCAN {
		if n.TableDef == nil || c.RelPos != 0 || c.ColPos < 0 || int(c.ColPos) >= len(n.TableDef.Cols) ||
			n.TableDef.Cols[c.ColPos].Typ.Id != e.Typ.Id {
			return localDomainColumn{}, false
		}
		return localDomainColumn{scan: nodeID, col: c.ColPos, typ: e.Typ.Id}, true
	}
	if n.NodeType != planpb.Node_JOIN && n.NodeType != planpb.Node_PROJECT && n.NodeType != planpb.Node_FILTER {
		return localDomainColumn{}, false
	}
	if !areTruncationSafePredicates(n.FilterList) {
		return localDomainColumn{}, false
	}
	if n.NodeType == planpb.Node_PROJECT {
		for _, projected := range n.ProjectList {
			if projected == nil || projected.GetCol() == nil {
				return localDomainColumn{}, false
			}
		}
	}
	if c.RelPos < 0 || int(c.RelPos) >= len(n.Children) {
		return localDomainColumn{}, false
	}
	column, ok := p.output(n.Children[c.RelPos], c.ColPos, depth+1)
	return column, ok && column.typ == e.Typ.Id
}

func newLocalDomainProof(q *planpb.Query) *localDomainProof {
	if q == nil || len(q.Steps) != 1 || len(q.SelectLockReferences) != 0 {
		return nil
	}
	if !slices.ContainsFunc(q.Nodes, func(n *planpb.Node) bool { return n != nil && n.NodeType == planpb.Node_JOIN }) {
		return nil
	}
	tree := newDeepRuntimeFilterTree(q, q.Steps[0])
	if tree == nil {
		return nil
	}
	p := &localDomainProof{query: q, tree: tree, parents: make(map[localDomainColumn]localDomainColumn), builds: make(map[localDomainColumn]bool)}
	for _, id := range tree.order {
		n := q.Nodes[id]
		// A domain can remove rows before these physical projections run.
		// Keep fallible/volatile computed projections on ordinary execution;
		// key lineage alone cannot prove their evaluation may be skipped.
		if n.NodeType == planpb.Node_TABLE_SCAN || n.NodeType == planpb.Node_JOIN || n.NodeType == planpb.Node_PROJECT {
			for _, projected := range n.ProjectList {
				if projected == nil || projected.GetCol() == nil {
					return nil
				}
			}
		}
		if n.NodeType != planpb.Node_JOIN {
			continue
		}
		if !areTruncationSafePredicates(n.OnList) || !areTruncationSafePredicates(n.FilterList) {
			return nil
		}
		for _, expr := range n.OnList {
			p.work++
			if p.work > 4096 {
				return nil
			}
			fn := expr.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
				continue
			}
			left, right := fn.Args[0], fn.Args[1]
			if left.GetCol() == nil || right.GetCol() == nil || left.Typ.Id != right.Typ.Id || !localDomainInteger(left.Typ.Id) {
				continue
			}
			lc, rc := left.GetCol(), right.GetCol()
			if lc.RelPos == 1 && rc.RelPos == 0 {
				lc, rc = rc, lc
			}
			if lc.RelPos != 0 || rc.RelPos != 1 {
				continue
			}
			a, aok := p.output(n.Children[0], lc.ColPos, 0)
			b, bok := p.output(n.Children[1], rc.ColPos, 0)
			if !aok || !bok || a.typ != b.typ {
				continue
			}
			p.parents[p.root(b)] = p.root(a)
			p.builds[b] = true
		}
	}
	return p
}

func (p *localDomainProof) source(col localDomainColumn) bool {
	n := p.query.Nodes[col.scan]
	if !p.builds[col] || n.TableDef == nil || n.TableDef.Cols[col.col].Name == "" || n.TableDef.Pkey == nil || len(n.TableDef.Pkey.Names) != 1 ||
		n.TableDef.Pkey.PkeyColName != n.TableDef.Cols[col.col].Name || len(n.FilterList) == 0 ||
		len(n.RuntimeFilterProbeList) != 0 || n.Stats == nil {
		return false
	}
	for _, expr := range n.FilterList {
		if !localDomainSourcePredicate(expr) {
			return false
		}
	}
	rows, total := n.Stats.Outcnt, n.Stats.TableCnt
	return rows > 0 && rows < 1_000_000_001 && total > 0 && rows < total &&
		!math.IsNaN(rows) && !math.IsInf(rows, 0) && !math.IsNaN(total) && !math.IsInf(total, 0)
}

// Source predicates execute in full, so they may report their normal errors,
// but must not wait on another query branch or external user code. LIKE is a
// bounded built-in source predicate, not a rule for skipping it on a probe.
func localDomainSourcePredicate(expr *planpb.Expr) bool {
	if isTruncationSafePredicateExpr(expr) {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	switch id {
	case function.LIKE:
		return len(fn.Args) == 2 && fn.Args[0].GetCol() != nil && fn.Args[1].GetLit() != nil
	case function.AND, function.OR:
		return len(fn.Args) == 2 && localDomainSourcePredicate(fn.Args[0]) && localDomainSourcePredicate(fn.Args[1])
	case function.NOT:
		return len(fn.Args) == 1 && localDomainSourcePredicate(fn.Args[0])
	}
	return false
}

// LocalRuntimeFilterDomains derives optional hints from finalized structural
// lineage. ndv supplies the planner's existing per-column statistics; nil is
// used only to revalidate semantic eligibility during physical compilation.
func LocalRuntimeFilterDomains(q *planpb.Query, ndv func(*planpb.Node, string) float64) []*planpb.LocalRuntimeFilterDomain {
	p := newLocalDomainProof(q)
	if p == nil {
		return nil
	}
	classes := make(map[localDomainColumn][]localDomainColumn)
	for col := range p.parents {
		root := p.root(col)
		classes[root] = append(classes[root], col)
	}
	var domains []*planpb.LocalRuntimeFilterDomain
	for _, columns := range classes {
		slices.SortFunc(columns, func(a, b localDomainColumn) int {
			if a.scan != b.scan {
				return int(a.scan - b.scan)
			}
			return int(a.col - b.col)
		})
		var sources []localDomainColumn
		for _, col := range columns {
			if p.source(col) {
				sources = append(sources, col)
			}
		}
		if len(sources) == 0 {
			continue
		}
		slices.SortStableFunc(sources, func(a, b localDomainColumn) int {
			return cmp.Compare(q.Nodes[a.scan].Stats.Outcnt, q.Nodes[b.scan].Stats.Outcnt)
		})
		source := sources[0]
		sourceNode := q.Nodes[source.scan]
		unsafe := false
		for _, id := range p.tree.order {
			if id != source.scan && !areTruncationSafePredicates(q.Nodes[id].FilterList) {
				unsafe = true
				break
			}
		}
		if unsafe {
			continue
		}
		domain := &planpb.LocalRuntimeFilterDomain{SourceScanNodeId: source.scan,
			SourceColumnName: sourceNode.TableDef.Cols[source.col].Name, KeyType: source.typ}
		for _, col := range columns {
			n := q.Nodes[col.scan]
			if col.scan == source.scan || n.Stats == nil || n.Stats.Outcnt <= 0 ||
				sourceNode.Stats.Outcnt > n.Stats.Outcnt*.3 || len(n.FilterList) != 0 ||
				len(n.BlockFilterList) != 0 || len(n.RuntimeFilterProbeList) != 0 {
				// This eager-scan stage cannot avoid an existing reader/static
				// predicate's work. Preserve that path and its overlap; combine
				// membership earlier only in the separate reader integration.
				continue
			}
			name := n.TableDef.Cols[col.col].Name
			probeNDV := float64(0)
			if ndv != nil {
				probeNDV = ndv(n, name)
				if !ValidLocalDomainNDV(probeNDV) {
					continue
				}
			}
			domain.Targets = append(domain.Targets, &planpb.LocalRuntimeFilterTarget{ScanNodeId: col.scan, ColumnName: name, ProbeNdv: probeNDV})
		}
		if len(domain.Targets) != 0 {
			domains = append(domains, domain)
		}
	}
	// Sources remain independent: none may acquire another optional domain wait.
	sources := make(map[int32]bool)
	for _, d := range domains {
		sources[d.SourceScanNodeId] = true
	}
	filtered := domains[:0]
	for _, d := range domains {
		d.Targets = slices.DeleteFunc(d.Targets, func(t *planpb.LocalRuntimeFilterTarget) bool { return sources[t.ScanNodeId] })
		if len(d.Targets) != 0 {
			filtered = append(filtered, d)
		}
	}
	slices.SortFunc(filtered, func(a, b *planpb.LocalRuntimeFilterDomain) int {
		if a.SourceScanNodeId != b.SourceScanNodeId {
			return int(a.SourceScanNodeId - b.SourceScanNodeId)
		}
		return cmp.Compare(a.SourceColumnName, b.SourceColumnName)
	})
	return filtered
}

// ValidLocalRuntimeFilterDomains returns only annotated subsets of independently
// rederived semantic domains. An annotation never authorizes a new predicate.
func ValidLocalRuntimeFilterDomains(q *planpb.Query) []*planpb.LocalRuntimeFilterDomain {
	if q == nil || len(q.LocalRuntimeFilterDomains) == 0 || len(q.LocalRuntimeFilterDomains) > 4096 {
		return nil
	}
	proved := LocalRuntimeFilterDomains(q, nil)
	var result []*planpb.LocalRuntimeFilterDomain
	for _, hint := range q.LocalRuntimeFilterDomains {
		if hint == nil || len(hint.Targets) == 0 || len(hint.Targets) > 4096 {
			continue
		}
		for _, proof := range proved {
			if hint.SourceScanNodeId != proof.SourceScanNodeId || hint.SourceColumnName != proof.SourceColumnName || hint.KeyType != proof.KeyType {
				continue
			}
			valid := true
			seen := make(map[int32]bool)
			for _, target := range hint.Targets {
				if target == nil || !ValidLocalDomainNDV(target.ProbeNdv) || seen[target.ScanNodeId] || !slices.ContainsFunc(proof.Targets, func(p *planpb.LocalRuntimeFilterTarget) bool {
					return p.ScanNodeId == target.ScanNodeId && p.ColumnName == target.ColumnName
				}) {
					valid = false
					break
				}
				seen[target.ScanNodeId] = true
			}
			if valid {
				result = append(result, hint)
			}
			break
		}
	}
	return result
}

// ValidLocalDomainNDV admits a cost hint only. Equality/source completion own
// the row-rejection proof; a stale NDV can change profitability, not results.
func ValidLocalDomainNDV(ndv float64) bool {
	return ndv > 1 && !math.IsNaN(ndv) && !math.IsInf(ndv, 0)
}

// pruneUsedNodes owns the final node-numbering change. Resolve each old endpoint
// through node identity, then re-prove the annotation against the compact tree.
// A removed endpoint cannot accidentally bind to another node that reused its ID.
func remapLocalRuntimeFilterDomains(q *planpb.Query, oldNodes []*planpb.Node) {
	if len(q.LocalRuntimeFilterDomains) == 0 {
		return
	}
	if len(q.LocalRuntimeFilterDomains) > 4096 {
		q.LocalRuntimeFilterDomains = nil
		return
	}
	newID := func(old int32) (int32, bool) {
		if old < 0 || int(old) >= len(oldNodes) || oldNodes[old] == nil {
			return 0, false
		}
		node := oldNodes[old]
		id := node.NodeId
		return id, id >= 0 && int(id) < len(q.Nodes) && q.Nodes[id] == node && node.NodeType == planpb.Node_TABLE_SCAN
	}
	remapped := make([]*planpb.LocalRuntimeFilterDomain, 0, len(q.LocalRuntimeFilterDomains))
	for _, hint := range q.LocalRuntimeFilterDomains {
		if hint == nil || len(hint.Targets) > 4096 {
			continue
		}
		source, ok := newID(hint.SourceScanNodeId)
		if !ok {
			continue
		}
		domain := &planpb.LocalRuntimeFilterDomain{SourceScanNodeId: source,
			SourceColumnName: hint.SourceColumnName, KeyType: hint.KeyType}
		for _, target := range hint.Targets {
			if target == nil {
				continue
			}
			if id, live := newID(target.ScanNodeId); live {
				domain.Targets = append(domain.Targets, &planpb.LocalRuntimeFilterTarget{
					ScanNodeId: id, ColumnName: target.ColumnName, ProbeNdv: target.ProbeNdv,
				})
			}
		}
		if len(domain.Targets) != 0 {
			remapped = append(remapped, domain)
		}
	}
	q.LocalRuntimeFilterDomains = remapped
	q.LocalRuntimeFilterDomains = ValidLocalRuntimeFilterDomains(q)
}

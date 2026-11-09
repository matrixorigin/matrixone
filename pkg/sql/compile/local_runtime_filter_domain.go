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

package compile

import (
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

type localDomainScan struct {
	scope  *Scope
	scan   *table_scan.TableScan
	worker int
}

func (c *Compile) bindLocalRuntimeFilterDomains(q *plan.Query, roots []*Scope) {
	if c.isPrepare || c.IsTpQuery() || len(q.LocalRuntimeFilterDomains) == 0 ||
		len(c.cnList) != 1 || c.cnList[0].Addr != c.addr || c.proc == nil || c.proc.GetMessageBoard() == nil {
		return
	}
	domains := plan2.ValidLocalRuntimeFilterDomains(q)
	if len(domains) == 0 {
		return
	}
	scans := make(map[int32][]localDomainScan)
	visited := make(map[*Scope]bool)
	safe := true
	var walk func(*Scope)
	walk = func(s *Scope) {
		if s == nil || visited[s] {
			return
		}
		visited[s] = true
		if s.IsRemote || s.LazyPreScopes || len(s.lazyRemoteFragmentCounts) != 0 ||
			(s.Magic != Normal && s.Magic != Merge && s.Magic != Remote) ||
			(s.Magic == Normal && len(s.PreScopes) != 0) ||
			(s.NodeInfo.Addr != "" && s.NodeInfo.Addr != c.addr) || s.Proc == nil ||
			s.Proc.GetMessageBoard() != c.proc.GetMessageBoard() {
			safe = false
		}
		_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if scan, ok := op.(*table_scan.TableScan); ok {
				workers := 1
				if s.isTableScan() {
					workers = max(1, s.NodeInfo.Mcpu)
				}
				id := int32(scan.GetIdx())
				scans[id] = append(scans[id], localDomainScan{scope: s, scan: scan, worker: workers})
			}
			return nil
		})
		for _, child := range s.PreScopes {
			walk(child)
		}
	}
	for _, root := range roots {
		walk(root)
	}
	if !safe {
		return
	}
	var reserved uint64
	for _, descriptor := range domains {
		source := scans[descriptor.SourceScanNodeId]
		if len(source) != 1 || len(source[0].scope.PreScopes) != 0 || source[0].scope.DataSource == nil ||
			len(source[0].scope.DataSource.RuntimeFilterSpecs) != 0 || !independentLocalDomainSource(source[0].scope) {
			continue
		}
		if !localDomainScanColumn(q, descriptor.SourceScanNodeId, descriptor.SourceColumnName, descriptor.KeyType) {
			continue
		}
		var targets []localDomainScan
		probes := 0
		maxProbeNDV := float64(0)
		valid := true
		for _, target := range descriptor.Targets {
			maxProbeNDV = max(maxProbeNDV, target.ProbeNdv)
			instances := scans[target.ScanNodeId]
			if len(instances) != 1 || !localDomainScanColumn(q, target.ScanNodeId, target.ColumnName, descriptor.KeyType) {
				valid = false
				break
			}
			if !slices.ContainsFunc(targets, func(t localDomainScan) bool { return t.scan == instances[0].scan }) {
				targets = append(targets, instances[0])
				probes += instances[0].worker
			}
		}
		if !valid || len(targets) == 0 {
			continue
		}
		rows := q.Nodes[descriptor.SourceScanNodeId].Stats.Outcnt
		domain, peak := runtimefilter.NewLocalDomain(types.T(descriptor.KeyType), int64(math.Ceil(rows)), source[0].worker, probes, maxProbeNDV)
		if domain == nil || peak > runtimefilter.LocalDomainQueryCap-reserved {
			continue
		}
		reserved += peak
		c.localRuntimeDomains = append(c.localRuntimeDomains, domain)
		source[0].scan.LocalDomainBuilds = append(source[0].scan.LocalDomainBuilds, runtimefilter.LocalDomainBinding{
			Domain: domain, Column: descriptor.SourceColumnName,
		})
		for _, target := range descriptor.Targets {
			scan := scans[target.ScanNodeId][0].scan
			scan.LocalDomainProbes = append(scan.LocalDomainProbes, runtimefilter.LocalDomainBinding{Domain: domain, Column: target.ColumnName, ProbeNDV: target.ProbeNdv})
		}
	}
}

func independentLocalDomainSource(s *Scope) bool {
	safe := true
	_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
		switch op.OpType() {
		case vm.TableScan, vm.Projection, vm.HashBuild:
		case vm.Shuffle:
			if sh, ok := op.(*shuffle.Shuffle); !ok || sh.RuntimeFilterSpec != nil {
				safe = false
			}
		default:
			safe = false
		}
		return nil
	})
	return safe
}

func localDomainScanColumn(q *plan.Query, id int32, name string, typ int32) bool {
	if id < 0 || int(id) >= len(q.Nodes) || q.Nodes[id] == nil || q.Nodes[id].TableDef == nil {
		return false
	}
	return slices.ContainsFunc(q.Nodes[id].TableDef.Cols, func(col *plan.ColDef) bool {
		return col != nil && col.Name == name && col.Typ.Id == typ
	})
}

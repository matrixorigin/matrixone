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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
)

func (c *Compile) newLocalShuffleFilter(node, left, right *plan.Node, probes, builds []*Scope) *runtimefilter.LocalShuffleFilter {
	if c.isPrepare || c.IsTpQuery() || len(c.cnList) != 1 || c.cnList[0].Addr != c.addr ||
		len(probes) != 1 || len(builds) != 1 || probes[0] == nil || builds[0] == nil ||
		!sameExecutionNode(probes[0].NodeInfo, c.cnList[0]) || !sameExecutionNode(builds[0].NodeInfo, c.cnList[0]) ||
		len(node.RuntimeFilterBuildList) != 1 || node.IsRightJoin ||
		(node.JoinType != plan.Node_INNER && node.JoinType != plan.Node_SEMI) || left.Stats == nil || right.Stats == nil {
		return nil
	}
	spec := node.RuntimeFilterBuildList[0]
	if spec == nil || spec.BuildExpr == nil || spec.ProbeType == nil || spec.BuildExpr.GetCol() == nil ||
		spec.BuildExpr.Typ.Id != spec.ProbeType.Id || len(node.OnList) != 1 {
		return nil
	}
	rows := right.Stats.Outcnt
	if rows <= 0 || rows >= 1_000_000_001 || math.IsNaN(rows) || math.IsInf(rows, 0) ||
		left.Stats.Outcnt <= 0 || rows > left.Stats.Outcnt*.3 {
		return nil
	}
	filter, peak := runtimefilter.NewLocalShuffleFilter(spec.Tag, types.T(spec.BuildExpr.Typ.Id), int64(math.Ceil(rows)), builds[0].NodeInfo.Mcpu)
	if filter == nil || peak > runtimefilter.LocalShuffleFilterQueryCap-c.localShuffleFilterBytes {
		return nil
	}
	c.localShuffleFilterBytes += peak
	c.localShuffleFilters = append(c.localShuffleFilters, filter)
	return filter
}

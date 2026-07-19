// Copyright 2026 Matrix Origin
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

package compile

import (
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// runtimeFilterTopology is built after physical scopes have been constructed
// and before any local or remote pipeline starts. Runtime-filter messages are
// current-CN only. Phase-1 right-SINGLE placement therefore requires one
// producer for the whole tag and every blocking scan consumer on that same
// execution CN.
type runtimeFilterTopology struct {
	producers map[int32][]*Scope
	consumers map[int32][]*Scope
}

func rightSingleLocalRuntimeFilterTags(qry *plan.Query, compiledNodeIDs []int32) []int32 {
	if qry == nil || len(compiledNodeIDs) == 0 {
		return nil
	}

	var tags []int32
	for _, nodeID := range compiledNodeIDs {
		if nodeID < 0 || int(nodeID) >= len(qry.Nodes) {
			continue
		}
		node := qry.Nodes[nodeID]
		if node == nil || node.NodeType != plan.Node_JOIN ||
			node.JoinType != plan.Node_SINGLE || !node.IsRightJoin {
			continue
		}
		if node.Stats != nil && node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle {
			continue
		}
		for _, spec := range node.RuntimeFilterBuildList {
			// Phase-1 right-SINGLE uses exact IN.  Shuffle PASS specs have no
			// expression and do not participate in this local delivery contract.
			if spec != nil && spec.Tag > 0 && spec.Expr != nil {
				tags = append(tags, spec.Tag)
			}
		}
	}
	if len(tags) < 2 {
		return tags
	}

	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	unique := tags[:1]
	for _, tag := range tags[1:] {
		if tag != unique[len(unique)-1] {
			unique = append(unique, tag)
		}
	}
	return unique
}

func collectRuntimeFilterTopology(roots []*Scope, tags []int32) runtimeFilterTopology {
	topology := runtimeFilterTopology{
		producers: make(map[int32][]*Scope, len(tags)),
		consumers: make(map[int32][]*Scope, len(tags)),
	}
	if len(tags) == 0 {
		return topology
	}

	tracked := make(map[int32]struct{}, len(tags))
	for _, tag := range tags {
		tracked[tag] = struct{}{}
	}

	visited := make(map[*Scope]struct{})
	var walkScope func(*Scope)
	walkScope = func(scope *Scope) {
		if scope == nil {
			return
		}
		if _, ok := visited[scope]; ok {
			return
		}
		visited[scope] = struct{}{}

		if scope.DataSource != nil {
			seenConsumerTags := make(map[int32]struct{})
			addConsumerSpecs := func(specs []*plan.RuntimeFilterSpec) {
				for _, spec := range specs {
					if spec == nil {
						continue
					}
					if _, ok := tracked[spec.Tag]; !ok {
						continue
					}
					if _, duplicate := seenConsumerTags[spec.Tag]; duplicate {
						continue
					}
					seenConsumerTags[spec.Tag] = struct{}{}
					topology.consumers[spec.Tag] = append(topology.consumers[spec.Tag], scope)
				}
			}
			addConsumerSpecs(scope.DataSource.RuntimeFilterSpecs)
			if scope.DataSource.node != nil {
				addConsumerSpecs(scope.DataSource.node.RuntimeFilterProbeList)
			}
		}

		_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			build, ok := op.(*hashbuild.HashBuild)
			if !ok || build.RuntimeFilterSpec == nil {
				return nil
			}
			tag := build.RuntimeFilterSpec.Tag
			if _, ok = tracked[tag]; ok {
				topology.producers[tag] = append(topology.producers[tag], scope)
			}
			return nil
		})

		for _, preScope := range scope.PreScopes {
			walkScope(preScope)
		}
	}

	for _, root := range roots {
		walkScope(root)
	}
	return topology
}

func validateRightSingleRuntimeFilterTopology(qry *plan.Query, compiledNodeIDs []int32, roots []*Scope) error {
	tags := rightSingleLocalRuntimeFilterTags(qry, compiledNodeIDs)
	if len(tags) == 0 {
		return nil
	}

	topology := collectRuntimeFilterTopology(roots, tags)
	for _, tag := range tags {
		producers := topology.producers[tag]
		consumers := topology.consumers[tag]
		if len(consumers) == 0 {
			return moerr.NewInternalErrorNoCtxf(
				"invalid local runtime-filter topology: tag %d has no scan consumer", tag)
		}
		if len(producers) == 0 {
			return moerr.NewInternalErrorNoCtxf(
				"invalid local runtime-filter topology: tag %d has no producer", tag)
		}
		if len(producers) != 1 {
			return moerr.NewInternalErrorNoCtxf(
				"invalid local runtime-filter topology: tag %d has %d producers; expected exactly one colocated producer: %s",
				tag, len(producers), runtimeFilterScopeAddresses(producers))
		}

		producer := producers[0]
		// Physical scan scopes are expanded into NodeInfo.Mcpu copies immediately
		// before execution, and HashBuild (including its runtime-filter tag) is
		// duplicated with them. Count that pending expansion as multiple producers
		// instead of accepting the pre-expansion scope graph.
		if producer.NodeInfo.Mcpu > 1 {
			return moerr.NewInternalErrorNoCtxf(
				"invalid local runtime-filter topology: tag %d producer %s has DOP %d; expected exactly one producer",
				tag, runtimeFilterScopeAddress(producer), producer.NodeInfo.Mcpu)
		}
		for _, consumer := range consumers {
			if !sameRuntimeFilterExecutionNode(consumer.NodeInfo, producer.NodeInfo) {
				return moerr.NewInternalErrorNoCtxf(
					"invalid local runtime-filter topology: tag %d consumer %s cannot reach colocated producer %s",
					tag,
					runtimeFilterScopeAddress(consumer),
					runtimeFilterScopeAddress(producer))
			}
		}
	}
	return nil
}

func sameRuntimeFilterExecutionNode(left, right engine.Node) bool {
	leftUnspecified := left.Id == "" && left.Addr == ""
	rightUnspecified := right.Id == "" && right.Addr == ""
	if leftUnspecified || rightUnspecified {
		// Standalone scopes may omit CN metadata entirely. A partially specified
		// pair is ambiguous and cannot prove current-CN delivery.
		return leftUnspecified && rightUnspecified
	}
	return sameExecutionNode(left, right)
}

func runtimeFilterScopeAddress(scope *Scope) string {
	if scope == nil {
		return "<nil>"
	}
	if scope.NodeInfo.Id != "" && scope.NodeInfo.Addr != "" {
		return fmt.Sprintf("%s(%s)", scope.NodeInfo.Id, scope.NodeInfo.Addr)
	}
	if scope.NodeInfo.Id != "" {
		return scope.NodeInfo.Id
	}
	if scope.NodeInfo.Addr != "" {
		return scope.NodeInfo.Addr
	}
	return "<local>"
}

func runtimeFilterScopeAddresses(scopes []*Scope) string {
	addresses := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		addresses = append(addresses, runtimeFilterScopeAddress(scope))
	}
	sort.Strings(addresses)
	return fmt.Sprintf("%v", addresses)
}

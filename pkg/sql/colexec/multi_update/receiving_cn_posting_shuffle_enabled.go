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

//go:build receiving_cn_acceptance

package multi_update

import (
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// configureReceivingCNPostingShuffle changes only the physical placement
// choice for the fixture's ordinary integer equality join. It does not
// construct, replace, or simulate a writer, a CN, or a transaction.
func configureReceivingCNPostingShuffle(enabled bool, node, left, right *plan.Node, query *plan.Query) bool {
	if !enabled || node == nil || left == nil || right == nil || query == nil ||
		node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER ||
		left.NodeType != plan.Node_TABLE_SCAN || left.TableDef == nil ||
		!strings.HasPrefix(left.TableDef.Name, "__mo_index_") ||
		right.NodeType != plan.Node_SINK_SCAN || node.Stats == nil ||
		node.Stats.HashmapStats == nil || node.Stats.HashmapStats.Shuffle || len(node.OnList) != 1 || node.OnList[0] == nil ||
		len(node.RuntimeFilterBuildList) != 0 || len(node.RuntimeFilterProbeList) != 0 {
		return false
	}
	fn := node.OnList[0].GetF()
	if fn == nil || fn.GetFunc() == nil || fn.GetFunc().GetObjName() != "=" || len(fn.Args) != 2 ||
		fn.Args[0] == nil || fn.Args[1] == nil ||
		fn.Args[0].GetCol() == nil || fn.Args[1].GetCol() == nil ||
		fn.Args[0].Typ.Id != int32(types.T_int32) || fn.Args[1].Typ.Id != int32(types.T_int32) {
		return false
	}
	// A real shuffle plan owns a paired PASS synchronization filter. Mirror
	// generateNodeRuntimeFilterInTree and reserve a tag absent from this plan;
	// do not weaken the hash-build guard or create an unmatched consumer.
	used := make(map[int32]bool)
	for _, n := range query.Nodes {
		if n == nil {
			continue
		}
		for _, specs := range [][]*plan.RuntimeFilterSpec{n.RuntimeFilterBuildList, n.RuntimeFilterProbeList} {
			for _, spec := range specs {
				if spec != nil {
					used[spec.Tag] = true
				}
			}
		}
		for _, header := range n.SendMsgList {
			used[header.MsgTag] = true
		}
		for _, header := range n.RecvMsgList {
			used[header.MsgTag] = true
		}
	}
	var tag int32 = 1
	for used[tag] && tag < 1<<31-1 {
		tag++
	}
	if used[tag] {
		return false
	}
	node.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{{Tag: tag}}
	node.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: tag}}
	stats := node.Stats.HashmapStats
	stats.Shuffle = true
	stats.ShuffleColIdx = 0
	stats.ShuffleType = plan.ShuffleType_Hash
	stats.ShuffleMethod = plan.ShuffleMethod_Normal
	return true
}

// receivingCNHasPlannedPostingShuffle recognizes the same actual shuffle
// compiler branch when synthetic fixture statistics selected it before the
// forcing hook. Follow plan children so filters around the posting scan do not
// hide that existing choice.
func receivingCNHasPlannedPostingShuffle(node, left, right *plan.Node, query *plan.Query) bool {
	if node == nil || node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER ||
		node.Stats == nil || node.Stats.HashmapStats == nil || !node.Stats.HashmapStats.Shuffle {
		return false
	}
	pending := []*plan.Node{left, right}
	seen := make(map[*plan.Node]bool)
	for len(pending) > 0 {
		n := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if n == nil || seen[n] {
			continue
		}
		seen[n] = true
		if n.NodeType == plan.Node_TABLE_SCAN && n.TableDef != nil && strings.HasPrefix(n.TableDef.Name, "__mo_index_") {
			return true
		}
		if query != nil {
			for _, child := range n.Children {
				if child >= 0 && int(child) < len(query.Nodes) {
					pending = append(pending, query.Nodes[child])
				}
			}
		}
	}
	return false
}

// ReceivingCNForcePostingShuffle is present only in the private diagnostic
// binary. Both its environment flag and transaction-scoped admission must
// match. An event distinguishes forced placement from a shuffle already
// selected with the fixture's synthetic statistics.
func ReceivingCNForcePostingShuffle(proc *process.Process, node, left, right *plan.Node, query *plan.Query) {
	enabled := os.Getenv("MO_RECEIVING_CN_FORCE_POSTING_SHUFFLE") == "1" && ReceivingCNForceMultiCN(proc)
	if !enabled {
		return
	}
	stage := ""
	if configureReceivingCNPostingShuffle(true, node, left, right, query) {
		stage = "forced-posting-shuffle"
	} else if receivingCNHasPlannedPostingShuffle(node, left, right, query) {
		stage = "planned-posting-shuffle"
	} else {
		return
	}
	p := receivingCNIdentity(proc)
	if p != nil {
		_ = receivingCNEmitForProbe(proc.Ctx, p, receivingCNEvent(proc, stage))
	}
}

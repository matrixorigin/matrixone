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

package compile

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// fulltext2SearchFuncName is the TVF name the planner assigns a fulltext2 index scan; it matches
// the unexported constant in pkg/sql/plan and the name the operator registers under.
const fulltext2SearchFuncName = "fulltext2_search"

// tableFunctionConfigHasProbeTail reports whether a serialized fulltext2_search TVF carries a
// self-completing json probe. The operator's config is its FIRST arg, a folded JSON string
// constant unmarshaled into the fulltext2 TableConfig; only the probe_tail flag is inspected here.
func tableFunctionConfigHasProbeTail(tf *pipeline.TableFunction) bool {
	if tf == nil || len(tf.Args) == 0 || tf.Args[0] == nil {
		return false
	}
	lit := tf.Args[0].GetLit()
	if lit == nil {
		return false
	}
	cfgstr := lit.GetSval()
	if cfgstr == "" {
		return false
	}
	var cfg struct {
		ProbeTail bool `json:"probe_tail"`
	}
	if err := json.Unmarshal([]byte(cfgstr), &cfg); err != nil {
		return false
	}
	return cfg.ProbeTail
}

// pipelineHasFulltext2ProbeTail reports whether the pipeline or any child carries a fulltext2_search
// TVF marked as a self-completing json probe (probe_tail).
func pipelineHasFulltext2ProbeTail(p *pipeline.Pipeline) bool {
	if p == nil {
		return false
	}
	for _, in := range p.InstructionList {
		if in == nil || in.TableFunction == nil {
			continue
		}
		if in.TableFunction.Name == fulltext2SearchFuncName && tableFunctionConfigHasProbeTail(in.TableFunction) {
			return true
		}
	}
	for _, child := range p.Children {
		if pipelineHasFulltext2ProbeTail(child) {
			return true
		}
	}
	return false
}

// validateFulltext2ProbeTailDestination fails closed when a scope carrying a self-completing
// fulltext2 json probe is about to be serialized to a CN that does not understand the probe_tail
// TableConfig contract (MORPCVersion81).
//
// addJSONFulltextProbes declines the probe at plan time on a mixed-version fleet, but that sample
// is taken while planning. MOProtocolVersion is explicitly lowered before a rollback, so it can drop
// between plan build and this send; the already-built scope would otherwise go out unchanged. An old
// CN silently drops the probe_tail JSON fields and runs only the stale bulk generation, so the
// mandatory INNER JOIN drops every row whose source commit falls in (searched, snapshot]. This
// re-check reads the CURRENT version (remoteWorkersSupportProtocol gates on it before probing the
// destination), so a rollback in that window is caught and the query fails instead of silently
// losing rows; a re-plan at the lowered version runs it as a plain Table Scan.
func validateFulltext2ProbeTailDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if !pipelineHasFulltext2ProbeTail(p) {
		return nil
	}
	if p != nil && p.Node != nil {
		supported, err := remoteWorkersSupportProtocol(proc, engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, defines.MORPCVersion81)
		if err != nil {
			return err
		}
		if supported {
			return nil
		}
	}
	return moerr.NewNotSupportedNoCtx("remote self-completing fulltext2 json probe requires a newer MORPC protocol version on the destination")
}

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

package search

import (
	"strconv"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Hooks struct{}

var _ searchplugin.Hooks = Hooks{}
var _ searchplugin.ParallelHooks = Hooks{}
var _ searchplugin.ExplainHooks = Hooks{}

func (Hooks) NewReader(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (engine.Reader, error) {
	return ivfflat.NewPlanReader(proc, spec, req)
}

// CanParallelize reports whether spec searches in one round; a multi-round
// search expands buckets from the merged result of the previous round.
func (Hooks) CanParallelize(spec *plan.IndexSearchScan) (bool, error) {
	multiRound, err := ivfflatplan.MultiRound(spec)
	return !multiRound, err
}

func (Hooks) NewReaders(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request, parallelism int) ([]engine.Reader, error) {
	return ivfflat.NewPlanReaders(proc, spec, req, parallelism)
}

// ExplainSettings returns the first-round probe count.
func (Hooks) ExplainSettings(spec *plan.IndexSearchScan) ([]string, error) {
	opts, err := ivfflatplan.DecodeScanOptions(spec.GetAlgoOptions())
	if err != nil {
		return nil, err
	}
	return []string{"NProbe: " + strconv.FormatUint(uint64(opts.InitialProbeCount), 10)}, nil
}

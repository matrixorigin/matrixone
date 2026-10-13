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

// Package search runs a cagra IndexSearchScan: one cuVS CAGRA search of the
// cached index, read from the cagra_meta and cagra_index hidden tables. The
// search itself needs a GPU build.
package search

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cagraPkg "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra"
	cagraplan "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Hooks struct{}

var _ searchplugin.Hooks = Hooks{}
var _ searchplugin.ExplainHooks = Hooks{}
var _ searchplugin.CandidateBudgetHooks = Hooks{}
var _ searchplugin.IndexFilterHooks = Hooks{}
var _ searchplugin.IterativeSearchHooks = Hooks{}

// StartRound is the first round of the CAGRA widening policy (cagra.IterativeSearch).
func (Hooks) StartRound(spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats) searchplugin.Round {
	return cagraPkg.IterativeSearch{}.StartRound(spec, req, stats)
}

// NextRound is the round after prev of the CAGRA widening policy (cagra.IterativeSearch).
func (Hooks) NextRound(spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats, prev searchplugin.Round, got uint64) (searchplugin.Round, bool) {
	return cagraPkg.IterativeSearch{}.NextRound(spec, req, stats, prev, got)
}

// SupportsIndexFilter reports true: the search applies the INCLUDE and
// primary-key predicates of its filter inside the index search.
func (Hooks) SupportsIndexFilter() bool { return true }

// ExplainSettings returns the filter the search applies on the device as SQL ("Index
// Filter"), and on detailed output the static settings first. Compact output prints
// nothing for algo options or index params it cannot decode.
func (Hooks) ExplainSettings(spec *plan.IndexSearchScan, detailed bool) ([]string, error) {
	opts, err := cagraplan.DecodeScanOptions(spec.GetAlgoOptions())
	if err != nil {
		if !detailed {
			return nil, nil
		}
		return nil, err
	}
	var settings []string
	if detailed {
		settings = []string{
			"ThreadsSearch: " + strconv.FormatInt(int64(opts.ThreadsSearch), 10),
			"BatchWindow: " + strconv.FormatInt(int64(opts.BatchWindow), 10),
			"GpuMultiSimulation: " + strconv.FormatInt(int64(opts.GpuMultiSimulation), 10),
			"KeyPartType: " + strconv.FormatInt(int64(opts.KeyPartType), 10),
		}
		if opts.FilterJSON != "" {
			settings = append(settings, "Filter Predicates: "+opts.FilterJSON)
		}
	}
	filter, err := describeIndexFilter(spec, opts.FilterJSON)
	if err != nil {
		if !detailed {
			return nil, nil
		}
		return nil, err
	}
	if filter != "" {
		settings = append(settings, "Index Filter: "+filter)
	}
	return settings, nil
}

// describeIndexFilter renders filterJSON as SQL on the primary key and INCLUDE columns
// of spec.
func describeIndexFilter(spec *plan.IndexSearchScan, filterJSON string) (string, error) {
	include, err := catalog.IncludeColumnsFromIndexParams(spec.GetIndex().GetIndexAlgoParams())
	if err != nil {
		return "", err
	}
	return vectorindex.DescribeIndexFilter(filterJSON, include, spec.GetSourceTableDef().GetPkey().GetPkeyColName())
}

// PostFilterCandidateBudget is the cagra post-filter over-fetch of resultLimit.
func (Hooks) PostFilterCandidateBudget(resultLimit uint64) uint64 {
	return overfetch.PostFilterLimit(resultLimit)
}

func (Hooks) NewReader(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (engine.Reader, error) {
	// The search does not apply a membership key set, so a request whose candidate
	// limit is only valid under one is refused rather than answered without it.
	if req.MembershipFilterRequired {
		return nil, moerr.NewNotSupported(proc.Ctx, "cagra index search with a required membership filter")
	}
	s, err := newSearch(proc, spec, req)
	if err != nil {
		return nil, err
	}
	return newReader(s)
}

// search is one cagra search: the index and table configuration, the query
// and the candidate budget.
type search struct {
	proc       *process.Process
	idxcfg     vectorindex.IndexConfig
	tblcfg     vectorindex.IndexTableConfig
	query      []byte
	queryType  types.T
	limit      uint64
	filterJSON string
	snapshot   *plan.Snapshot
	// iterative is set for mode=iterative: the search widens in rounds up to
	// maxLimit (max_itopk) or maxProbe (max_nprobe) until resultLimit rows qualify.
	iterative   bool
	resultLimit uint64
	maxLimit    uint64
	maxProbe    uint64
}

func newSearch(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (*search, error) {
	if proc == nil {
		return nil, moerr.NewInvalidStateNoCtx("cagra index search requires a process")
	}
	if spec == nil || spec.Index == nil || spec.SourceTable == nil || spec.SourceTableDef == nil {
		return nil, moerr.NewInvalidInputNoCtx("cagra index search is missing source or index metadata")
	}
	idxcfg, err := cagraplan.SearchIndexConfig(spec.Index.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	opts, err := cagraplan.DecodeScanOptions(spec.AlgoOptions)
	if err != nil {
		return nil, err
	}
	// The query vector type must equal the index's base column type, which
	// selects the stored element type of the index.
	queryType := types.T(req.QueryType.Id)
	if int32(queryType) != opts.KeyPartType {
		return nil, moerr.NewInvalidInputNoCtx("query vector type does not match the index base column type")
	}
	idxcfg.CuvsCagra.Dimensions = uint(req.QueryType.Width)
	// A vecf16 base with no QUANTIZATION stores natively as half.
	if queryType == types.T_array_float16 &&
		metric.QuantizationType(idxcfg.CuvsCagra.Quantization) == metric.Quantization_F32 {
		idxcfg.CuvsCagra.Quantization = uint16(metric.Quantization_F16)
	}
	tblcfg := vectorindex.IndexTableConfig{
		DbName:             spec.SourceTable.SchemaName,
		SrcTable:           spec.SourceTableDef.Name,
		OrigFuncName:       spec.DistanceFunction,
		ThreadsSearch:      opts.ThreadsSearch,
		BatchWindow:        opts.BatchWindow,
		GpuMultiSimulation: opts.GpuMultiSimulation,
		KeyPartType:        opts.KeyPartType,
	}
	for _, table := range spec.HiddenTables {
		switch table.GetRole() {
		case catalog.Cagra_TblType_Metadata:
			tblcfg.MetadataTable = table.GetObject().GetObjName()
		case catalog.Cagra_TblType_Storage:
			tblcfg.IndexTable = table.GetObject().GetObjName()
		}
	}
	if tblcfg.MetadataTable == "" || tblcfg.IndexTable == "" {
		return nil, moerr.NewInvalidInputNoCtx("cagra index search is missing its hidden tables")
	}
	return &search{
		proc:        proc,
		idxcfg:      idxcfg,
		tblcfg:      tblcfg,
		query:       req.QueryPayload,
		queryType:   queryType,
		limit:       max(req.CandidateBudget, 1),
		filterJSON:  opts.FilterJSON,
		snapshot:    spec.ScanSnapshot,
		iterative:   opts.Iterative,
		resultLimit: req.ResultLimit,
		maxLimit:    req.MaxInternalTopK,
		maxProbe:    req.MaxNprobe,
	}, nil
}

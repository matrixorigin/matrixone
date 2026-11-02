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

// Package search runs an ivfpq IndexSearchScan: one cuVS IVF-PQ search of the
// cached index, read from the ivfpq_meta and ivfpq_index hidden tables. The
// search itself needs a GPU build.
package search

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	ivfpqPkg "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq"
	ivfpqplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/plan"
	ivfpqruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/runtime"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Hooks struct{}

var _ searchplugin.Hooks = Hooks{}
var _ searchplugin.ExplainHooks = Hooks{}
var _ searchplugin.CandidateBudgetHooks = Hooks{}
var _ searchplugin.ProbeKnobsHooks = Hooks{}
var _ searchplugin.IndexFilterHooks = Hooks{}
var _ searchplugin.MembershipHooks = Hooks{}
var _ searchplugin.IterativeSearchHooks = Hooks{}

// StartRound is the first round of the IVF-PQ widening policy (ivfpq.IterativeSearch).
func (Hooks) StartRound(spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats) searchplugin.Round {
	return ivfpqPkg.IterativeSearch{}.StartRound(spec, req, stats)
}

// NextRound is the round after prev of the IVF-PQ widening policy (ivfpq.IterativeSearch).
func (Hooks) NextRound(spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats, prev searchplugin.Round, got uint64) (searchplugin.Round, bool) {
	return ivfpqPkg.IterativeSearch{}.NextRound(spec, req, stats, prev, got)
}

// SupportsProbeKnobs reports true: the search probes IVF lists.
func (Hooks) SupportsProbeKnobs() bool { return true }

// SupportsIndexFilter reports true: the search applies the INCLUDE and
// primary-key predicates of its filter inside the index search.
func (Hooks) SupportsIndexFilter() bool { return true }

// SupportsMembership reports true: the search ANDs the membership key set into its
// filter mask on the device.
func (Hooks) SupportsMembership() bool { return true }

// ExplainSettings returns the filter the search applies on the device as SQL ("Index
// Filter"), and on detailed output the static settings first. Compact output prints
// nothing for algo options or index params it cannot decode.
func (Hooks) ExplainSettings(spec *plan.IndexSearchScan, detailed bool) ([]string, error) {
	opts, err := ivfpqplan.DecodeScanOptions(spec.GetAlgoOptions())
	if err != nil {
		if !detailed {
			return nil, nil
		}
		return nil, err
	}
	var settings []string
	if detailed {
		settings = []string{
			"NProbe: " + strconv.FormatUint(uint64(opts.Nprobe), 10),
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

// PostFilterCandidateBudget is the ivfpq post-filter over-fetch of resultLimit.
func (Hooks) PostFilterCandidateBudget(resultLimit uint64) uint64 {
	return overfetch.PostFilterLimit(resultLimit)
}

func (Hooks) NewReader(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (engine.Reader, error) {
	if req.MembershipFilterRequired && !req.HasMembershipFilter {
		return nil, moerr.NewInvalidInput(proc.Ctx, "ivfpq index search requires a membership filter that is missing")
	}
	s, err := newSearch(proc, spec, req)
	if err != nil {
		return nil, err
	}
	r, err := newReader(s)
	if err != nil {
		s.freeMembership()
		return nil, err
	}
	return r, nil
}

// search is one ivfpq search: the index and table configuration, the query
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
	// membership holds the primary keys the search is restricted to, or nil.
	// emptyMembership is an exact empty key set: the search returns no rows.
	membership      docfilter.MembershipFilter
	membershipCount uint64
	emptyMembership bool
}

// freeMembership frees the membership filter of s; s may be nil.
func (s *search) freeMembership() {
	if s != nil && s.membership != nil {
		s.membership.Free()
		s.membership = nil
	}
}

func newSearch(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (*search, error) {
	if proc == nil {
		return nil, moerr.NewInvalidStateNoCtx("ivfpq index search requires a process")
	}
	if spec == nil || spec.Index == nil || spec.SourceTable == nil || spec.SourceTableDef == nil {
		return nil, moerr.NewInvalidInputNoCtx("ivfpq index search is missing source or index metadata")
	}
	idxcfg, err := ivfpqplan.SearchIndexConfig(spec.Index.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	opts, err := ivfpqplan.DecodeScanOptions(spec.AlgoOptions)
	if err != nil {
		return nil, err
	}
	queryType := types.T(req.QueryType.Id)
	if !catalogplugin.SupportsVectorType(ivfpqruntime.CatalogHooks{}, queryType) {
		return nil, moerr.NewInvalidInputNoCtx("second argument (query vector) must be a float32 array")
	}
	// The query vector type must equal the index's base column type, which
	// selects the stored element type of the index.
	if int32(queryType) != opts.KeyPartType {
		return nil, moerr.NewInvalidInputNoCtx("query vector type does not match the index base column type")
	}
	idxcfg.CuvsIvfpq.Dimensions = uint(req.QueryType.Width)
	// A vecf16 base with no QUANTIZATION stores natively as half.
	if queryType == types.T_array_float16 &&
		metric.QuantizationType(idxcfg.CuvsIvfpq.Quantization) == metric.Quantization_F32 {
		idxcfg.CuvsIvfpq.Quantization = uint16(metric.Quantization_F16)
	}
	tblcfg := vectorindex.IndexTableConfig{
		DbName:             spec.SourceTable.SchemaName,
		SrcTable:           spec.SourceTableDef.Name,
		OrigFuncName:       spec.DistanceFunction,
		ThreadsSearch:      opts.ThreadsSearch,
		BatchWindow:        opts.BatchWindow,
		GpuMultiSimulation: opts.GpuMultiSimulation,
		Nprobe:             opts.Nprobe,
		KeyPartType:        opts.KeyPartType,
	}
	for _, table := range spec.HiddenTables {
		switch table.GetRole() {
		case catalog.Ivfpq_TblType_Metadata:
			tblcfg.MetadataTable = table.GetObject().GetObjName()
		case catalog.Ivfpq_TblType_Storage:
			tblcfg.IndexTable = table.GetObject().GetObjName()
		}
	}
	if tblcfg.MetadataTable == "" || tblcfg.IndexTable == "" {
		return nil, moerr.NewInvalidInputNoCtx("ivfpq index search is missing its hidden tables")
	}
	s := &search{
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
	}
	if req.HasMembershipFilter {
		if s.membership, s.membershipCount, s.emptyMembership, err = vectorindex.BuildMembership(
			proc.GetService(), "ivfpq", spec.SourceTableDef, req.MembershipFilter); err != nil {
			return nil, err
		}
	}
	return s, nil
}

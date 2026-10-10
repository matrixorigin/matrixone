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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	ivfpqplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/plan"
	ivfpqruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/runtime"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Hooks struct{}

var _ searchplugin.Hooks = Hooks{}
var _ searchplugin.CandidateBudgetHooks = Hooks{}

// PostFilterCandidateBudget is the ivfpq post-filter over-fetch of resultLimit.
func (Hooks) PostFilterCandidateBudget(resultLimit uint64) uint64 {
	return overfetch.PostFilterLimit(resultLimit)
}

func (Hooks) NewReader(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (engine.Reader, error) {
	// The search does not apply a membership key set, so a request whose candidate
	// limit is only valid under one is refused rather than answered without it.
	if req.MembershipFilterRequired {
		return nil, moerr.NewNotSupported(proc.Ctx, "ivfpq index search with a required membership filter")
	}
	s, err := newSearch(proc, spec, req)
	if err != nil {
		return nil, err
	}
	return newReader(s)
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
	return &search{
		proc:       proc,
		idxcfg:     idxcfg,
		tblcfg:     tblcfg,
		query:      req.QueryPayload,
		queryType:  queryType,
		limit:      max(req.CandidateBudget, 1),
		filterJSON: opts.FilterJSON,
		snapshot:   spec.ScanSnapshot,
	}, nil
}

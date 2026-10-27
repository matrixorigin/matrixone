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

// Package search runs an hnsw IndexSearchScan: one usearch search of the
// cached index, read from the hnsw_meta and hnsw_index hidden tables.
package search

import (
	"context"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/search/planreader"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	hnswplan "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

type Hooks struct{}

var _ searchplugin.Hooks = Hooks{}
var _ searchplugin.ExplainHooks = Hooks{}
var _ searchplugin.CandidateBudgetHooks = Hooks{}
var _ searchplugin.MembershipHooks = Hooks{}

// SupportsMembership reports true: the search applies Request.MembershipFilter to every
// sub-index search.
func (Hooks) SupportsMembership() bool { return true }

var _ searchplugin.IterativeSearchHooks = Hooks{}

// StartRound is the first round of the HNSW widening policy (hnsw.IterativeSearch).
func (Hooks) StartRound(spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats) searchplugin.Round {
	return hnsw.IterativeSearch{}.StartRound(spec, req, stats)
}

// NextRound is the round after prev of the HNSW widening policy (hnsw.IterativeSearch).
func (Hooks) NextRound(spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats, prev searchplugin.Round, got uint64) (searchplugin.Round, bool) {
	return hnsw.IterativeSearch{}.NextRound(spec, req, stats, prev, got)
}

// ExplainSettings preserves compact text and exposes static settings on detailed output.
func (Hooks) ExplainSettings(spec *plan.IndexSearchScan, detailed bool) ([]string, error) {
	if !detailed {
		return nil, nil
	}
	opts, err := hnswplan.DecodeScanOptions(spec.GetAlgoOptions())
	if err != nil {
		return nil, err
	}
	settings := []string{
		"ThreadsSearch: " + strconv.FormatInt(int64(opts.ThreadsSearch), 10),
	}
	if opts.PrefilterExactThreshold > 0 {
		settings = append(settings, "PrefilterExactThreshold: "+strconv.FormatInt(opts.PrefilterExactThreshold, 10))
	}
	return settings, nil
}

// PostFilterCandidateBudget is the hnsw post-filter over-fetch of resultLimit.
func (Hooks) PostFilterCandidateBudget(resultLimit uint64) uint64 {
	return overfetch.PostFilterLimit(resultLimit)
}

func (Hooks) NewReader(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (engine.Reader, error) {
	if req.MembershipFilterRequired && !req.HasMembershipFilter {
		return nil, moerr.NewInvalidInput(proc.Ctx, "hnsw index search requires a membership filter that is missing")
	}
	s, err := newSearcher(proc, spec, req)
	if err != nil {
		return nil, err
	}
	return planreader.New(s), nil
}

// newAlgo creates the usearch search of one query.
var newAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, f64 bool) veccache.VectorIndexSearchIf {
	if f64 {
		return hnsw.NewHnswSearch[float64](idxcfg, tblcfg)
	}
	return hnsw.NewHnswSearch[float32](idxcfg, tblcfg)
}

type searcher struct {
	proc     *process.Process
	idxcfg   vectorindex.IndexConfig
	tblcfg   vectorindex.IndexTableConfig
	query    []byte
	limit    uint64
	snapshot *plan.Snapshot

	// iterative is set for mode=iterative: a membership search widens in rounds.
	// resultLimit is the row count of the query and maxLimit caps limit while it
	// widens.
	iterative   bool
	resultLimit uint64
	maxLimit    uint64
	// exactThreshold is the largest membership searched by scanning its vectors.
	exactThreshold uint64
	// f64 is set for a float64 query: the base column is vecf64. A vecf16/vecbf16/vecint8/
	// vecuint8 query of type queryType is widened to float32.
	f64       bool
	queryType int32
	// membership holds the primary keys the search is restricted to, or nil.
	// emptyMembership is an exact empty key set: the search returns no rows.
	membership      docfilter.MembershipFilter
	membershipCount uint64
	emptyMembership bool
}

func newSearcher(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (*searcher, error) {
	if proc == nil {
		return nil, moerr.NewInvalidStateNoCtx("hnsw index search requires a process")
	}
	if spec == nil || spec.Index == nil || spec.SourceTable == nil || spec.SourceTableDef == nil {
		return nil, moerr.NewInvalidInputNoCtx("hnsw index search is missing source or index metadata")
	}
	idxcfg, err := hnswplan.SearchIndexConfig(spec.Index.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	quantization, err := hnswplan.Quantization(spec.Index.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	idxcfg.Usearch.Quantization, err = hnsw.StorageQuantization(quantization, req.QueryType.Id)
	if err != nil {
		return nil, err
	}
	queryKind, err := hnsw.QuantizationToUsearch(req.QueryType.Id)
	if err != nil {
		return nil, err
	}
	idxcfg.Usearch.Dimensions = uint(req.QueryType.Width)
	opts, err := hnswplan.DecodeScanOptions(spec.AlgoOptions)
	if err != nil {
		return nil, err
	}
	tblcfg := vectorindex.IndexTableConfig{
		DbName:        spec.SourceTable.SchemaName,
		SrcTable:      spec.SourceTableDef.Name,
		OrigFuncName:  spec.DistanceFunction,
		ThreadsSearch: opts.ThreadsSearch,
	}
	for _, table := range spec.HiddenTables {
		switch table.GetRole() {
		case catalog.Hnsw_TblType_Metadata:
			tblcfg.MetadataTable = table.GetObject().GetObjName()
		case catalog.Hnsw_TblType_Storage:
			tblcfg.IndexTable = table.GetObject().GetObjName()
		}
	}
	if tblcfg.MetadataTable == "" || tblcfg.IndexTable == "" {
		return nil, moerr.NewInvalidInputNoCtx("hnsw index search is missing its hidden tables")
	}
	s := &searcher{
		proc:        proc,
		idxcfg:      idxcfg,
		tblcfg:      tblcfg,
		query:       req.QueryPayload,
		limit:       max(req.CandidateBudget, 1),
		snapshot:    spec.ScanSnapshot,
		resultLimit: req.ResultLimit,
		maxLimit:    req.MaxInternalTopK,
		iterative:   opts.Iterative,
		f64:         queryKind == usearch.F64,
		queryType:   req.QueryType.Id,
	}
	if opts.PrefilterExactThreshold > 0 {
		s.exactThreshold = uint64(opts.PrefilterExactThreshold)
	}
	if req.HasMembershipFilter {
		if err := s.setMembership(spec, req.MembershipFilter); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// setMembership builds the exact primary-key filter of the serialized key vector payload.
func (s *searcher) setMembership(spec *plan.IndexSearchScan, payload []byte) (err error) {
	s.membership, s.membershipCount, s.emptyMembership, err = vectorindex.BuildMembership(
		s.proc.GetService(), "hnsw", spec.SourceTableDef, payload)
	return err
}

// Next returns the whole result as one chunk.
func (s *searcher) Next(ctx context.Context) (planreader.Chunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return planreader.Chunk{}, false, err
	}
	if s.emptyMembership {
		return planreader.Chunk{}, false, nil
	}
	veccache.Cache.Once()
	var keys []int64
	var distances []float64
	var err error
	if s.f64 {
		keys, distances, err = search[float64](s)
	} else {
		keys, distances, err = search[float32](s)
	}
	if err != nil {
		return planreader.Chunk{}, false, err
	}
	return planreader.Chunk{Keys: keys, Scores: distances}, false, nil
}

func (s *searcher) Close() error {
	if s.membership != nil {
		s.membership.Free()
		s.membership = nil
	}
	return nil
}

func search[T types.RealNumbers](s *searcher) ([]int64, []float64, error) {
	query := types.BytesToArray[T](s.query)
	if f32, narrow := hnsw.BytesToFloat32(s.queryType, s.query); narrow {
		query = any(f32).([]T)
	}
	if uint(len(query)) != s.idxcfg.Usearch.Dimensions {
		return nil, nil, moerr.NewInvalidInput(s.proc.Ctx, fmt.Sprintf(
			"vector ops between different dimensions (%d, %d) is not permitted.", s.idxcfg.Usearch.Dimensions, len(query)))
	}
	// Named-snapshot search (#27927): the index-load SQL runs on a txn cloned at
	// the snapshot TS, and the cache key carries that TS.
	sp := sqlexec.NewSqlProcess(s.proc)
	cacheKey := s.tblcfg.IndexTable
	if ets := sp.ApplyScanSnapshot(s.snapshot); ets != nil {
		cacheKey = veccache.SnapshotKey(s.tblcfg.IndexTable, *ets)
	}
	rt := vectorindex.RuntimeConfig{
		Limit:           uint(s.limit),
		OrigFuncName:    s.tblcfg.OrigFuncName,
		Membership:      s.membership,
		MembershipCount: s.membershipCount,
		Iterative:       s.iterative,
		ExactThreshold:  s.exactThreshold,
		ResultLimit:     uint(s.resultLimit),
		MaxLimit:        uint(s.maxLimit),
	}
	keys, distances, err := veccache.Cache.Search(sp, cacheKey, newAlgo(s.idxcfg, s.tblcfg, s.f64), query, rt)
	if err != nil {
		return nil, nil, err
	}
	ids, ok := keys.([]int64)
	if !ok {
		return nil, nil, moerr.NewInternalError(s.proc.Ctx, "keys is not []int64")
	}
	return ids, distances, nil
}

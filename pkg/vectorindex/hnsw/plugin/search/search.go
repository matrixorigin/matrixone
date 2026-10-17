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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/search/planreader"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	hnswcoverage "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/coverage"
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
	return settings, nil
}

// PostFilterCandidateBudget is the hnsw post-filter over-fetch of resultLimit.
func (Hooks) PostFilterCandidateBudget(resultLimit uint64) uint64 {
	return overfetch.PostFilterLimit(resultLimit)
}

func (Hooks) NewReader(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (engine.Reader, error) {
	// The search does not apply a membership key set, so a request whose candidate
	// limit is only valid under one is refused rather than answered without it.
	if req.MembershipFilterRequired {
		return nil, moerr.NewNotSupported(proc.Ctx, "hnsw index search with a required membership filter")
	}
	s, err := newSearcher(proc, spec, req)
	if err != nil {
		return nil, err
	}
	return planreader.New(s), nil
}

// newAlgo creates the usearch search of one query.
var newAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	if idxcfg.Usearch.Quantization == usearch.F64 {
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
	sourceID uint64
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
	idxcfg.Usearch.Quantization, err = hnsw.QuantizationToUsearch(req.QueryType.Id)
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
	return &searcher{
		proc:     proc,
		idxcfg:   idxcfg,
		tblcfg:   tblcfg,
		query:    req.QueryPayload,
		limit:    max(req.CandidateBudget, 1),
		snapshot: spec.ScanSnapshot,
		sourceID: spec.SourceTableDef.TblId,
	}, nil
}

// Next returns the whole result as one chunk.
func (s *searcher) Next(ctx context.Context) (planreader.Chunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return planreader.Chunk{}, false, err
	}
	veccache.Cache.Once()
	var keys []int64
	var distances []float64
	var err error
	if s.idxcfg.Usearch.Quantization == usearch.F64 {
		keys, distances, err = search[float64](s)
	} else {
		keys, distances, err = search[float32](s)
	}
	if err != nil {
		return planreader.Chunk{}, false, err
	}
	return planreader.Chunk{Keys: keys, Scores: distances}, false, nil
}

func (*searcher) Close() error { return nil }

func search[T types.RealNumbers](s *searcher) ([]int64, []float64, error) {
	query := types.BytesToArray[T](s.query)
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
	var searchedBuildTS int64
	rt := vectorindex.RuntimeConfig{
		Limit: uint(s.limit), OrigFuncName: s.tblcfg.OrigFuncName,
		SearchedBuildTS: &searchedBuildTS,
	}
	keys, distances, err := veccache.Cache.Search(sp, cacheKey, newAlgo(s.idxcfg, s.tblcfg), query, rt)
	if err != nil {
		return nil, nil, err
	}
	if err := s.validateSearchedGeneration(searchedBuildTS); err != nil {
		return nil, nil, err
	}
	ids, ok := keys.([]int64)
	if !ok {
		return nil, nil, moerr.NewInternalError(s.proc.Ctx, "keys is not []int64")
	}
	return ids, distances, nil
}

// validateSearchedGeneration closes the planning-to-execution race. The cache
// entry is protected while Search records the generation it actually used, so
// this check cannot be fooled by a concurrent evict/reload publishing a
// different generation after the search. An unsafe generation requests the
// compiler's exact (pre-vector) retry instead of returning incomplete top-k
// candidates.
func (s *searcher) validateSearchedGeneration(buildTS int64) error {
	if s == nil || s.proc == nil || s.sourceID == 0 || s.proc.GetTxnOperator() == nil {
		// Synthetic reader tests do not have a catalog transaction or source ID.
		return nil
	}
	if buildTS <= 0 || s.proc.GetSessionInfo() == nil || s.proc.GetSessionInfo().StorageEngine == nil {
		return moerr.NewVectorNeedRetryWithPreModeNoCtx()
	}

	sp := sqlexec.NewSqlProcess(s.proc)
	snapshotTS := sp.ApplyScanSnapshot(s.snapshot)
	readTxn := s.proc.GetTxnOperator()
	readTS := types.TimestampToTS(readTxn.SnapshotTS())
	if snapshotTS != nil {
		readTxn = readTxn.CloneSnapshotOp(*snapshotTS)
		readTS = types.TimestampToTS(*snapshotTS)
	}
	ctx := s.proc.GetTopContext()
	if accountID, err := sp.EffectiveAccountID(); err == nil {
		ctx = defines.AttachAccountId(ctx, accountID)
	}
	_, _, rel, err := s.proc.GetSessionInfo().StorageEngine.GetRelationById(ctx, readTxn, s.sourceID)
	if err != nil {
		return moerr.NewVectorNeedRetryWithPreModeNoCtx()
	}
	provider, ok := rel.(engine.SourceCommitTSProvider)
	if !ok {
		return moerr.NewVectorNeedRetryWithPreModeNoCtx()
	}
	sourceTS, err := provider.SourceCommitTS(ctx, types.BuildTS(buildTS, 0))
	if err != nil || !hnswcoverage.BuildCoversSnapshot(buildTS, readTS, sourceTS) {
		return moerr.NewVectorNeedRetryWithPreModeNoCtx()
	}
	return nil
}

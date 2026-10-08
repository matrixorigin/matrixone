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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/search/planreader"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

type Hooks struct{}

var _ searchplugin.Hooks = Hooks{}
var _ searchplugin.CandidateBudgetHooks = Hooks{}

// PostFilterCandidateBudget is the hnsw post-filter over-fetch of resultLimit.
func (Hooks) PostFilterCandidateBudget(resultLimit uint64) uint64 {
	return overfetch.PostFilterLimit(resultLimit)
}

func (Hooks) NewReader(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (engine.Reader, error) {
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
}

func newSearcher(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (*searcher, error) {
	if proc == nil {
		return nil, moerr.NewInvalidStateNoCtx("hnsw index search requires a process")
	}
	if spec == nil || spec.Index == nil || spec.SourceTable == nil || spec.SourceTableDef == nil {
		return nil, moerr.NewInvalidInputNoCtx("hnsw index search is missing source or index metadata")
	}
	idxcfg, err := hnsw.SearchIndexConfig(spec.Index.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	idxcfg.Usearch.Quantization, err = hnsw.QuantizationToUsearch(req.QueryType.Id)
	if err != nil {
		return nil, err
	}
	idxcfg.Usearch.Dimensions = uint(req.QueryType.Width)
	opts, err := hnsw.DecodeScanOptions(spec.AlgoOptions)
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
		limit:    req.CandidateBudget,
		snapshot: spec.ScanSnapshot,
	}, nil
}

// Next returns the whole result as one chunk.
func (s *searcher) Next(ctx context.Context) (planreader.Chunk, bool, error) {
	if s.limit == 0 {
		return planreader.Chunk{}, false, nil
	}
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
	rt := vectorindex.RuntimeConfig{Limit: uint(s.limit), OrigFuncName: s.tblcfg.OrigFuncName}
	keys, distances, err := veccache.Cache.Search(sp, cacheKey, newAlgo(s.idxcfg, s.tblcfg), query, rt)
	if err != nil {
		return nil, nil, err
	}
	ids, ok := keys.([]int64)
	if !ok {
		return nil, nil, moerr.NewInternalError(s.proc.Ctx, "keys is not []int64")
	}
	return ids, distances, nil
}

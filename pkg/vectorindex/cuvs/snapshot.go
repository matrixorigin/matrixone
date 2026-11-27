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

package cuvs

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

const GPUMultiSimulationVar = "gpu_multi_simulation"

// ReindexChangesStorageType reports whether the new decoder needs the model
// to be published atomically with the index parameters.
func ReindexChangesStorageType(table *plan.TableDef, index *plan.IndexDef, oldQuantization, newQuantization string) bool {
	if oldQuantization == newQuantization {
		return false
	}
	if len(index.Parts) == 1 {
		for _, column := range table.Cols {
			if strings.EqualFold(column.Name, index.Parts[0]) {
				baseType := types.T(column.Typ.Id)
				return metric.CuvsStorageQuantization(baseType, oldQuantization) !=
					metric.CuvsStorageQuantization(baseType, newQuantization)
			}
		}
	}
	return true
}

// ResolveIndexIncludeColumns uses the stored index parameters and source schema
// to derive the CDC frame header and row layout.
func ResolveIndexIncludeColumns(table *plan.TableDef, algoParams string) ([]IncludeBinding, string, int, error) {
	if algoParams == "" {
		return nil, "", 0, nil
	}
	node, err := sonic.Get([]byte(algoParams), catalog.IncludedColumns)
	if err != nil {
		return nil, "", 0, nil
	}
	names, err := node.StrictString()
	if err != nil {
		return nil, "", 0, nil
	}
	return ResolveIncludeColumns(names, table.Name2ColIndex,
		func(pos int32) int32 { return table.Cols[pos].Typ.Id })
}

// PrepareSnapshotMarker captures the old chunk floor before a full rebuild.
// Its completion writes a zero-operation frame in the same transaction as the
// replacement. It preserves INCLUDE metadata and a distinct generation even
// when the rebuild has no native model.
func PrepareSnapshotMarker(cfg vectorindex.IndexTableConfig, colMetaJSON string,
	runSQL func(string) (executor.Result, error)) (func() error, error) {
	nextChunk := func() (int64, error) {
		res, err := runSQL(NextChunkIdSql(cfg, vectorindex.CdcTailId, vectorindex.Tag_CdcEvents))
		if err != nil {
			return 0, err
		}
		defer res.Close()
		for _, bat := range res.Batches {
			if bat != nil && bat.RowCount() > 0 {
				return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0), nil
			}
		}
		return 0, nil
	}
	floor, err := nextChunk()
	if err != nil {
		return nil, err
	}
	return func() error {
		next, err := nextChunk()
		if err != nil {
			return err
		}
		frame := FrameCdcChunk(nil, []byte(colMetaJSON), 0, 0, 0)
		sql := fmt.Sprintf("INSERT INTO %s VALUES (%s,%d,unhex(%s),%d)",
			sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable), sqlquote.String(vectorindex.CdcTailId),
			max(next, floor), sqlquote.String(hex.EncodeToString(frame)), vectorindex.Tag_CdcEvents)
		res, err := runSQL(sql)
		res.Close()
		return err
	}, nil
}

// ReplaceSnapshot executes an inline rebuild and publishes its chunk marker.
// Async rebuilds use the same marker around their own guarded transaction.
func ReplaceSnapshot(dbName string, table *plan.TableDef, storage *plan.IndexDef,
	statements []string, runSQL func(string) (executor.Result, error)) error {
	_, header, _, err := ResolveIndexIncludeColumns(table, storage.IndexAlgoParams)
	if err != nil {
		return err
	}
	cfg := vectorindex.IndexTableConfig{DbName: dbName, IndexTable: storage.IndexTableName}
	finish, err := PrepareSnapshotMarker(cfg, header, runSQL)
	if err != nil {
		return err
	}
	for _, sql := range statements {
		res, err := runSQL(sql)
		res.Close()
		if err != nil {
			return err
		}
	}
	return finish()
}

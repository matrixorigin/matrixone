// Copyright 2022 Matrix Origin
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

package table_function

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"go.uber.org/zap"

	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// XXX: TODO:
// to all my understanding, we are getting metadata info of a table
// as a simple one batch TVF.   This won't work at all for large tables.
// This need to be a REAL scanner.
type metadataScanState struct {
	simpleOneBatchState
}

func metadataScanPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var err error
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))
	for i := range tableFunction.Attrs {
		tableFunction.Attrs[i] = strings.ToUpper(tableFunction.Attrs[i])
	}
	return &metadataScanState{}, err
}

func getIndexTableNameByIndexName(proc *process.Process, dbname, tablename, indexname string) (string, error) {
	var indexTableName string

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	db, err := e.Database(proc.Ctx, dbname, proc.GetTxnOperator())
	if err != nil {
		return "", moerr.NewInternalError(proc.Ctx, "get database failed in metadata scan")
	}

	rel, err := db.Relation(proc.Ctx, tablename, nil)
	if err != nil {
		return "", err
	}
	tableid := rel.GetTableID(proc.Ctx)
	logutil.Info("relID", zap.Uint64("value", tableid))

	sql := fmt.Sprintf("SELECT distinct(index_table_name) FROM mo_catalog.mo_indexes WHERE table_id = '%d' AND name = '%s'", tableid, indexname)
	result, err := sqlexec.RunSql(sqlexec.NewSqlProcess(proc), sql)
	if err != nil {
		return "", err
	}
	for _, batch := range result.Batches {
		logutil.Info("Batch debug",
			zap.Int("vec_count", len(batch.Vecs)),
			zap.Strings("vector_types", func() []string {
				types := make([]string, len(batch.Vecs))
				for i, v := range batch.Vecs {
					types[i] = v.GetType().String()
				}
				return types
			}()),
		)
		if len(batch.Vecs) == 0 {
			continue
		}
		vec := batch.Vecs[0]
		for row := 0; row < vec.Length(); row++ {
			if !vec.IsNull(uint64(row)) {
				indexTableName = vec.GetStringAt(row)
				logutil.Info("Index table name", zap.String("value", indexTableName))
			}
		}
	}
	invalidIndexErr := "check whether the index \"" + indexname + "\" really exists"
	if indexTableName == "" {
		return "", moerr.NewInternalError(proc.Ctx, invalidIndexErr)
	}
	return indexTableName, nil
}

func (s *metadataScanState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	s.startPreamble(tf, proc, nthRow)

	source := tf.ctr.argVecs[0]
	col := tf.ctr.argVecs[1]
	dbname, tablename, indexname, colname, visitTombstone, err := handleDataSource(source, col)
	logutil.Infof("db: %s, table: %s, index: %s, col: %s in metadataScan", dbname, tablename, indexname, colname)
	if err != nil {
		return err
	}

	// When the source format is db_name.table_name.?index_name
	// metadata_scan actually returns the metadata of the index table
	if indexname != "" {
		indexTableName, err := getIndexTableNameByIndexName(proc, dbname, tablename, indexname)
		if err != nil {
			return err
		}
		tablename = indexTableName
	}

	// Oh my
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	db, err := e.Database(proc.Ctx, dbname, proc.GetTxnOperator())
	if err != nil {
		return moerr.NewInternalError(proc.Ctx, "get database failed in metadata scan")
	}

	rel, err := db.Relation(proc.Ctx, tablename, nil)
	if err != nil {
		return err
	}

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
	metaInfos, err := rel.GetColumMetadataScanInfo(newCtx, colname, visitTombstone)
	if err != nil {
		return err
	}
	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)

	for i := range metaInfos {
		err = fillMetadataInfoBat(s.batch, proc, tf, metaInfos[i])
		if err != nil {
			return err
		}
	}
	s.batch.AddRowCount(len(metaInfos))
	return nil
}

func handleDataSource(source, col *vector.Vector) (string, string, string, string, bool, error) {
	if source.Length() != 1 || col.Length() != 1 {
		return "", "", "", "", false, moerr.NewInternalErrorNoCtx("wrong input len")
	}
	sourceStr := source.GetStringAt(0)
	parts := strings.Split(sourceStr, ".")
	switch len(parts) {
	// Old source format: db_name.table_name
	case 2:
		dbname, tablename := parts[0], parts[1]
		return dbname, tablename, "", col.GetStringAt(0), false, nil
	// Newly supported source format:
	// db_name.table_name.?index_name
	// or db_name.table_name.#
	case 3:
		dbname, tablename, thirdPart := parts[0], parts[1], parts[2]
		if len(thirdPart) == 1 && thirdPart[0] == '#' {
			return dbname, tablename, "", col.GetStringAt(0), true, nil
		}
		if len(thirdPart) == 0 || thirdPart[0] != '?' {
			return "", "", "", "", false, moerr.NewInternalErrorNoCtx("index name must start with ? and follow identifier rules")
		}
		indexName := thirdPart[1:]
		return dbname, tablename, indexName, col.GetStringAt(0), false, nil
	// db_name.table_name.?index_name.#
	case 4:
		dbname, tablename, indexPart, tombstonePart := parts[0], parts[1], parts[2], parts[3]
		if len(tombstonePart) != 1 || tombstonePart[0] != '#' {
			return "", "", "", "", false, moerr.NewInternalErrorNoCtx("invalid tombstone identifier: must be #")
		}
		if len(indexPart) == 0 || indexPart[0] != '?' {
			return "", "", "", "", false, moerr.NewInternalErrorNoCtx("index name must start with ? and follow identifier rules")
		}
		indexName := indexPart[1:]
		return dbname, tablename, indexName, col.GetStringAt(0), true, nil
	default:
		return "", "", "", "", false, moerr.NewInternalErrorNoCtx("source must be in db_name.table_name or db_name.table_name.?index_name or db_name.table_name.# or db_name.table_name.?index_name.# format")
	}
}

func fillMetadataInfoBat(opBat *batch.Batch, proc *process.Process, tableFunction *TableFunction, info *plan.MetadataScanInfo) error {
	mp := proc.GetMPool()
	zm := index.ZM(info.ZoneMap)
	zmNull := !zm.IsInited()

	for i, colname := range tableFunction.Attrs {
		idx, ok := plan.MetadataScanInfo_MetadataScanInfoType_value[colname]
		if !ok {
			opBat.Clean(proc.GetMPool())
			return moerr.NewInternalErrorf(proc.Ctx, "bad input select columns name %v", colname)
		}

		switch plan.MetadataScanInfo_MetadataScanInfoType(idx) {
		case plan.MetadataScanInfo_COL_NAME:
			vector.AppendBytes(opBat.Vecs[i], []byte(info.ColName), false, mp)

		case plan.MetadataScanInfo_OBJECT_NAME:
			vector.AppendBytes(opBat.Vecs[i], []byte(info.ObjectName), false, mp)

		case plan.MetadataScanInfo_IS_HIDDEN:
			vector.AppendFixed(opBat.Vecs[i], info.IsHidden, false, mp)

		case plan.MetadataScanInfo_OBJ_LOC:
			vector.AppendBytes(opBat.Vecs[i], []byte(objectio.Location(info.ObjLoc).String()), false, mp)

		case plan.MetadataScanInfo_CREATE_TS:
			var ts types.TS
			if err := ts.Unmarshal(info.CreateTs); err != nil {
				return err
			}
			vector.AppendFixed(opBat.Vecs[i], ts, false, mp)

		case plan.MetadataScanInfo_DELETE_TS:
			var ts types.TS
			if err := ts.Unmarshal(info.DeleteTs); err != nil {
				return err
			}
			vector.AppendFixed(opBat.Vecs[i], ts, false, mp)

		case plan.MetadataScanInfo_ROWS_CNT:
			vector.AppendFixed(opBat.Vecs[i], info.RowCnt, false, mp)

		case plan.MetadataScanInfo_NULL_CNT:
			vector.AppendFixed(opBat.Vecs[i], info.NullCnt, false, mp)

		case plan.MetadataScanInfo_COMPRESS_SIZE:
			vector.AppendFixed(opBat.Vecs[i], info.CompressSize, false, mp)

		case plan.MetadataScanInfo_ORIGIN_SIZE:
			vector.AppendFixed(opBat.Vecs[i], info.OriginSize, false, mp)

		case plan.MetadataScanInfo_MIN: // TODO: find a way to show this info
			vector.AppendBytes(opBat.Vecs[i], zm.GetMinBuf(), zmNull, mp)

		case plan.MetadataScanInfo_MAX: // TODO: find a way to show this info
			vector.AppendBytes(opBat.Vecs[i], zm.GetMaxBuf(), zmNull, mp)

		case plan.MetadataScanInfo_SUM: // TODO: find a way to show this info
			vector.AppendBytes(opBat.Vecs[i], zm.GetSumBuf(), zmNull, mp)

		case plan.MetadataScanInfo_LEVEL:
			vector.AppendFixed(opBat.Vecs[i], info.Level, false, mp)

		default:
		}
	}

	return nil
}

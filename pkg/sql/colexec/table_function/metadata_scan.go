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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
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

func (s *metadataScanState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	s.startPreamble(tf, proc, nthRow)

	source := tf.ctr.argVecs[0]
	col := tf.ctr.argVecs[1]
	dbname, tablename, colname, err := handleDataSource(source, col)
	logutil.Infof("db: %s, table: %s, col: %s in metadataScan", dbname, tablename, colname)
	if err != nil {
		return err
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

	metaInfos, err := rel.GetColumMetadataScanInfo(proc.Ctx, colname)
	if err != nil {
		return err
	}

	for i := range metaInfos {
		err = fillMetadataInfoBat(s.batch, proc, tf, metaInfos[i])
		if err != nil {
			return err
		}
	}
	s.batch.AddRowCount(len(metaInfos))
	return nil
}

func handleDataSource(source, col *vector.Vector) (string, string, string, error) {
	if source.Length() != 1 || col.Length() != 1 {
		return "", "", "", moerr.NewInternalErrorNoCtx("wrong input len")
	}
	strs := strings.Split(source.GetStringAt(0), ".")
	if len(strs) != 2 {
		return "", "", "", moerr.NewInternalErrorNoCtx("wrong len of db and tbl input")
	}
	return strs[0], strs[1], col.GetStringAt(0), nil
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

		default:
		}
	}

	return nil
}

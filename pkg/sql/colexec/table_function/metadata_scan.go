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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func metadataScanPrepare(proc *process.Process, arg *Argument) (err error) {
	arg.ctr = new(container)
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)

	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return err
}

func metadataScan(_ int, proc *process.Process, arg *Argument) (bool, error) {
	var (
		err         error
		source, col *vector.Vector
		rbat        *batch.Batch
	)
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
		if source != nil {
			source.Free(proc.Mp())
		}
		if col != nil {
			col.Free(proc.Mp())
		}
	}()

	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}

	source, err = arg.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{bat})
	if err != nil {
		return false, err
	}
	col, err = arg.ctr.executorsForArgs[1].Eval(proc, []*batch.Batch{bat})
	if err != nil {
		return false, err
	}

	dbname, tablename, colname, err := handleDatasource(vector.MustStrCol(source), vector.MustStrCol(col))
	if err != nil {
		return false, err
	}

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	db, err := e.Database(proc.Ctx, dbname, proc.TxnOperator)
	if err != nil {
		return false, moerr.NewInternalError(proc.Ctx, "get database failed in metadata scan: %v", err)
	}

	rel, err := db.Relation(proc.Ctx, tablename, nil)
	if err != nil {
		return false, err
	}

	metaInfos, err := rel.GetColumMetadataScanInfo(proc.Ctx, colname)
	if err != nil {
		return false, err
	}

	rbat, err = genRetBatch(*proc, arg, metaInfos, colname, rel)
	if err != nil {
		return false, err
	}

	proc.SetInputBatch(rbat)
	return false, nil
}

func handleDatasource(first []string, second []string) (string, string, string, error) {
	if len(first) != 1 || len(second) != 1 {
		return "", "", "", moerr.NewInternalErrorNoCtx("wrong input len")
	}
	s := first[0]
	strs := strings.Split(s, ".")
	if len(strs) != 2 {
		return "", "", "", moerr.NewInternalErrorNoCtx("wrong len of db and tbl input")
	}
	return strs[0], strs[1], second[0], nil
}

func genRetBatch(proc process.Process, arg *Argument, metaInfos []*plan.MetadataScanInfo, colName string, rel engine.Relation) (*batch.Batch, error) {
	retBat, err := initMetadataInfoBat(proc, arg)
	if err != nil {
		return nil, err
	}

	for i := range metaInfos {
		fillMetadataInfoBat(retBat, proc, arg, metaInfos[i])
	}

	return retBat, nil
}

func initMetadataInfoBat(proc process.Process, arg *Argument) (*batch.Batch, error) {
	retBat := batch.New(false, arg.Attrs)
	retBat.Cnt = 1

	for i, a := range arg.Attrs {
		idx, ok := plan.MetadataScanInfo_MetadataScanInfoType_value[a]
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", a)
		}

		tp := plan2.MetadataScanColTypes[idx]
		retBat.Vecs[i] = vector.NewVec(tp)
	}

	return retBat, nil
}

func fillMetadataInfoBat(opBat *batch.Batch, proc process.Process, arg *Argument, info *plan.MetadataScanInfo) error {
	mp := proc.GetMPool()
	for i, colname := range arg.Attrs {
		idx, ok := plan.MetadataScanInfo_MetadataScanInfoType_value[colname]
		if !ok {
			opBat.Clean(proc.GetMPool())
			return moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", colname)
		}

		switch plan.MetadataScanInfo_MetadataScanInfoType(idx) {
		case plan.MetadataScanInfo_COL_NAME:
			vector.AppendBytes(opBat.Vecs[i], []byte(info.ColName), false, mp)

		case plan.MetadataScanInfo_BLOCK_ID:
			var bid types.Blockid
			if err := bid.Unmarshal(info.BlockId); err != nil {
				return err
			}
			vector.AppendFixed(opBat.Vecs[i], bid, false, mp)
		case plan.MetadataScanInfo_OBJECT_NAME:
			vector.AppendBytes(opBat.Vecs[i], []byte(info.ObjectName), false, mp)

		case plan.MetadataScanInfo_ENTRY_STATE:
			vector.AppendFixed(opBat.Vecs[i], info.EntryState, false, mp)

		case plan.MetadataScanInfo_SORTED:
			vector.AppendFixed(opBat.Vecs[i], info.Sorted, false, mp)

		case plan.MetadataScanInfo_IS_HIDDEN:
			vector.AppendFixed(opBat.Vecs[i], info.IsHidden, false, mp)

		case plan.MetadataScanInfo_META_LOC:
			vector.AppendBytes(opBat.Vecs[i], info.MetaLoc, false, mp)

		case plan.MetadataScanInfo_DELTA_LOC:
			vector.AppendBytes(opBat.Vecs[i], info.DelLoc, false, mp)

		case plan.MetadataScanInfo_COMMIT_TS:
			var ts types.TS
			if err := ts.Unmarshal(info.CommitTs); err != nil {
				return err
			}
			vector.AppendFixed(opBat.Vecs[i], ts, false, mp)

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

		case plan.MetadataScanInfo_SEG_ID:
			var sid types.Uuid
			if err := sid.Unmarshal(info.SegId); err != nil {
				return err
			}
			vector.AppendFixed(opBat.Vecs[i], sid, false, mp)

		case plan.MetadataScanInfo_ROWS_CNT:
			vector.AppendFixed(opBat.Vecs[i], info.RowCnt, false, mp)

		case plan.MetadataScanInfo_NULL_CNT:
			vector.AppendFixed(opBat.Vecs[i], info.NullCnt, false, mp)

		case plan.MetadataScanInfo_COMPRESS_SIZE:
			vector.AppendFixed(opBat.Vecs[i], info.CompressSize, false, mp)

		case plan.MetadataScanInfo_ORIGIN_SIZE:
			vector.AppendFixed(opBat.Vecs[i], info.OriginSize, false, mp)

		case plan.MetadataScanInfo_MIN: // TODO: find a way to show this info
			vector.AppendBytes(opBat.Vecs[i], info.Min, false, mp)

		case plan.MetadataScanInfo_MAX: // TODO: find a way to show this info
			vector.AppendBytes(opBat.Vecs[i], info.Max, false, mp)
		default:
		}
	}
	opBat.Zs = append(opBat.Zs, 1)

	return nil
}

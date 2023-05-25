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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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

	rel, err := db.Relation(proc.Ctx, tablename)
	if err != nil {
		return false, err
	}

	metaInfos, err := getMetadataScanInfos(rel, proc, colname)
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

func getMetadataScanInfos(rel engine.Relation, proc *process.Process, colname string) ([]*plan.MetadataScanInfo, error) {
	infobytes, err := rel.GetColumMetadataScanInfo(proc.Ctx, colname)
	if err != nil {
		return nil, err
	}

	return infobytes, nil
}

func genRetBatch(proc process.Process, arg *Argument, metaInfos []*plan.MetadataScanInfo, colName string, rel engine.Relation) (*batch.Batch, error) {
	retBat, err := genMetadataInfoBat(proc, arg)
	if err != nil {
		return nil, err
	}

	for i := range metaInfos {
		fillMetadataInfoBat(retBat, proc, arg, metaInfos[i])
	}

	return retBat, nil
}

func genMetadataInfoBat(proc process.Process, arg *Argument) (*batch.Batch, error) {
	retBat := batch.New(false, arg.Attrs)
	retBat.Cnt = 1

	for i, a := range arg.Attrs {
		idx, ok := plan.MetadataScanInfo_MetadataScanInfoType_value[a]
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", a)
		}

		tp := plan2.MetadataScanColTypes[idx]
		fmt.Printf("[genMetadataInfoBat] name: %s -> type: %s\n", a, tp.String())
		retBat.Vecs[i] = vector.NewVec(tp)
	}

	return retBat, nil
}

func fillMetadataInfoBat(opBat *batch.Batch, proc process.Process, arg *Argument, info *plan.MetadataScanInfo) error {
	mp := proc.GetMPool()
	for i, a := range arg.Attrs {
		idx, ok := plan.MetadataScanInfo_MetadataScanInfoType_value[a]
		if !ok {
			opBat.Clean(proc.GetMPool())
			return moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", a)
		}

		switch plan.MetadataScanInfo_MetadataScanInfoType(idx) {
		case plan.MetadataScanInfo_COL_NAME:
			vector.AppendBytes(opBat.Vecs[i], []byte(info.ColName), false, mp)

		case plan.MetadataScanInfo_BLOCK_ID:
			var bid types.Blockid
			if err := bid.Unmarshal(info.BlockId); err != nil {
				return err
			}
			vector.AppendAny(opBat.Vecs[i], bid, false, mp)

		case plan.MetadataScanInfo_ENTRY_STATE:
			vector.AppendFixed(opBat.Vecs[i], info.EntryState, false, mp)

		case plan.MetadataScanInfo_SORTED:
			vector.AppendFixed(opBat.Vecs[i], info.Sorted, false, mp)

		case plan.MetadataScanInfo_IS_HIDDEN:
			vector.AppendFixed(opBat.Vecs[i], info.IsHidden, false, mp)

		case plan.MetadataScanInfo_META_LOC:
			var mLoc catalog.ObjectLocation
			if err := mLoc.Unmarshal(info.MetaLoc); err != nil {
				return err
			}
			vector.AppendBytes(opBat.Vecs[i], mLoc[:], false, mp)

		case plan.MetadataScanInfo_DELTA_LOC:
			var dLoc catalog.ObjectLocation
			if err := dLoc.Unmarshal(info.MetaLoc); err != nil {
				return err
			}
			vector.AppendBytes(opBat.Vecs[i], dLoc[:], false, mp)

		case plan.MetadataScanInfo_COMMIT_TS:
			var ts types.TS
			if err := ts.Unmarshal(info.CommitTs); err != nil {
				return err
			}
			vector.AppendAny(opBat.Vecs[i], ts, false, mp)

		case plan.MetadataScanInfo_CREATE_TS:
			var ts types.TS
			if err := ts.Unmarshal(info.CreateTs); err != nil {
				return err
			}
			vector.AppendAny(opBat.Vecs[i], ts, false, mp)

		case plan.MetadataScanInfo_DELETE_TS:
			var ts types.TS
			if err := ts.Unmarshal(info.DeleteTs); err != nil {
				return err
			}
			vector.AppendAny(opBat.Vecs[i], ts, false, mp)

		case plan.MetadataScanInfo_SEG_ID:
			var sid types.Uuid
			if err := sid.Unmarshal(info.SegId); err != nil {
				return err
			}
			vector.AppendAny(opBat.Vecs[i], sid, false, mp)

		case plan.MetadataScanInfo_ROWS_CNT:
			fmt.Printf("[metadtascan] RowCnt %d\n", info.RowCnt)
			vector.AppendFixed(opBat.Vecs[i], info.RowCnt, false, mp)

		case plan.MetadataScanInfo_NULL_CNT:
			fmt.Printf("[metadtascan] NullCnt %d\n", info.NullCnt)
			vector.AppendFixed(opBat.Vecs[i], info.NullCnt, false, mp)

		case plan.MetadataScanInfo_COMPRESS_SIZE:
			fmt.Printf("[metadtascan] CompressSize %d\n", info.CompressSize)
			vector.AppendFixed(opBat.Vecs[i], info.CompressSize, false, mp)

		case plan.MetadataScanInfo_ORIGIN_SIZE:
			fmt.Printf("[metadtascan] OriginSize %d\n", info.OriginSize)
			vector.AppendFixed(opBat.Vecs[i], info.OriginSize, false, mp)

		case plan.MetadataScanInfo_MIN: // TODO: find a way to show this info
			vector.AppendAny(opBat.Vecs[i], []byte("min"), false, mp)

		case plan.MetadataScanInfo_MAX: // TODO: find a way to show this info
			vector.AppendAny(opBat.Vecs[i], []byte("max"), false, mp)
		default:
		}
		opBat.Zs = append(opBat.Zs, 1)
	}

	return nil
}

/*
func fillMetadataInfoBat(opBat *batch.Batch, proc process.Process, arg *Argument, info *catalog.MetadataScanInfo) error {
	mp := proc.GetMPool()
	for i, a := range arg.Attrs {
		switch a {
		case catalog.MetadataScanInfoNames[catalog.COL_NAME]:
			vector.AppendBytes(opBat.Vecs[i], []byte(info.ColName), false, mp)

		case catalog.MetadataScanInfoNames[catalog.BLOCK_ID]:
			vector.AppendAny(opBat.Vecs[i], info.BlockId, false, mp)

		case catalog.MetadataScanInfoNames[catalog.ENTRY_STATE]:
			vector.AppendFixed(opBat.Vecs[i], info.EntryState, false, mp)

		case catalog.MetadataScanInfoNames[catalog.SORTED]:
			vector.AppendFixed(opBat.Vecs[i], info.Sorted, false, mp)

		case catalog.MetadataScanInfoNames[catalog.META_LOC]:
			vector.AppendBytes(opBat.Vecs[i], info.MetaLoc[:], false, mp)

		case catalog.MetadataScanInfoNames[catalog.DELTA_LOC]:
			vector.AppendBytes(opBat.Vecs[i], info.DelLoc[:], false, mp)

		case catalog.MetadataScanInfoNames[catalog.COMMIT_TS]:
			vector.AppendAny(opBat.Vecs[i], info.CommitTs, false, mp)

		case catalog.MetadataScanInfoNames[catalog.CREATE_TS]:
			vector.AppendAny(opBat.Vecs[i], info.CreateTs, false, mp)

		case catalog.MetadataScanInfoNames[catalog.DELETE_TS]:
			vector.AppendAny(opBat.Vecs[i], info.DeleteTs, false, mp)

		case catalog.MetadataScanInfoNames[catalog.SEG_ID]:
			vector.AppendAny(opBat.Vecs[i], info.SegId, false, mp)

		case catalog.MetadataScanInfoNames[catalog.ROWS_CNT]:
			vector.AppendFixed(opBat.Vecs[i], info.RowCnt, false, mp)

		case catalog.MetadataScanInfoNames[catalog.NULL_CNT]:
			vector.AppendFixed(opBat.Vecs[i], info.NullCnt, false, mp)

		case catalog.MetadataScanInfoNames[catalog.COMPRESS_SIZE]:
			vector.AppendFixed(opBat.Vecs[i], info.CompressSize, false, mp)

		case catalog.MetadataScanInfoNames[catalog.ORIGIN_SIZE]:
			vector.AppendFixed(opBat.Vecs[i], info.OriginSize, false, mp)

		case catalog.MetadataScanInfoNames[catalog.MIN]: // TODO: find a way to show this info
			vector.AppendBytes(opBat.Vecs[i], []byte("min"), false, mp)

		case catalog.MetadataScanInfoNames[catalog.MAX]: // TODO: find a way to show this info
			vector.AppendBytes(opBat.Vecs[i], []byte("max"), false, mp)

		default:
			opBat.Clean(proc.GetMPool())
			return moerr.NewInvalidInput(proc.Ctx, "bad input select columns name %v", a)
		}
	}
	opBat.Zs = append(opBat.Zs, 1)
	return nil
}
*/

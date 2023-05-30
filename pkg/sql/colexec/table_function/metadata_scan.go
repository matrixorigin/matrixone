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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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

func getMetadataScanInfos(rel engine.Relation, proc *process.Process, colname string) ([]*catalog.MetadataScanInfo, error) {
	if _, err := rel.Ranges(proc.Ctx, nil); err != nil {
		return nil, err
	}

	infobytes, err := rel.GetMetadataScanInfoBytes(proc.Ctx, colname)
	if err != nil {
		return nil, err
	}

	metaInfos := make([]*catalog.MetadataScanInfo, 0, len(infobytes))
	for i := range infobytes {
		metaInfos = append(metaInfos, catalog.DecodeMetadataScanInfo(infobytes[i]))
	}

	return metaInfos, nil
}

func genRetBatch(proc process.Process, arg *Argument, metaInfos []*catalog.MetadataScanInfo, colName string, rel engine.Relation) (*batch.Batch, error) {
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
		switch a {
		case catalog.MetadataScanInfoNames[catalog.COL_NAME]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.COL_NAME])

		case catalog.MetadataScanInfoNames[catalog.BLOCK_ID]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.BLOCK_ID])

		case catalog.MetadataScanInfoNames[catalog.ENTRY_STATE]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.ENTRY_STATE])

		case catalog.MetadataScanInfoNames[catalog.SORTED]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.SORTED])

		case catalog.MetadataScanInfoNames[catalog.META_LOC]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.META_LOC])

		case catalog.MetadataScanInfoNames[catalog.DELTA_LOC]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.DELTA_LOC])

		case catalog.MetadataScanInfoNames[catalog.COMMIT_TS]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.COMMIT_TS])

		case catalog.MetadataScanInfoNames[catalog.SEG_ID]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.SEG_ID])

		case catalog.MetadataScanInfoNames[catalog.ROWS_CNT]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.ROWS_CNT])

		case catalog.MetadataScanInfoNames[catalog.NULL_CNT]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.NULL_CNT])

		case catalog.MetadataScanInfoNames[catalog.COMPRESS_SIZE]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.COMPRESS_SIZE])

		case catalog.MetadataScanInfoNames[catalog.ORIGIN_SIZE]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.ORIGIN_SIZE])

		case catalog.MetadataScanInfoNames[catalog.MIN]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.MIN])

		case catalog.MetadataScanInfoNames[catalog.MAX]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.MAX])

		default:
			retBat.Clean(proc.GetMPool())
			return nil, moerr.NewInvalidInput(proc.Ctx, "bad input select columns name %v", a)
		}
	}

	return retBat, nil
}

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

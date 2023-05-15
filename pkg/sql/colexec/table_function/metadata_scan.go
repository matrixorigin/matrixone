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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ColunmsInfo struct {
	Name       []string
	BlockID    []types.Blockid
	EntryState []bool
	Sorted     []bool
	MetaLoc    []catalog.ObjectLocation
	DeltaLoc   []catalog.ObjectLocation
	CommitTs   []types.TS
	SegmentID  []types.Uuid
}

func metadataScanPrepare(_ *process.Process, arg *Argument) error {
	return nil
}
func metadataScan(_ int, proc *process.Process, arg *Argument) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}

	tb, err := colexec.EvalExpr(bat, proc, arg.Args[0])
	if err != nil {
		return false, err
	}
	col, err := colexec.EvalExpr(bat, proc, arg.Args[1])
	if err != nil {
		return false, err
	}
	fmt.Printf("[metadatascan] first: %s, second: %s\n", tb, col)
	fmt.Printf("arg.Attrs = %s\n", arg.Attrs)

	dbname, tablename, colname, err := handleDatasourceInfo(vector.MustStrCol(tb), vector.MustStrCol(col))
	if err != nil {
		return false, err
	}
	fmt.Printf("[metadatascan] db: %s, table: %s, column: %s\n", dbname, tablename, colname)
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	db, err := e.Database(proc.Ctx, dbname, proc.TxnOperator)
	if err != nil {
		return false, err
	}
	fmt.Printf("[metadatascan] get db %s success\n", dbname)

	rel, err := db.Relation(proc.Ctx, tablename)
	if err != nil {
		return false, err
	}
	fmt.Printf("[metadatascan] get db table %s success\n", tablename)
	if _, err := rel.Ranges(proc.Ctx, nil); err != nil {
		return false, err
	}

	infobytes, err := rel.GetColumMetadataScanInfo(proc.Ctx, colname)
	if err != nil {
		return false, nil
	}
	fmt.Printf("[metadatascan] get infobytes success\n")

	metaInfos := make([]*catalog.MetadataScanInfo, 0, len(infobytes))
	for i := range infobytes {
		metaInfos = append(metaInfos, catalog.DecodeMetadataScanInfo(infobytes[i]))
	}

	retb, err := genRetBatch(*proc, arg, metaInfos, colname, rel)
	if err != nil {
		return false, err
	}

	proc.SetInputBatch(retb)
	return true, nil
}

func handleDatasourceInfo(first []string, second []string) (string, string, string, error) {
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

		case catalog.MetadataScanInfoNames[catalog.ROW_CNT]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.ROW_CNT])

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

		case catalog.MetadataScanInfoNames[catalog.ROW_CNT]:
			vector.AppendFixed(opBat.Vecs[i], info.RowCnt, false, mp)

		case catalog.MetadataScanInfoNames[catalog.NULL_CNT]:
			vector.AppendFixed(opBat.Vecs[i], info.NullCnt, false, mp)

		case catalog.MetadataScanInfoNames[catalog.COMPRESS_SIZE]:
			vector.AppendFixed(opBat.Vecs[i], info.CompressSize, false, mp)

		case catalog.MetadataScanInfoNames[catalog.ORIGIN_SIZE]:
			vector.AppendFixed(opBat.Vecs[i], info.OriginSize, false, mp)

		case catalog.MetadataScanInfoNames[catalog.MIN]:
			vector.AppendBytes(opBat.Vecs[i], []byte("min"), false, mp)

		case catalog.MetadataScanInfoNames[catalog.MAX]:
			vector.AppendBytes(opBat.Vecs[i], []byte("max"), false, mp)

		default:
			opBat.Clean(proc.GetMPool())
			return moerr.NewInvalidInput(proc.Ctx, "bad input select columns name %v", a)
		}
	}
	opBat.Zs = append(opBat.Zs, 1)
	return nil
}

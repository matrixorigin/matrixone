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

	dd, ttt, cc, err := handleDbAndTable(vector.MustStrCol(tb), vector.MustStrCol(col))
	if err != nil {
		return false, err
	}
	fmt.Printf("[metadatascan] db: %s, table: %s, column: %s\n", dd, ttt, cc)
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	db, err := e.Database(proc.Ctx, dd, proc.TxnOperator)
	if err != nil {
		return false, err
	}
	fmt.Printf("[metadatascan] get db %s success\n", dd)

	rel, err := db.Relation(proc.Ctx, ttt)
	if err != nil {
		return false, err
	}
	fmt.Printf("[metadatascan] get db table %s success\n", ttt)
	rs, err := rel.Ranges(proc.Ctx, nil)
	if err != nil {
		return false, err
	}

	blockinfos := make([]*catalog.BlockInfo, 0, 1)
	fmt.Printf("[metadatascan] ranges = %s\n", rs)
	for i := range rs {
		if i == 0 {
			continue
		}
		blockinfos = append(blockinfos, catalog.DecodeBlockInfo(rs[i]))
	}

	retb, err := genRetBatch(*proc, arg, blockinfos, cc, rel)
	if err != nil {
		return false, err
	}

	proc.SetInputBatch(retb)
	return true, nil
}

func handleDbAndTable(first []string, second []string) (string, string, string, error) {
	if len(first) != 1 {
		return "", "", "", moerr.NewInternalErrorNoCtx("wrong len of first(more than 1)")
	}
	s := first[0]
	strs := strings.Split(s, ".")
	if len(strs) != 2 {
		return "", "", "", moerr.NewInternalErrorNoCtx("wrong len of first(xx.xx)")
	}
	if len(second) != 1 {
		return "", "", "", moerr.NewInternalErrorNoCtx("wrong len of second(xx.xx)")
	}
	return strs[0], strs[1], second[0], nil
}

func genRetBatch(proc process.Process, arg *Argument, blockInfos []*catalog.BlockInfo, colName string, rel engine.Relation) (*batch.Batch, error) {
	retBat, err := genMetadataInfoBat(proc, arg)
	if err != nil {
		return nil, err
	}
	mp := proc.GetMPool()

	cols, err := rel.TableColumns(proc.Ctx)
	if err != nil {
		retBat.Clean(mp)
		return nil, err
	}

	if colName != "*" {
		found := false
		for i := range cols {
			if cols[i].Name == colName {
				found = true
				break
			}
		}
		if !found {
			retBat.Clean(mp)
			return nil, moerr.NewInvalidInput(proc.Ctx, "wrong columns input %v", colName)
		}
	}

	for i := range blockInfos {
		if colName != "*" {
			for j, a := range arg.Attrs {
				switch a {
				case catalog.MoTableMetaColName:
					vector.AppendBytes(retBat.Vecs[j], []byte(colName), false, mp)
				case catalog.MoTableMetaBlockID:
					vector.AppendAny(retBat.Vecs[j], blockInfos[i].BlockID, false, mp)
				case catalog.MoTableMetaEntryState:
					vector.AppendFixed(retBat.Vecs[j], blockInfos[i].EntryState, false, mp)
				case catalog.MoTableMetaSorted:
					vector.AppendFixed(retBat.Vecs[j], blockInfos[i].Sorted, false, mp)
				case catalog.MoTableMetaLoc:
					vector.AppendAny(retBat.Vecs[j], blockInfos[i].MetaLoc[:], false, mp)
				case catalog.MoTableMetaDelLoc:
					vector.AppendAny(retBat.Vecs[j], blockInfos[i].DeltaLoc[:], false, mp)
				case catalog.MoTableMetaCommitTS:
					vector.AppendAny(retBat.Vecs[j], blockInfos[i].CommitTs, false, mp)
				case catalog.MoTableMetaSegID:
					vector.AppendAny(retBat.Vecs[j], blockInfos[i].SegmentID, false, mp)
				default:
					retBat.Clean(mp)
					return nil, moerr.NewInvalidInput(proc.Ctx, "wrong columns %v", a)
				}
			}
		} else {
			cols, err := rel.TableColumns(proc.Ctx)
			if err != nil {
				return nil, err
			}
			for k := range cols {
				if cols[k].Name == "__mo_rowid" {
					continue
				}
				for j, a := range arg.Attrs {
					switch a {
					case catalog.MoTableMetaColName:
						vector.AppendBytes(retBat.Vecs[j], []byte(cols[k].Name), false, mp)
					case catalog.MoTableMetaBlockID:
						vector.AppendAny(retBat.Vecs[j], blockInfos[i].BlockID, false, mp)
					case catalog.MoTableMetaEntryState:
						vector.AppendFixed(retBat.Vecs[j], blockInfos[i].EntryState, false, mp)
					case catalog.MoTableMetaSorted:
						vector.AppendFixed(retBat.Vecs[j], blockInfos[i].Sorted, false, mp)
					case catalog.MoTableMetaLoc:
						vector.AppendAny(retBat.Vecs[j], blockInfos[i].MetaLoc[:], false, mp)
					case catalog.MoTableMetaDelLoc:
						vector.AppendAny(retBat.Vecs[j], blockInfos[i].DeltaLoc[:], false, mp)
					case catalog.MoTableMetaCommitTS:
						vector.AppendAny(retBat.Vecs[j], blockInfos[i].CommitTs, false, mp)
					case catalog.MoTableMetaSegID:
						vector.AppendAny(retBat.Vecs[j], blockInfos[i].SegmentID, false, mp)
					default:
						retBat.Clean(mp)
						return nil, moerr.NewInvalidInput(proc.Ctx, "wrong columns %v", a)
					}
				}
			}
		}
	}
	retBat.SetZs(len(blockInfos), mp)

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
		case catalog.MetadataScanInfoNames[catalog.META_SEG]:
			retBat.Vecs[i] = vector.NewVec(catalog.MetadataScanInfoTypes[catalog.META_SEG])
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

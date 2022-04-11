// Copyright 2021 Matrix Origin
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

package engine

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
	"sort"
	"strings"
)

var (
	errorMismatchRefcntWithAttributeCnt          = errors.New("mismatch refcnts and attribute cnt")
	errorSomeAttributeNamesAreNotInAttributeDesc = errors.New("some attriute names are not in attribute desc")
	errorInvalidParameters                       = errors.New("invalid parameters")
	errorDifferentReadAttributesInSameReader     = errors.New("different attributes in same reader")
	errorVectorIsInvalid                         = errors.New("vector is invalid")
)

func (tr *TpeReader) NewFilter() engine.Filter {
	return nil
}

func (tr *TpeReader) NewSummarizer() engine.Summarizer {
	return nil
}

func (tr *TpeReader) NewSparseFilter() engine.SparseFilter {
	return nil
}

func (tr *TpeReader) Read(refCnts []uint64, attrs []string) (*batch.Batch, error) {
	if tr.isDumpReader {
		//read nothing
		return nil, nil
	}
	if len(refCnts) == 0 || len(attrs) == 0 {
		return nil, errorInvalidParameters
	}
	if len(refCnts) != len(attrs) {
		return nil, errorMismatchRefcntWithAttributeCnt
	}

	attrSet := make(map[string]uint32)
	for _, tableAttr := range tr.tableDesc.Attributes {
		attrSet[tableAttr.Name] = tableAttr.ID
	}

	//check if the attribute is in the relation
	var readAttrs []*descriptor.AttributeDesc
	for _, attr := range attrs {
		if attrID, exist := attrSet[attr]; exist {
			readAttrs = append(readAttrs, &tr.tableDesc.Attributes[attrID])
		} else {
			return nil, errorSomeAttributeNamesAreNotInAttributeDesc
		}
	}

	var bat *batch.Batch
	var err error

	if tr.readCtx == nil {
		tr.readCtx = &tuplecodec.ReadContext{
			DbDesc:              tr.dbDesc,
			TableDesc:           tr.tableDesc,
			IndexDesc:           &tr.tableDesc.Primary_index,
			ReadAttributesNames: attrs,
			ReadAttributeDescs:  readAttrs,
			ParallelReader:      tr.parallelReader,
			MultiNode:           tr.multiNode,
			ReadCount:           0,
		}

		if tr.readCtx.ParallelReader || tr.readCtx.MultiNode {
			tr.readCtx.ParallelReaderContext = tuplecodec.ParallelReaderContext{
				ID:                   tr.id,
				ShardIndex:           0,
				ShardStartKey:        tr.shardInfos[0].startKey,
				ShardEndKey:          tr.shardInfos[0].endKey,
				ShardNextScanKey:     tr.shardInfos[0].startKey,
				ShardScanEndKey:      nil,
				CompleteInShard:      tr.shardInfos[0].completeInShard,
				ReadCnt:              0,
				CountOfWithoutPrefix: 0,
			}

			logutil.Infof("reader %d info --> shard %v readCtx %v",
				tr.id,
				tr.shardInfos,
				tr.readCtx.ParallelReaderContext,
			)
		} else {
			tr.readCtx.SingleReaderContext = tuplecodec.SingleReaderContext{
				CompleteInAllShards:      false,
				PrefixForScanKey:         nil,
				LengthOfPrefixForScanKey: 0,
			}
		}
	} else {
		//check if these attrs are same as last attrs
		if len(tr.readCtx.ReadAttributesNames) != len(attrs) {
			return nil, errorDifferentReadAttributesInSameReader
		}

		for i := 0; i < len(attrs); i++ {
			if attrs[i] != tr.readCtx.ReadAttributesNames[i] {
				return nil, errorDifferentReadAttributesInSameReader
			}
		}

		if tr.readCtx.ParallelReader || tr.readCtx.MultiNode {
			logutil.Infof("reader %d info --> readCtx %v",
				tr.id,
				tr.readCtx.ParallelReaderContext,
			)
			//update new shard if needed
			if tr.readCtx.CompleteInShard {
				tr.shardInfos[tr.readCtx.ShardIndex].completeInShard = true
				shardIdx := tr.readCtx.ShardIndex
				shardIdx++
				id := tr.readCtx.ID
				if shardIdx < len(tr.shardInfos) {
					tr.readCtx.ParallelReaderContext.Reset()
					tr.readCtx.ParallelReaderContext.Set(id, shardIdx)
					tr.readCtx.ParallelReaderContext.SetShardInfo(tr.shardInfos[shardIdx].startKey,
						tr.shardInfos[shardIdx].endKey,
						tr.shardInfos[shardIdx].nextScanKey,
						nil)
					logutil.Infof("reader %d switch from %v to %v--> readCtx %v",
						tr.id,
						tr.shardInfos[tr.readCtx.ShardIndex-1],
						tr.shardInfos[tr.readCtx.ShardIndex],
						tr.readCtx.ParallelReaderContext,
					)
				} else {
					return nil, nil
				}
			}
		}
	}

	bat, err = tr.computeHandler.Read(tr.readCtx)
	if err != nil {
		return nil, err
	}

	//for test
	if tr.readCtx.ParallelReader && tr.multiNode {
		cnt := 0
		if bat != nil {
			cnt = vector.Length(bat.Vecs[0])

			var indexes []int = make([]int, len(bat.Vecs))
			for i := 0; i < len(bat.Vecs); i++ {
				indexes[i] = i
			}

			sort.Slice(indexes, func(i, j int) bool {
				ai := indexes[i]
				bi := indexes[j]
				a := attrs[ai]
				b := attrs[bi]
				return strings.Compare(a, b) < 0
			})

			logutil.Infof("store id %d reader %d readCount %d parallelContext %v ", tr.storeID, tr.id, cnt, tr.readCtx.ParallelReaderContext)
			row := make([]interface{}, len(bat.Vecs))

			var names []string
			for _, index := range indexes {
				names = append(names, attrs[index])
			}
			logutil.Infof("attrs %v", attrs)
			logutil.Infof("attrs_names %v", names)
			for rowIndex := 0; rowIndex < cnt; rowIndex++ {
				buf := &bytes.Buffer{}
				buf.WriteString(fmt.Sprintf("batchrow rowIndex %d ]", rowIndex))
				for i := 0; i < len(bat.Vecs); i++ {
					k := indexes[i]
					vec := bat.Vecs[k]
					switch vec.Typ.Oid { //get col
					case types.T_int8:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]int8)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]int8)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_uint8:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]uint8)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]uint8)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_int16:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]int16)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]int16)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_uint16:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]uint16)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]uint16)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_int32:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]int32)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]int32)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_uint32:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]uint32)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]uint32)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_int64:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]int64)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]int64)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_uint64:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]uint64)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]uint64)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_float32:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]float32)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]float32)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_float64:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]float64)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]float64)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_char:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.(*types.Bytes)
							row[i] = string(vs.Get(int64(rowIndex)))
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.(*types.Bytes)
								row[i] = string(vs.Get(int64(rowIndex)))
							}
						}
					case types.T_varchar:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.(*types.Bytes)
							row[i] = string(vs.Get(int64(rowIndex)))
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.(*types.Bytes)
								row[i] = string(vs.Get(int64(rowIndex)))
							}
						}
					case types.T_date:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]types.Date)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]types.Date)
								row[i] = vs[rowIndex]
							}
						}
					case types.T_datetime:
						if !nulls.Any(vec.Nsp) { //all data in this column are not null
							vs := vec.Col.([]types.Datetime)
							row[i] = vs[rowIndex]
						} else {
							if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
								row[i] = nil
							} else {
								vs := vec.Col.([]types.Datetime)
								row[i] = vs[rowIndex]
							}
						}
					default:
						logutil.Errorf("reader.Read : unsupported type %d \n", vec.Typ.Oid)
						return nil, fmt.Errorf("reader.Read : unsupported type %d \n", vec.Typ.Oid)
					}
					buf.WriteString(fmt.Sprintf("colname %v typ %v value %v ", attrs[k], vec.Typ, row[i]))
				}
				logutil.Infof("%s", buf.String())
			}
		}
	}

	//when bat is null,it means no data anymore.
	if bat != nil {
		//attach refCnts
		for i, ref := range refCnts {
			bat.Vecs[i].Ref = ref
		}
		for _, vec := range bat.Vecs {
			if !vec.Or || vec.Data == nil {
				return nil, errorVectorIsInvalid
			}
		}
	}
	return bat, err
}

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

package tempengine

import (
	"bytes"
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/pierrec/lz4"
)

func (tempRelation *TempRelation) Rows() int64 {
	return int64(tempRelation.rows)
}

func (tempRelation *TempRelation) Size(_ string) int64 {
	return 0
}

func (tempRelation *TempRelation) Ranges(context.Context) ([][]byte, error) {
	return nil, nil
}

func (tempRelation *TempRelation) TableDefs(context.Context) ([]engine.TableDef, error) {
	// now we just support normal attribute, later we will add primary key,auto_crement,partition key
	// and so on
	defs := make([]engine.TableDef, len(tempRelation.tblSchema.attrs))
	for i := 0; i < len(defs); i++ {
		defs[i] = &engine.AttributeDef{Attr: tempRelation.tblSchema.attrs[i]}
	}
	return defs, nil
}

func (tempRelation *TempRelation) GetPrimaryKeys(context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (tempRelation *TempRelation) GetHideKeys(context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

// a batch is a block
func (tempRelation *TempRelation) Write(_ context.Context, bat *batch.Batch) error {
	// first get this batch id
	batId := tempRelation.blockNums
	db_tblName := tempRelation.tblSchema.tblName
	for i, vec := range bat.Vecs {
		// let vec data serializ as bytedata
		seriData, err := vec.Show()
		if err != nil {
			return err
		}
		// after serialize vec, we need to see if there is a compress algorithm require
		// for now, we just have Lz4
		if tempRelation.tblSchema.attrs[i].Alg == compress.Lz4 {
			data := make([]byte, lz4.CompressBlockBound(len(seriData)))
			if data, err = compress.Compress(seriData, data, compress.Lz4); err != nil {
				return err
			}
			// store the origin seriData's size in the last
			length := int32(len(seriData))
			data = append(data, types.EncodeInt32(&length)...)
			seriData = data
		}
		tempRelation.bytesData[db_tblName+"-"+fmt.Sprintf("%d", batId)+"-"+bat.Attrs[i]] = seriData
	}
	tempRelation.blockNums++
	tempRelation.rows += uint64(batch.Length(bat))
	return nil
}

func (tempRelation *TempRelation) Update(context.Context, *batch.Batch) error {
	return nil
}

func (tempRelation *TempRelation) Delete(context.Context, *vector.Vector, string) error {
	return nil
}

func (tempRelation *TempRelation) Truncate(context.Context) (uint64, error) {
	return 0, nil
}

func (tempRelation *TempRelation) AddTableDef(context.Context, engine.TableDef) error {
	return nil
}

func (tempRelation *TempRelation) DelTableDef(context.Context, engine.TableDef) error {
	return nil
}

func (tempRelation *TempRelation) GetTableID(context.Context) string {
	return ""
}

// second argument is the number of reader, third argument is the filter extend, foruth parameter is the payload required by the engine
func (tempRelation *TempRelation) NewReader(ctx context.Context, partitionNums int, _ *plan.Expr, _ [][]byte) ([]engine.Reader, error) {
	// we will use the blockNums parameters
	readers := make([]TempReader, partitionNums)
	for i := 0; i < int(tempRelation.blockNums); i++ {
		idx := i % partitionNums
		readers[idx].blockIdxs = append(readers[idx].blockIdxs, uint64(i))
		if readers[idx].ownRelation == nil {
			readers[idx].ownRelation = tempRelation
			for j := 0; j < len(tempRelation.tblSchema.attrs); j++ {
				readers[idx].helperBufs = append(readers[idx].helperBufs, bytes.NewBuffer(make([]byte, 0, 1024)))
				readers[idx].decodeBufs = append(readers[idx].decodeBufs, bytes.NewBuffer(make([]byte, 0, 1024)))
			}
		}
	}
	res := make([]engine.Reader, partitionNums)
	for i := 0; i < partitionNums; i++ {
		if readers[i].ownRelation == nil {
			readers[i].ownRelation = tempRelation
		}
		res[i] = &readers[i]
	}
	return res, nil
}

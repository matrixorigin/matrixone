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

package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
)

func BuildColumnIndex(writer objectio.Writer, block objectio.BlockObject, colDef *catalog.ColDef, columnData containers.Vector, isPk, isSorted bool) (metas []indexwrapper.IndexMeta, err error) {
	zmPos := 0

	zoneMapWriter := indexwrapper.NewZMWriter()
	if err = zoneMapWriter.Init(writer, block, indexwrapper.Plain, uint16(colDef.Idx), uint16(zmPos)); err != nil {
		return
	}
	if isSorted && columnData.Length() > 2 {
		slimForZmVec := containers.MakeVector(columnData.GetType(), columnData.Nullable())
		slimForZmVec.Append(columnData.Get(0))
		slimForZmVec.Append(columnData.Get(columnData.Length() - 1))
		err = zoneMapWriter.AddValues(slimForZmVec)
	} else {
		err = zoneMapWriter.AddValues(columnData)
	}
	if err != nil {
		return
	}
	zmMeta, err := zoneMapWriter.Finalize()
	if err != nil {
		return
	}
	metas = append(metas, *zmMeta)

	if !isPk {
		return
	}

	/*bfPos := 1
	bfWriter := indexwrapper.NewBFWriter()
	bfFile, err := file.OpenIndexFile(bfPos)
	if err != nil {
		return
	}
	defer bfFile.Unref()
	if err = bfWriter.Init(bfFile, indexwrapper.Plain, uint16(colDef.Idx), uint16(bfPos)); err != nil {
		return
	}
	if err = bfWriter.AddValues(columnData); err != nil {
		return
	}
	bfMeta, err := bfWriter.Finalize()
	if err != nil {
		return
	}
	metas = append(metas, *bfMeta)*/
	return
}

func BuildBlockIndex(writer objectio.Writer, block objectio.BlockObject, meta *catalog.BlockEntry, columnsData *containers.Batch) (err error) {
	schema := meta.GetSchema()
	blkMetas := indexwrapper.NewEmptyIndicesMeta()
	// ATTENTION: COMPOUNDPK
	pkIdx := -10086
	if schema.HasPK() {
		pkIdx = schema.GetSingleSortKey().Idx
	}

	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		data := columnsData.GetVectorByName(colDef.GetName())
		isPk := colDef.Idx == pkIdx
		// FIXME: there are several sorted column if compound pk exists?
		colMetas, err := BuildColumnIndex(writer, block, colDef, data, isPk, isPk)
		if err != nil {
			return err
		}
		blkMetas.AddIndex(colMetas...)
	}
	return nil
}

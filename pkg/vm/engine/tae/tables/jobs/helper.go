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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

func BuildColumnIndex(writer objectio.Writer, block objectio.BlockObject, colDef *catalog.ColDef, columnData containers.Vector, isPk, isSorted bool) (metas []indexwrapper.IndexMeta, err error) {
	zmPos := 0

	zoneMapWriter := indexwrapper.NewZMWriter()
	if err = zoneMapWriter.Init(writer, block, common.Plain, uint16(colDef.Idx), uint16(zmPos)); err != nil {
		return
	}
	if isSorted && isPk && columnData.Length() > 2 {
		slimForZmVec := containers.MakeVector(columnData.GetType(), columnData.Nullable())
		defer slimForZmVec.Close()
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

	bfPos := 1
	bfWriter := indexwrapper.NewBFWriter()
	if err = bfWriter.Init(writer, block, common.Plain, uint16(colDef.Idx), uint16(bfPos)); err != nil {
		return
	}
	if err = bfWriter.AddValues(columnData); err != nil {
		return
	}
	bfMeta, err := bfWriter.Finalize()
	if err != nil {
		return
	}
	metas = append(metas, *bfMeta)
	return
}

func BuildBlockIndex(writer objectio.Writer, block objectio.BlockObject, schema *catalog.Schema, columnsData *containers.Batch, isSorted bool) (err error) {
	blkMetas := indexwrapper.NewEmptyIndicesMeta()
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
		colMetas, err := BuildColumnIndex(writer, block, colDef, data, isPk, isSorted)
		if err != nil {
			return err
		}
		blkMetas.AddIndex(colMetas...)
	}
	return nil
}

type delSegTask struct {
	*tasks.BaseTask
	delSegs []*catalog.SegmentEntry
	txn     txnif.AsyncTxn
}

func NewDelSegTask(ctx *tasks.Context, txn txnif.AsyncTxn, delSegs []*catalog.SegmentEntry) *delSegTask {
	task := &delSegTask{
		delSegs: delSegs,
		txn:     txn,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return task
}

func (t *delSegTask) String() string {
	segs := "DelSeg:"
	for _, seg := range t.delSegs {
		segs = fmt.Sprintf("%s%d,", segs, seg.GetID())
	}
	return segs
}

func (t *delSegTask) Execute() (err error) {
	tdesc := t.String()
	logutil.Info("Mergeblocks delete merged segments [Start]", zap.String("task", tdesc))
	dbId := t.delSegs[0].GetTable().GetDB().ID
	database, err := t.txn.GetDatabaseByID(dbId)
	if err != nil {
		return
	}
	relId := t.delSegs[0].GetTable().ID
	rel, err := database.GetRelationByID(relId)
	if err != nil {
		return
	}
	for _, entry := range t.delSegs {
		if err = rel.SoftDeleteSegment(entry.GetID()); err != nil {
			return
		}
	}
	logutil.Info("Mergeblocks delete merged segments [Done]", zap.String("task", tdesc))
	return
}

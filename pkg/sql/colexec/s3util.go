// Copyright 2022 Matrix Origin
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

package colexec

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// WriteS3Threshold when batches'  size of table reaches this, we will
	// trigger write s3
	WriteS3Threshold uint64 = 128 * mpool.MB
)

type CNS3Writer struct {
	_sinker       *ioutil.Sinker
	_written      []objectio.ObjectStats
	_blockInfoBat *batch.Batch

	_hold                   []*batch.Batch
	_holdFlushUntilSyncCall bool

	_isTombstone bool
}

func NewCNS3TombstoneWriter(
	mp *mpool.MPool,
	fs fileservice.FileService,
	pkType types.Type) *CNS3Writer {

	writer := &CNS3Writer{}

	writer._sinker = ioutil.NewTombstoneSinker(
		objectio.HiddenColumnSelection_None,
		pkType, mp, fs,
		ioutil.WithMemorySizeThreshold(int(WriteS3Threshold)),
		ioutil.WithTailSizeCap(0))

	writer._isTombstone = true
	writer.ResetBlockInfoBat()

	return writer
}

func NewCNS3DataWriter(
	mp *mpool.MPool,
	fs fileservice.FileService,
	tableDef *plan.TableDef,
	holdFlushUntilSyncCall bool,
) *CNS3Writer {

	var (
		sequms       []uint16
		sortKeyIdx   = -1
		isPrimaryKey bool

		attrs     []string
		attrTypes []types.Type
	)

	for idx, colDef := range tableDef.Cols {
		if colDef.Name == tableDef.Pkey.PkeyColName && !catalog.IsFakePkName(colDef.Name) {
			sortKeyIdx = idx
			isPrimaryKey = true
			break
		}
	}

	if sortKeyIdx == -1 && tableDef.ClusterBy != nil {
		// the rowId column has been excluded from the TableDef of the target table for the insert statements(insert,load).
		// link: pkg/sql/plan/build_constraint_util.go --> func setTableExprToDmlTableInfo,
		// and the sortKeyIdx position can be directly obtained by using a name that matches the sorting key.
		for idx, colDef := range tableDef.Cols {
			if colDef.Name == tableDef.ClusterBy.Name {
				sortKeyIdx = idx
			}
		}
	}

	for i, colDef := range tableDef.Cols {
		if colDef.Name != catalog.Row_ID {
			sequms = append(sequms, uint16(colDef.Seqnum))
			attrs = append(attrs, colDef.Name)

			attrTypes = append(attrTypes, types.New(types.T(colDef.Typ.Id), colDef.Typ.Width, colDef.Typ.Scale))
		} else {
			// check rowid as the last column
			if i != len(tableDef.Cols)-1 {
				logutil.Errorf("bad rowid position for %q, %+v", tableDef.Name, colDef)
			}
		}
	}
	logutil.Debugf("s3 table set from NewS3Writer %q seqnums: %+v", tableDef.Name, sequms)

	writer := &CNS3Writer{
		_holdFlushUntilSyncCall: holdFlushUntilSyncCall,
	}

	factor := ioutil.NewFSinkerImplFactory(sequms, sortKeyIdx, isPrimaryKey, false, tableDef.Version)

	writer._sinker = ioutil.NewSinker(
		sortKeyIdx, attrs, attrTypes,
		factor, mp, fs,
		ioutil.WithTailSizeCap(0),
		ioutil.WithMemorySizeThreshold(int(WriteS3Threshold)))

	writer.ResetBlockInfoBat()

	return writer
}

func (w *CNS3Writer) Write(ctx context.Context, mp *mpool.MPool, bat *batch.Batch) error {
	if w._holdFlushUntilSyncCall {
		copied, err := bat.Dup(mp)
		if err != nil {
			return err
		}
		w._hold = append(w._hold, copied)
	} else {
		return w._sinker.Write(ctx, bat)
	}
	return nil
}

func (w *CNS3Writer) Sync(ctx context.Context, mp *mpool.MPool) ([]objectio.ObjectStats, error) {
	defer func() {
		if len(w._hold) > 0 {
			for _, bat := range w._hold {
				bat.Clean(mp)
			}
			w._hold = w._hold[:0]
		}
	}()

	if len(w._hold) != 0 {
		for _, bat := range w._hold {
			if err := w._sinker.Write(ctx, bat); err != nil {
				return nil, err
			}
		}
	}

	if err := w._sinker.Sync(ctx); err != nil {
		return nil, err
	}

	w._written, _ = w._sinker.GetResult()

	return w._written, nil
}

func (w *CNS3Writer) Close(mp *mpool.MPool) error {
	if w._sinker != nil {
		if err := w._sinker.Close(); err != nil {
			return err
		}
		w._sinker = nil
	}

	if len(w._hold) != 0 {
		for _, bat := range w._hold {
			bat.Clean(mp)
		}
		w._hold = nil
		w._holdFlushUntilSyncCall = false
	}

	if w._blockInfoBat != nil {
		w._blockInfoBat.Clean(mp)
		w._blockInfoBat = nil
	}

	w._written = nil

	return nil
}

func (w *CNS3Writer) FillBlockInfoBat(
	mp *mpool.MPool,
) (*batch.Batch, error) {

	var err error

	if !w._isTombstone {
		objectio.ForeachBlkInObjStatsList(
			true, nil,
			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				if err = vector.AppendBytes(
					w._blockInfoBat.Vecs[0],
					objectio.EncodeBlockInfo(&blk), false, mp); err != nil {
					return false
				}

				return true

			}, w._written...)

		for i := range w._written {
			if err = vector.AppendBytes(w._blockInfoBat.Vecs[1],
				w._written[i].Marshal(), false, mp); err != nil {
				return nil, err
			}
		}
	} else {
		for i := range w._written {
			if err = vector.AppendBytes(w._blockInfoBat.Vecs[0],
				w._written[i].Marshal(), false, mp); err != nil {
				return nil, err
			}
		}
	}

	w._blockInfoBat.SetRowCount(w._blockInfoBat.Vecs[0].Length())
	return w._blockInfoBat, nil
}

func AllocCNS3ResultBat(
	isTombstone bool,
	isMemoryTable bool,
) *batch.Batch {

	var (
		attrs     []string
		attrTypes []types.Type

		blockInfoBat *batch.Batch
	)

	if isMemoryTable {
		attrs = []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
		attrTypes = []types.Type{types.T_int16.ToType(), types.T_text.ToType(), types.T_binary.ToType()}
	} else if !isTombstone {
		attrs = []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
		attrTypes = []types.Type{types.T_text.ToType(), types.T_binary.ToType()}
	} else {
		attrs = []string{catalog.ObjectMeta_ObjectStats}
		attrTypes = []types.Type{types.T_binary.ToType()}
	}

	blockInfoBat = batch.NewWithSize(len(attrs))
	blockInfoBat.Attrs = attrs

	for i := range attrs {
		blockInfoBat.Vecs[i] = vector.NewVec(attrTypes[i])
	}

	return blockInfoBat
}

func (w *CNS3Writer) ResetBlockInfoBat() {

	if w._blockInfoBat != nil {
		w._blockInfoBat.CleanOnlyData()
	}

	w._blockInfoBat = AllocCNS3ResultBat(w._isTombstone, false)
}

// reference to pkg/sql/colexec/order/order.go logic
func SortByKey(
	proc *process.Process,
	bat *batch.Batch,
	sortIndex int,
	allow_null bool,
	m *mpool.MPool,
) error {

	hasNull := false
	// Not-Null Check, notice that cluster by support null value
	if nulls.Any(bat.Vecs[sortIndex].GetNulls()) {
		hasNull = true
		if !allow_null {
			return moerr.NewConstraintViolationf(proc.Ctx,
				"sort key can not be null, sortIndex = %d, sortCol = %s",
				sortIndex, bat.Attrs[sortIndex])
		}
	}
	rowCount := int64(bat.RowCount())
	sels := proc.GetMPool().GetSels()
	defer func() {
		proc.GetMPool().PutSels(sels)
	}()
	for i := int64(0); i < rowCount; i++ {
		sels = append(sels, i)
	}
	ovec := bat.GetVector(int32(sortIndex))
	if allow_null {
		// null last
		sort.Sort(false, true, hasNull, sels, ovec)
	} else {
		sort.Sort(false, false, hasNull, sels, ovec)
	}

	needSort := false
	for i := int64(0); i < int64(rowCount); i++ {
		if sels[i] != i {
			needSort = true
			break
		}
	}
	if needSort {
		return bat.Shuffle(sels, m)
	}
	return nil
}

func (w *CNS3Writer) OutputRawData(
	proc *process.Process,
	result *batch.Batch,
) error {
	defer func() {
		if len(w._hold) > 0 {
			for _, bat := range w._hold {
				bat.Clean(proc.Mp())
			}
			w._hold = w._hold[:0]
		}
	}()

	for _, bat := range w._hold {
		if err := vector.AppendFixed(
			result.Vecs[0], int16(-1), false, proc.Mp()); err != nil {
			return err
		}

		bytes, err := bat.MarshalBinary()
		if err != nil {
			return err
		}
		if err = vector.AppendBytes(
			result.Vecs[1], bytes, false, proc.Mp()); err != nil {
			return err
		}
	}

	result.SetRowCount(result.Vecs[0].Length())

	w._hold = nil

	return nil
}

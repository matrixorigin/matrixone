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
	"bytes"
	"context"
	"fmt"

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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// WriteS3Threshold when batches'  size of table reaches this, we will
	// trigger write s3
	WriteS3Threshold uint64 = 128 * mpool.MB
)

type CNS3Writer struct {
	sinker       *ioutil.Sinker
	written      []objectio.ObjectStats
	blockInfoBat *batch.Batch

	hold                   []*batch.Batch
	holdFlushUntilSyncCall bool

	isTombstone bool
}

func (w *CNS3Writer) String() string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(fmt.Sprintf("Sinker: %s\n", w.sinker.String()))
	buf.WriteString(fmt.Sprintf(
		"Others: {written=%d, isTombstone=%v, holdFlushUntilSyncCall=%v, hold=%v, blockInfoBat=%v}",
		len(w.written), w.isTombstone, w.holdFlushUntilSyncCall, len(w.hold),
		common.MoBatchToString(w.blockInfoBat, w.blockInfoBat.RowCount())))

	return buf.String()
}

func NewCNS3TombstoneWriter(
	mp *mpool.MPool,
	fs fileservice.FileService,
	pkType types.Type) *CNS3Writer {

	writer := &CNS3Writer{}

	writer.sinker = ioutil.NewTombstoneSinker(
		objectio.HiddenColumnSelection_None,
		pkType, mp, fs,
		ioutil.WithMemorySizeThreshold(int(WriteS3Threshold)),
		ioutil.WithTailSizeCap(0))

	writer.isTombstone = true
	writer.ResetBlockInfoBat()

	return writer
}

func GetSequmsAttrsSortKeyIdxFromTableDef(
	tableDef *plan.TableDef,
) ([]uint16, []types.Type, []string, int, bool) {

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

	// create table t1(a int primary key) cluster by a ==> not support
	// the `primary key` and `cluster by` cannot both exist.
	// the condition of sortIdx == -1 may be unnecessary.
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

	return sequms, attrTypes, attrs, sortKeyIdx, isPrimaryKey
}

func NewCNS3DataWriter(
	mp *mpool.MPool,
	fs fileservice.FileService,
	tableDef *plan.TableDef,
	holdFlushUntilSyncCall bool,
) *CNS3Writer {

	writer := &CNS3Writer{
		holdFlushUntilSyncCall: holdFlushUntilSyncCall,
	}

	sequms, attrTypes, attrs, sortKeyIdx, isPrimaryKey := GetSequmsAttrsSortKeyIdxFromTableDef(tableDef)

	factor := ioutil.NewFSinkerImplFactory(sequms, sortKeyIdx, isPrimaryKey, false, tableDef.Version)

	writer.sinker = ioutil.NewSinker(
		sortKeyIdx, attrs, attrTypes,
		factor, mp, fs,
		ioutil.WithTailSizeCap(0),
		ioutil.WithMemorySizeThreshold(int(WriteS3Threshold)))

	writer.ResetBlockInfoBat()

	return writer
}

func (w *CNS3Writer) Write(ctx context.Context, mp *mpool.MPool, bat *batch.Batch) error {
	if w.holdFlushUntilSyncCall {
		copied, err := bat.Dup(mp)
		if err != nil {
			return err
		}
		w.hold = append(w.hold, copied)
	} else {
		return w.sinker.Write(ctx, bat)
	}
	return nil
}

func (w *CNS3Writer) Sync(ctx context.Context, mp *mpool.MPool) ([]objectio.ObjectStats, error) {
	defer func() {
		for _, bat := range w.hold {
			bat.Clean(mp)
		}
		w.hold = nil
	}()

	if len(w.hold) != 0 {
		for _, bat := range w.hold {
			if err := w.sinker.Write(ctx, bat); err != nil {
				return nil, err
			}
		}
	}

	if err := w.sinker.Sync(ctx); err != nil {
		return nil, err
	}

	w.written, _ = w.sinker.GetResult()

	return w.written, nil
}

func (w *CNS3Writer) Close(mp *mpool.MPool) error {
	if w.sinker != nil {
		if err := w.sinker.Close(); err != nil {
			return err
		}
		w.sinker = nil
	}

	if len(w.hold) != 0 {
		for _, bat := range w.hold {
			bat.Clean(mp)
		}
		w.hold = nil
		w.holdFlushUntilSyncCall = false
	}

	if w.blockInfoBat != nil {
		w.blockInfoBat.Clean(mp)
		w.blockInfoBat = nil
	}

	w.written = nil

	return nil
}

func ExpandObjectStatsToBatch(
	mp *mpool.MPool,
	isTombstone bool,
	outBath *batch.Batch,
	isCNCreated bool,
	statsList ...objectio.ObjectStats,
) (err error) {

	if !isTombstone {
		objectio.ForeachBlkInObjStatsList(
			true, nil,
			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				if err = vector.AppendBytes(
					outBath.Vecs[0],
					objectio.EncodeBlockInfo(&blk), false, mp); err != nil {
					return false
				}

				return true

			}, statsList...)

		for i := range statsList {
			if isCNCreated {
				objectio.WithCNCreated()(&statsList[i])
			}

			if err = vector.AppendBytes(outBath.Vecs[1],
				statsList[i].Marshal(), false, mp); err != nil {
				return err
			}
		}
	} else {
		for i := range statsList {
			if isCNCreated {
				objectio.WithCNCreated()(&statsList[i])
			}

			if err = vector.AppendBytes(outBath.Vecs[0],
				statsList[i].Marshal(), false, mp); err != nil {
				return err
			}
		}
	}

	outBath.SetRowCount(outBath.Vecs[0].Length())
	return nil
}

func (w *CNS3Writer) FillBlockInfoBat(
	mp *mpool.MPool,
) (*batch.Batch, error) {

	err := ExpandObjectStatsToBatch(mp, w.isTombstone, w.blockInfoBat, true, w.written...)

	return w.blockInfoBat, err
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

	if w.blockInfoBat != nil {
		w.blockInfoBat.CleanOnlyData()
	}

	w.blockInfoBat = AllocCNS3ResultBat(w.isTombstone, false)
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
		if len(w.hold) > 0 {
			for _, bat := range w.hold {
				bat.Clean(proc.Mp())
			}
			w.hold = nil
		}
	}()

	for _, bat := range w.hold {
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

	return nil
}

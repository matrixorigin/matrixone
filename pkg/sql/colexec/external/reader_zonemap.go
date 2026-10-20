// Copyright 2024 Matrix Origin
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

package external

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ZonemapReader handles MO internal result files (RESULT_SCAN).
type ZonemapReader struct {
	param       *ExternalParam
	zoneparam   *ZonemapFileparam
	blockReader *ioutil.BlockReader
}

// NewZonemapReader creates a ZonemapReader.
// zonemappable is computed here since only ZonemapReader uses it.
func NewZonemapReader(param *ExternalParam, proc *process.Process) *ZonemapReader {
	r := &ZonemapReader{
		zoneparam: &ZonemapFileparam{},
	}
	param.Filter.zonemappable = plan2.ExprIsZonemappable(
		proc.Ctx, param.Filter.FilterExpr)
	return r
}

func (r *ZonemapReader) Open(param *ExternalParam, proc *process.Process) (fileEmpty bool, err error) {
	r.param = param
	// Block metadata (zone maps, null counts) is indexed by physical file
	// column, but param.Filter.columnMap was resolved against the (possibly
	// pruned) output schema. Translate it through the physical file indexes
	// before block pruning (issue #30571 follow-up, filtered projections).
	translateZonemapFilterColumnMap(param)
	r.blockReader, err = ioutil.NewFileReader(
		param.Extern.FileService, param.Fileparam.Filepath)
	if err != nil {
		return false, err
	}
	r.zoneparam.offset = 0
	r.zoneparam.bs = nil
	return false, nil
}

// translateZonemapFilterColumnMap rewrites a filter columnMap that was
// resolved against the scan output schema so each referenced column maps to
// its physical file index. Zone maps and null counts in block metadata are
// indexed by physical file column, and column projection may prune the
// output schema without affecting the file layout.
func translateZonemapFilterColumnMap(param *ExternalParam) {
	if param.Filter == nil || len(param.Filter.columnMap) == 0 {
		return
	}
	for _, attr := range param.Attrs {
		key := int(attr.ColIndex)
		if _, ok := param.Filter.columnMap[key]; ok {
			param.Filter.columnMap[key] = int(attr.ColFieldIndex)
		}
	}
}

func (r *ZonemapReader) ReadBatch(
	ctx context.Context, buf *batch.Batch,
	proc *process.Process, analyzer process.Analyzer,
) (fileFinished bool, err error) {
	_, span := trace.Start(ctx, "ZonemapReader.ReadBatch")
	defer span.End()

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(ctx, crs)

	// Load all block metadata
	r.zoneparam.bs, err = process.MeasureFilesystemWait(analyzer, func() ([]objectio.BlockObject, error) {
		return r.blockReader.LoadAllBlocks(newCtx, proc.GetMPool())
	})
	if err != nil {
		return false, err
	}

	// ZoneMap filter: skip blocks that don't match
	if r.param.Filter.zonemappable {
		for r.zoneparam.offset < len(r.zoneparam.bs) && !r.needRead(ctx, proc) {
			r.zoneparam.offset++
		}
	}

	// All blocks filtered out
	if r.zoneparam.offset >= len(r.zoneparam.bs) {
		return true, nil
	}

	// Read the matching block
	if err = process.MeasureFilesystemWaitErr(analyzer, func() error {
		return r.getBatchFromZonemapFile(newCtx, proc, buf)
	}); err != nil {
		return false, err
	}

	r.zoneparam.offset++
	fileFinished = r.zoneparam.offset >= len(r.zoneparam.bs)
	return fileFinished, nil
}

func (r *ZonemapReader) Close() error {
	r.blockReader = nil
	r.zoneparam.bs = nil
	r.zoneparam.offset = 0
	r.param = nil
	return nil
}

func (r *ZonemapReader) needRead(ctx context.Context, proc *process.Process) bool {
	_, span := trace.Start(ctx, "ZonemapReader.needRead")
	defer span.End()

	expr := r.param.Filter.FilterExpr
	if expr == nil {
		return true
	}
	if r.zoneparam.offset >= len(r.zoneparam.bs) {
		return true
	}

	notReportErrCtx := errutil.ContextWithNoReport(ctx, true)

	meta := r.zoneparam.bs[r.zoneparam.offset]
	columnMap := r.param.Filter.columnMap
	var (
		zms  []objectio.ZoneMap
		vecs []*vector.Vector
	)

	if r.param.Filter.AuxIdCnt > 0 {
		zms = make([]objectio.ZoneMap, r.param.Filter.AuxIdCnt)
		vecs = make([]*vector.Vector, r.param.Filter.AuxIdCnt)
	}

	return colexec.EvaluateFilterByZoneMap(
		notReportErrCtx, proc, expr, meta, columnMap, zms, vecs)
}

// getBatchFromZonemapFile reads one block from the zonemap file into bat.
// Migrated from external.go getBatchFromZonemapFile.
func (r *ZonemapReader) getBatchFromZonemapFile(ctx context.Context, proc *process.Process, bat *batch.Batch) error {
	var tmpBat *batch.Batch
	var vecTmp *vector.Vector
	var release func()
	mp := proc.Mp()
	param := r.param

	ctx, span := trace.Start(ctx, "getBatchFromZonemapFile")
	defer func() {
		span.End()
		if tmpBat != nil {
			for i, v := range tmpBat.Vecs {
				if v == vecTmp {
					tmpBat.Vecs[i] = nil
				}
			}
			tmpBat.Clean(mp)
		}
		if vecTmp != nil {
			vecTmp.Free(mp)
		}
		if release != nil {
			release()
		}
	}()

	if r.zoneparam.offset >= len(r.zoneparam.bs) {
		return nil
	}

	rows := 0
	idxs := make([]uint16, len(param.Attrs))
	meta := r.zoneparam.bs[r.zoneparam.offset].GetMeta()
	colCnt := meta.BlockHeader().ColumnCount()
	for i := 0; i < len(param.Attrs); i++ {
		// Column positions inside the file live in ColFieldIndex: the query
		// may prune or projection-reorder the scan schema, remapping ColIndex
		// to the position in the output schema. Using ColIndex here reads
		// the wrong file column (issue #30571).
		fieldIdx := param.Attrs[i].ColFieldIndex
		if fieldIdx < 0 || fieldIdx >= int32(colCnt) {
			idxs[i] = 0 // column absent from the file; an all-null column is produced below
			continue
		}
		idxs[i] = uint16(fieldIdx)
	}

	var err error
	tmpBat, release, err = r.blockReader.LoadColumns(ctx, idxs, nil,
		r.zoneparam.bs[r.zoneparam.offset].BlockHeader().BlockID().Sequence(), mp)
	if err != nil {
		return err
	}
	filepathBytes := []byte(param.Fileparam.Filepath)

	var sels []int64
	for i := 0; i < len(param.Attrs); i++ {
		attr := &param.Attrs[i]
		if attr.ColFieldIndex < 0 || attr.ColFieldIndex >= int32(colCnt) {
			vecTmp, err = proc.AllocVectorOfRows(makeType(&param.Cols[attr.ColIndex].Typ, false), rows, nil)
			if err != nil {
				return err
			}
			for j := 0; j < rows; j++ {
				nulls.Add(vecTmp.GetNulls(), uint64(j))
			}
		} else if catalog.ContainExternalHidenCol(param.Attrs[i].ColName) {
			if rows == 0 {
				rows = tmpBat.Vecs[i].Length()
			}
			vecTmp, err = proc.AllocVectorOfRows(makeType(&param.Cols[i].Typ, false), rows, nil)
			if err != nil {
				return err
			}
			for j := 0; j < rows; j++ {
				if err = vector.SetBytesAt(vecTmp, j, filepathBytes, mp); err != nil {
					return err
				}
			}
		} else {
			vecTmp = tmpBat.Vecs[i]
			rows = vecTmp.Length()
		}
		if cap(sels) >= vecTmp.Length() {
			sels = sels[:vecTmp.Length()]
		} else {
			sels = make([]int64, vecTmp.Length())
			for j, k := int64(0), int64(len(sels)); j < k; j++ {
				sels[j] = j
			}
		}

		if err = bat.Vecs[i].Union(vecTmp, sels, proc.GetMPool()); err != nil {
			return err
		}
	}

	n := bat.Vecs[0].Length()
	bat.SetRowCount(n)
	return nil
}

//go:build gpu

// Copyright 2026 Matrix Origin
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

package table_function

import (
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cuvsfilter "github.com/matrixorigin/matrixone/pkg/cuvs/filter"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"go.uber.org/zap"
)

// cuvsRowBuilder is the part of a CAGRA or IVF-PQ build that takes rows.
type cuvsRowBuilder interface {
	AddRow(id int64, vecBytes []byte) error
	filterColumnBuilder
}

// cuvsIgnoredRows leaves out of a CAGRA or IVF-PQ build the rows the index cannot store, up to
// the index's limit, and keeps them in a spill file until end writes them to the ignored table.
//
// The build plans its sub-indexes from the count of non-NULL rows, and the builder starts a new
// sub-index every capacity rows, so a left-out row shortens the last sub-index, which cuVS cannot
// build below threshold rows. The first threshold rows of each sub-index are therefore held until
// all of them arrive; rows still held at the end go to the CDC tail.
type cuvsIgnoredRows struct {
	limit     int64
	toFloat16 bool
	spill     *vectorindex.IgnoredSpill
	// left counts the rows the index cannot store, and kept the rows it stores.
	left, kept int64
	// firstPk and firstErr are the key and the error of the first row the index cannot store.
	firstPk  string
	firstErr error

	threshold, capacity int64
	// inSub counts the rows of the builder's current sub-index; 0 means rows are held.
	inSub int64
	held  []cuvscdc.PendingRecord
}

// newCuvsIgnoredRows returns the tracker of a build whose index has an ignored table, or nil.
// toFloat16 reports that the index stores a vecf32 base as float16.
func newCuvsIgnoredRows(tblcfg vectorindex.IndexTableConfig, spillDir string, toFloat16 bool) *cuvsIgnoredRows {
	if tblcfg.IgnoredTable == "" {
		return nil
	}
	return &cuvsIgnoredRows{
		limit:     tblcfg.MaxBadRecords,
		toFloat16: toFloat16,
		spill:     vectorindex.NewIgnoredSpill(spillDir),
	}
}

// setPlan sets the rows per sub-index (0 is one sub-index) and the fewest rows cuVS builds.
func (g *cuvsIgnoredRows) setPlan(capacity, threshold int64) {
	g.capacity, g.threshold = capacity, threshold
}

// checkCuvsRow reports whether the build leaves out the row with key id and vector vec; one row
// more than the limit fails the build.
func checkCuvsRow[T float32 | types.Float16](g *cuvsIgnoredRows, id int64, vec []T) (bool, error) {
	err := cuvscdc.CheckStorable(vec, g.toFloat16)
	if err == nil {
		g.kept++
		return false, nil
	}
	code, ok := vectorindex.UnstorableCode(err)
	if !ok {
		return false, err
	}
	g.left++
	pk := strconv.FormatInt(id, 10)
	if g.firstErr == nil {
		g.firstPk, g.firstErr = pk, err
	}
	if g.spill.Len() >= g.limit {
		return false, moerr.NewIndexTooManyBadRecordsNoCtx(g.limit, pk, err.Error())
	}
	return true, g.spill.Add(pk, code)
}

// holding reports whether the next row is held: the builder has no row of a sub-index yet.
func (g *cuvsIgnoredRows) holding() bool {
	return g.inSub == 0
}

// hold keeps a row, copying vec; once threshold rows are held it gives them to the builder.
func (g *cuvsIgnoredRows) hold(b cuvsRowBuilder, cols []cuvsfilter.ColumnMeta, id int64, vec, include []byte) error {
	g.held = append(g.held, cuvscdc.PendingRecord{Pkid: id, Vec: append([]byte(nil), vec...), Include: include})
	if int64(len(g.held)) < g.threshold {
		return nil
	}
	for _, r := range g.held {
		if err := b.AddRow(r.Pkid, r.Vec); err != nil {
			return err
		}
		if err := appendFilterRowFromInclude(b, cols, r.Include); err != nil {
			return err
		}
	}
	g.held = g.held[:0]
	g.inSub = g.threshold
	g.nextSub()
	return nil
}

// added counts a row given to the builder directly.
func (g *cuvsIgnoredRows) added() {
	g.inSub++
	g.nextSub()
}

func (g *cuvsIgnoredRows) nextSub() {
	if g.capacity > 0 && g.inSub >= g.capacity {
		g.inSub = 0
	}
}

// tail returns the rows still held, which go to the CDC tail.
func (g *cuvsIgnoredRows) tail() []cuvscdc.PendingRecord {
	return g.held
}

// allBad fails a build that can store none of the rows it reads.
func (g *cuvsIgnoredRows) allBad() error {
	if g.left > 0 && g.kept == 0 {
		return moerr.NewIndexAllRowsBadNoCtx(g.left, g.firstPk, g.firstErr.Error())
	}
	return nil
}

// sqls passes to exec the statements that record the left-out rows with commit time ts.
func (g *cuvsIgnoredRows) sqls(tblcfg vectorindex.IndexTableConfig, ts int64, exec func(string) error) error {
	if g.left > 0 {
		logutil.Warn("cuvs create ignored rows",
			zap.String("db", tblcfg.DbName),
			zap.String("table", tblcfg.SrcTable),
			zap.Int64("ignored", g.left),
			zap.String("first", g.firstPk+": "+g.firstErr.Error()))
	}
	cols := vectorindex.IgnoredColumns{Pk: catalog.IndexIgnored_TblCol_Pk, Code: catalog.IndexIgnored_TblCol_Code, Ts: catalog.IndexIgnored_TblCol_Ts}
	return g.spill.Sqls(tblcfg.DbName, tblcfg.IgnoredTable, cols, ts, exec)
}

func (g *cuvsIgnoredRows) close() {
	if g != nil {
		g.spill.Close()
	}
}

// appendFilterRowFromInclude gives the builder one row of INCLUDE values encoded as
// encodeIncludeRowFromArgVecs encodes them.
func appendFilterRowFromInclude(b filterColumnBuilder, cols []cuvsfilter.ColumnMeta, include []byte) error {
	if len(cols) == 0 {
		return nil
	}
	if len(include) != includeBytesPerRowFromCols(cols) {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("include row has %d bytes, want %d", len(include), includeBytesPerRowFromCols(cols)))
	}
	maskStart := len(include) - (len(cols)+7)/8
	off := 0
	for i, meta := range cols {
		size := int(meta.TypeOid.ElemSize())
		var nullBitmap []uint32
		if include[maskStart+i/8]&(1<<(i%8)) != 0 {
			nullBitmap = nullBitmapOneRowIsNull
		}
		if err := b.AddFilterChunk(uint32(i), include[off:off+size], nullBitmap, 1); err != nil {
			return err
		}
		off += size
	}
	return nil
}

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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	cuvsfilter "github.com/matrixorigin/matrixone/pkg/cuvs/filter"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
)

// heldRowOverhead is the Go bytes of a held row beside its vector and INCLUDE bytes.
const heldRowOverhead = 64

// cuvsRowBuilder is the part of a CAGRA or IVF-PQ build that takes rows.
type cuvsRowBuilder interface {
	AddRow(id int64, vecBytes []byte) error
	filterColumnBuilder
}

// cuvsIgnoredRows leaves out of a CAGRA or IVF-PQ build the rows the index cannot store.
//
// The build plans its sub-indexes from the count of non-NULL rows, and the builder starts a new
// sub-index every capacity rows, so a left-out row shortens the last sub-index, which cuVS
// cannot build below holdRows rows (minSubIndexRows). The first holdRows rows of each sub-index
// are therefore held until all of them arrive; rows still held at the end go to the CDC tail.
type cuvsIgnoredRows struct {
	*ignoredBuild
	toFloat16 bool

	holdRows, capacity int64
	// inSub counts the rows of the builder's current sub-index; 0 means rows are held.
	inSub int64
	held  []cuvscdc.PendingRecord
}

// newCuvsIgnoredRows returns the tracker of a build whose index has an ignored table, or nil.
// toFloat16 reports that the index stores a vecf32 base as float16.
func newCuvsIgnoredRows(tblcfg vectorindex.IndexTableConfig, spillDir string, toFloat16 bool) *cuvsIgnoredRows {
	b := newIgnoredBuild(tblcfg, spillDir)
	if b == nil {
		return nil
	}
	return &cuvsIgnoredRows{ignoredBuild: b, toFloat16: toFloat16}
}

// setPlan sets the rows per sub-index (0 is one sub-index) and the fewest rows cuVS builds one of.
func (g *cuvsIgnoredRows) setPlan(capacity, holdRows int64) {
	g.capacity, g.holdRows = capacity, holdRows
}

// checkCuvsRow reports whether the build leaves out the row with key id and vector vec; one row
// more than the limit fails the build.
func checkCuvsRow[T float32 | types.Float16](g *cuvsIgnoredRows, id int64, vec []T) (bool, error) {
	err := cuvscdc.CheckStorable(vec, g.toFloat16)
	if err == nil {
		g.stored()
		return false, nil
	}
	return true, g.leaveOut(strconv.FormatInt(id, 10), err)
}

// route gives the builder the row nthRow of argVecs, with key id and vector vec, or holds it
// while the builder's sub-index has no row yet.
func (g *cuvsIgnoredRows) route(b cuvsRowBuilder, cols []cuvsfilter.ColumnMeta, argVecs []*vector.Vector, nthRow int, id int64, vec []byte) error {
	if g.inSub == 0 {
		var include []byte
		if len(cols) > 0 {
			var err error
			if include, err = encodeIncludeRowFromArgVecs(cols, argVecs, 3, nthRow); err != nil {
				return err
			}
		}
		return g.hold(b, cols, id, vec, include)
	}
	if err := b.AddRow(id, vec); err != nil {
		return err
	}
	if len(cols) > 0 {
		if err := appendFilterRow(b, cols, argVecs, 3, nthRow); err != nil {
			return err
		}
	}
	g.inSub++
	g.nextSub()
	return nil
}

// hold keeps a row, copying vec; once holdRows rows are held it gives them to the builder.
func (g *cuvsIgnoredRows) hold(b cuvsRowBuilder, cols []cuvsfilter.ColumnMeta, id int64, vec, include []byte) error {
	g.held = append(g.held, cuvscdc.PendingRecord{Pkid: id, Vec: append([]byte(nil), vec...), Include: include})
	if int64(len(g.held)) < g.holdRows {
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
	clear(g.held)
	g.held = g.held[:0]
	g.inSub = g.holdRows
	g.nextSub()
	return nil
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

// close removes the spill file; g may be nil.
func (g *cuvsIgnoredRows) close() {
	if g != nil {
		g.ignoredBuild.close()
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

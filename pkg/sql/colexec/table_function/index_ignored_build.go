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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"go.uber.org/zap"
)

// ignoredBuild leaves out of an index build the rows the index cannot store, up to the index's
// limit, and keeps them in a spill file until the build writes them to its ignored table.
type ignoredBuild struct {
	limit int64
	spill *vectorindex.IgnoredSpill
	// left counts the rows the index cannot store, and kept the rows it stores.
	left, kept int64
	// firstPk and firstErr are the key and the error of the first row the index cannot store.
	firstPk  string
	firstErr error
}

// newIgnoredBuild returns the tracker of a build whose index has an ignored table, or nil.
// spillDir is the directory of the spill file ("" is $TMPDIR).
func newIgnoredBuild(tblcfg vectorindex.IndexTableConfig, spillDir string) *ignoredBuild {
	if tblcfg.IgnoredTable == "" {
		return nil
	}
	return &ignoredBuild{limit: tblcfg.MaxBadRecords, spill: vectorindex.NewIgnoredSpill(spillDir)}
}

// stored counts a row the index stores.
func (g *ignoredBuild) stored() {
	g.kept++
}

// leaveOut records that the index cannot store the row with key pk, a SQL literal, because of
// err. An err with no unstorable code, or one row more than the limit, is returned.
func (g *ignoredBuild) leaveOut(pk string, err error) error {
	code, ok := vectorindex.UnstorableCode(err)
	if !ok {
		return err
	}
	g.left++
	if g.firstErr == nil {
		g.firstPk, g.firstErr = pk, err
	}
	if g.spill.Len() >= g.limit {
		return moerr.NewIndexTooManyBadRecordsNoCtx(g.limit, pk, err.Error())
	}
	return g.spill.Add(pk, code, vectorindex.IgnoredReason(err))
}

// allBad fails a build that can store none of the rows it reads.
func (g *ignoredBuild) allBad() error {
	if g.left > 0 && g.kept == 0 {
		return moerr.NewIndexAllRowsBadNoCtx(g.left, g.firstPk, g.firstErr.Error())
	}
	return nil
}

// sqls passes to exec the statements that record the left-out rows with commit time ts.
func (g *ignoredBuild) sqls(tblcfg vectorindex.IndexTableConfig, ts int64, exec func(string) error) error {
	if g.left > 0 {
		logutil.Warn("index build ignored rows",
			zap.String("db", tblcfg.DbName),
			zap.String("table", tblcfg.SrcTable),
			zap.Int64("ignored", g.left),
			zap.String("first", g.firstPk+": "+g.firstErr.Error()))
	}
	cols := vectorindex.IgnoredColumns{Pk: catalog.IndexIgnored_TblCol_Pk, Code: catalog.IndexIgnored_TblCol_Code, Reason: catalog.IndexIgnored_TblCol_Reason, Ts: catalog.IndexIgnored_TblCol_Ts}
	return g.spill.Sqls(tblcfg.DbName, tblcfg.IgnoredTable, cols, ts, exec)
}

// close removes the spill file; g may be nil.
func (g *ignoredBuild) close() {
	if g != nil {
		g.spill.Close()
	}
}

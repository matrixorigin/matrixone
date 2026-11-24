// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type indexPointAccess struct {
	index   int
	filters []int32
}

type indexPointInterval struct {
	lower, upper float64
}

func pointSampleInterval(matches, missing, candidates int) indexPointInterval {
	// Bound every completion of the original sample. Eviction or admission
	// can correlate with the data, so a cache hit is not a sampling decision.
	lower := float64(matches) / engine.PointSampleRows
	upper := float64(matches+missing) / engine.PointSampleRows
	width := math.Sqrt(math.Log(2*float64(candidates)/0.01) / (2 * engine.PointSampleRows))
	return indexPointInterval{max(0, lower-width), min(1, upper+width)}
}

// Observe compatible ordinary point candidates after native admission. Every
// residual must stay on the base scan, where it preserves the same predicate
// and ordering evaluation for every candidate.
func (builder *QueryBuilder) observePointIndexMatches(node *plan.Node, indexes []*IndexDef, group []indexPointAccess) ([engine.MaxPointSampleCandidates]indexPointInterval, bool) {
	var result [engine.MaxPointSampleCandidates]indexPointInterval
	if len(group) < 2 || len(group) > len(result) || node == nil || node.TableDef == nil || node.TableDef.Pkey == nil || node.Stats == nil || len(node.FilterList) == 0 ||
		len(node.FilterList) > engine.MaxPointSampleColumns || len(node.BindingTags) != 1 ||
		node.ObjRef == nil || node.ObjRef.Obj <= 0 || uint64(node.ObjRef.Obj) != node.TableDef.TblId ||
		node.ObjRef.PubInfo != nil || node.ObjRef.SubscriptionName != "" ||
		node.ScanSnapshot.GetTS() != nil || node.ObjRef.Snapshot.GetTS() != nil ||
		builder.isPrepareStatement || builder.qry == nil || builder.qry.StmtType != plan.Query_SELECT ||
		len(builder.qry.Params) != 0 || GetForceScanOnMultiCN() ||
		len(node.OrderBy) != 0 || node.IndexReaderParam != nil || node.TableDef.IsTemporary || node.TableDef.IsDynamic || node.TableDef.Partition != nil ||
		catalog.IsSystemTable(node.TableDef.TblId) ||
		node.Stats.TableCnt <= objectio.BlockMaxRows || node.Stats.TableCnt > 32*objectio.BlockMaxRows ||
		math.Trunc(node.Stats.TableCnt) != node.Stats.TableCnt {
		return result, false
	}
	provider, ok := builder.compCtx.(interface {
		SamplePointIndexMatches(db, table uint64, rows int, candidates [][]engine.IndexPointKey) (engine.IndexPointSample, bool)
	})
	if !ok {
		return result, false
	}
	var filters [engine.MaxPointSampleColumns]engine.IndexPointKey
	var columnPositions [engine.MaxPointSampleColumns]int32
	var columnNames [engine.MaxPointSampleColumns]string
	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 ||
			fn.Args[0] == nil || fn.Args[1] == nil || types.T(fn.Args[0].Typ.Id) != types.T_int32 ||
			types.T(fn.Args[1].Typ.Id) != types.T_int32 {
			return result, false
		}
		col, lit := fn.Args[0].GetCol(), fn.Args[1].GetLit()
		if col == nil || col.RelPos != node.BindingTags[0] || lit == nil || lit.Isnull ||
			col.ColPos < 0 || int(col.ColPos) >= len(node.TableDef.Cols) ||
			slices.Contains(columnPositions[:i], col.ColPos) {
			return result, false
		}
		coldef := node.TableDef.Cols[col.ColPos]
		if coldef == nil || coldef.Typ.Id != int32(types.T_int32) || coldef.Seqnum >= objectio.SEQNUM_UPPER {
			return result, false
		}
		value, ok := lit.Value.(*plan.Literal_I32Val)
		if !ok {
			return result, false
		}
		filters[i] = engine.IndexPointKey{Seqnum: uint16(coldef.Seqnum), Value: value.I32Val}
		columnPositions[i] = col.ColPos
		columnNames[i] = coldef.Name
	}
	var keyGroups [engine.MaxPointSampleCandidates][engine.MaxPointSampleColumns]engine.IndexPointKey
	var candidates [engine.MaxPointSampleCandidates][]engine.IndexPointKey
	for i, candidate := range group {
		idx := indexes[candidate.index]
		if idx.Unique || len(idx.Parts) < 2 || len(idx.Parts) > engine.MaxPointSampleColumns+1 ||
			len(candidate.filters) == 0 || len(candidate.filters) > len(node.FilterList) {
			return result, false
		}
		for j, pos := range candidate.filters {
			if pos < 0 || int(pos) >= len(node.FilterList) {
				return result, false
			}
			keyGroups[i][j] = filters[pos]
		}
		for j := range node.FilterList {
			if slices.Contains(candidate.filters, int32(j)) {
				continue
			}
			name := columnNames[j]
			pk := node.TableDef.Pkey
			if name == pk.PkeyColName || slices.Contains(pk.Names, name) {
				return result, false
			}
			// A residual on any stored index part can move below the
			// backfill. Leave those paths to the ordinary ranking.
			for _, part := range idx.Parts {
				if catalog.ResolveAlias(part) == name {
					return result, false
				}
			}
		}
		candidates[i] = keyGroups[i][:len(candidate.filters)]
	}
	sample, ok := provider.SamplePointIndexMatches(node.TableDef.DbId, node.TableDef.TblId, int(node.Stats.TableCnt), candidates[:len(group)])
	if !ok || sample.Observed <= 0 || sample.Observed > engine.PointSampleRows {
		return result, false
	}
	for i := range group {
		matches := sample.Matches[i]
		if matches < 0 || matches > sample.Observed {
			return result, false
		}
		result[i] = pointSampleInterval(matches, engine.PointSampleRows-sample.Observed, len(group))
	}
	return result, true
}

// Choose a strict separation from every other admitted candidate. Falling
// back on overlapping intervals preserves the native ranking and its ties.
func separatedPointSampleWinner(intervals []indexPointInterval) int {
	best := 0
	for i := 1; i < len(intervals); i++ {
		if intervals[i].upper < intervals[best].upper {
			best = i
		}
	}
	for i, interval := range intervals {
		if i != best && intervals[best].upper >= interval.lower {
			return -1
		}
	}
	return best
}

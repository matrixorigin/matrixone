// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// MemClusterKeyFilter is the in-memory replay filter for a table's hidden
// composite cluster-by column. It shares the encoded-key domain and spec
// shape with MemPKFilter, but its key is nonunique: an equality predicate
// must yield every row sharing the cluster value. It therefore never takes
// part in unique-key shortcuts such as MemPKFilter.Exact/RecordExactHit.
type MemClusterKeyFilter struct {
	op        int
	packed    [][]byte
	disjuncts []MemClusterKeyFilter
	isVec     bool
	isValid   bool
	TS        types.TS

	// ColName is the hidden compound column the filter was built from. It is
	// informational: pruning is decided by the encoded-key domain, which is
	// stable across clustered-part renames.
	ColName string
}

func (f *MemClusterKeyFilter) Valid() bool {
	return f.isValid
}

func (f *MemClusterKeyFilter) Op() int {
	return f.op
}

func (f *MemClusterKeyFilter) Keys() [][]byte {
	return f.packed
}

// Specs flattens the filter into atomic specs OR-ed by the replay iterators.
func (f *MemClusterKeyFilter) Specs() []MemPKFilterSpec {
	if !f.isValid {
		return nil
	}
	if len(f.disjuncts) == 0 {
		return []MemPKFilterSpec{{Op: f.op, Keys: f.packed}}
	}

	specs := make([]MemPKFilterSpec, 0, len(f.disjuncts))
	for idx := range f.disjuncts {
		specs = append(specs, f.disjuncts[idx].Specs()...)
	}
	return specs
}

func (f *MemClusterKeyFilter) setFullData(op int, isVec bool, val ...[]byte) {
	f.packed = append(f.packed[:0], val...)
	f.op = op
	f.isVec = isVec
	f.isValid = true
	f.disjuncts = nil
}

// NewMemClusterKeyFilter builds the cluster-key mem filter from the same scan
// expression consumed by NewMemPKFilter. It returns an invalid filter unless
// the table's sort key is a composite cluster-by column and the expression
// carries a serializable predicate on the hidden compound column. The
// optional pruning never changes results: any row it drops from the replay
// iterator would also be dropped by the residual scan predicate.
func NewMemClusterKeyFilter(
	tableDef *plan.TableDef,
	expr *plan.Expr,
	ts timestamp.Timestamp,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
) (filter MemClusterKeyFilter, err error) {

	filter.TS = types.TimestampToTS(ts)

	clusterName, ok := compositeClusterByColumnName(tableDef)
	if !ok || packerPool == nil {
		return
	}
	// The hidden compound column is a serialized VARCHAR. A missing or
	// differently-typed column means the table definition cannot describe the
	// replay index content; fail open.
	if !clusterKeyColumnIsVarChar(tableDef, clusterName) {
		return
	}
	filter.ColName = clusterName

	baseFilter, err := ConstructBaseClusterFilter(expr, tableDef, mp)
	if err != nil {
		return MemClusterKeyFilter{}, err
	}
	defer baseFilter.Cleanup()
	if !baseFilter.Valid {
		return
	}

	if len(baseFilter.Disjuncts) > 0 {
		disjuncts := make([]MemClusterKeyFilter, 0, len(baseFilter.Disjuncts))
		for idx := range baseFilter.Disjuncts {
			disjunct, disjunctErr := newMemClusterKeyFilterFromBase(
				clusterName, ts, baseFilter.Disjuncts[idx], packerPool,
			)
			if disjunctErr != nil {
				return MemClusterKeyFilter{}, disjunctErr
			}
			if !disjunct.Valid() {
				return
			}
			disjuncts = append(disjuncts, disjunct)
		}
		filter.disjuncts = disjuncts
		filter.isValid = len(filter.disjuncts) > 0
		filter.isVec = true
		return
	}

	return newMemClusterKeyFilterFromBase(clusterName, ts, baseFilter, packerPool)
}

func newMemClusterKeyFilterFromBase(
	clusterName string,
	ts timestamp.Timestamp,
	basePKFilter BasePKFilter,
	packerPool *fileservice.Pool[*types.Packer],
) (filter MemClusterKeyFilter, err error) {
	filter.TS = types.TimestampToTS(ts)
	filter.ColName = clusterName

	var packer *types.Packer
	put := packerPool.Get(&packer)
	defer put.Put()

	op, packed, isVec, ok := encodeMemSortKeySpecs(basePKFilter, packer)
	if !ok {
		return
	}
	filter.setFullData(op, isVec, packed...)
	return filter, nil
}

func clusterKeyColumnIsVarChar(tableDef *plan.TableDef, name string) bool {
	if tableDef == nil {
		return false
	}
	if pos, ok := tableDef.Name2ColIndex[name]; ok && pos >= 0 &&
		int(pos) < len(tableDef.Cols) && tableDef.Cols[pos] != nil {
		return types.T(tableDef.Cols[pos].Typ.Id) == types.T_varchar
	}
	for _, col := range tableDef.Cols {
		if col != nil && col.Name == name {
			return types.T(col.Typ.Id) == types.T_varchar
		}
	}
	return false
}

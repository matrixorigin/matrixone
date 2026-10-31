// Copyright 2021-2024 Matrix Origin
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
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

// MemPKFilterSpec describes one atomic primary-key predicate. Multiple specs
// are OR-ed by partition-state iterators.
type MemPKFilterSpec struct {
	Op   int
	Keys [][]byte
}

type MemPKFilter struct {
	op        int
	packed    [][]byte
	inSet     map[string]struct{}
	prefixes  [][]byte
	disjuncts []MemPKFilter
	isVec     bool
	isValid   bool
	empty     bool
	TS        types.TS

	exact struct {
		hit bool
	}

	FilterHint engine.FilterHint
	HasBF      bool
	BFSeqNum   int16

	// clusterKey is the optional composite cluster-by filter derived from the
	// same scan expression. It is set only by NewReader for tables whose sort
	// key is a composite cluster-by column. Every primary-key accessor --
	// Valid, Exact, RecordExactHit, FilterVector -- deliberately ignores it:
	// the cluster filter selects a separate nonunique replay iterator and
	// takes no part in primary-key semantics.
	clusterKey *MemClusterKeyFilter
}

// SetClusterKeyFilter attaches the cluster-key filter. It must be called
// before the filter is published to a data source.
func (f *MemPKFilter) SetClusterKeyFilter(cluster *MemClusterKeyFilter) {
	f.clusterKey = cluster
}

// ClusterKeyFilter returns the attached cluster-key filter, or nil.
func (f *MemPKFilter) ClusterKeyFilter() *MemClusterKeyFilter {
	return f.clusterKey
}

// ClusterKeyValid reports whether the attached cluster-key filter can prune
// the committed replay iteration.
func (f *MemPKFilter) ClusterKeyValid() bool {
	return f.clusterKey != nil && f.clusterKey.Valid()
}

// ClusterKeySpecs returns the cluster-key filter specs, or nil when absent.
func (f *MemPKFilter) ClusterKeySpecs() []MemPKFilterSpec {
	if f.clusterKey == nil {
		return nil
	}
	return f.clusterKey.Specs()
}

// ClusterKeyTS returns the snapshot timestamp of the attached cluster-key
// filter.
func (f *MemPKFilter) ClusterKeyTS() types.TS {
	if f.clusterKey == nil {
		return types.TS{}
	}
	return f.clusterKey.TS
}

func (f *MemPKFilter) ClusterKeyDescriptor() ClusterKeyDescriptor {
	if f.clusterKey == nil {
		return ClusterKeyDescriptor{}
	}
	return f.clusterKey.Descriptor
}

func (f *MemPKFilter) Valid() bool {
	return f.isValid
}

func (f *MemPKFilter) Op() int {
	return f.op
}

func (f *MemPKFilter) Keys() [][]byte {
	return f.packed
}

func (f *MemPKFilter) Specs() []MemPKFilterSpec {
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

func NewMemPKFilter(
	tableDef *plan.TableDef,
	ts timestamp.Timestamp,
	packerPool *fileservice.Pool[*types.Packer],
	basePKFilter BasePKFilter,
	filterHint engine.FilterHint,
) (filter MemPKFilter, err error) {

	filter.TS = types.TimestampToTS(ts)

	if tableDef == nil {
		return
	}
	// The membership filter selects a separate key column for IVF/fulltext and
	// remains useful even when the SQL predicate cannot produce a PK fast path.
	filter.setFilterHint(tableDef, filterHint)
	if basePKFilter.Valid && basePKFilter.Empty {
		filter.SetFullData(function.IN, true)
		filter.empty = true
		return filter, nil
	}
	if tablePrimaryKeyUsesUnicodeCollation(tableDef) {
		// The in-memory primary-key iterator also compares packed original bytes;
		// do not publish a raw probe for a UCA text key. The independent
		// membership hint above remains available for index/fulltext readers.
		return filter, nil
	}

	if !basePKFilter.Valid || tableDef.Pkey == nil || packerPool == nil {
		return
	}

	if len(basePKFilter.Disjuncts) > 0 {
		disjuncts := make([]MemPKFilter, 0, len(basePKFilter.Disjuncts))
		for idx := range basePKFilter.Disjuncts {
			disjunct, disjunctErr := NewMemPKFilter(
				tableDef,
				ts,
				packerPool,
				basePKFilter.Disjuncts[idx],
				engine.FilterHint{},
			)
			if disjunctErr != nil {
				return MemPKFilter{}, disjunctErr
			}
			if !disjunct.Valid() {
				// PK pruning fails open as a unit: do not publish a
				// partially constructed disjunction, and do not discard the
				// independently initialized membership filter.
				return
			}
			disjuncts = append(disjuncts, disjunct)
		}
		filter.disjuncts = disjuncts
		filter.isValid = len(filter.disjuncts) > 0
		filter.isVec = true
		return
	}
	if !validBlockPKSearchFilter(basePKFilter) {
		return
	}

	var packer *types.Packer
	put := packerPool.Get(&packer)
	defer put.Put()

	op, packed, isVec, ok := encodeMemSortKeySpecs(basePKFilter, packer)
	if !ok {
		return
	}
	filter.SetFullData(op, isVec, packed...)

	return
}

// encodeMemSortKeySpecs packs a base sort-key filter into the encoded-key
// domain shared by the in-memory replay indexes. ok is false when the filter
// shape is not representable; callers fail open to full row iteration.
func encodeMemSortKeySpecs(
	basePKFilter BasePKFilter,
	packer *types.Packer,
) (op int, packed [][]byte, isVec bool, ok bool) {
	var lbVal, ubVal any
	if basePKFilter.Op != function.IN && basePKFilter.Op != function.PREFIX_IN {
		switch basePKFilter.Oid {
		case types.T_bool:
			lbVal = types.DecodeBool(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeBool(basePKFilter.UB)
			}
		case types.T_bit:
			lbVal = types.DecodeUint64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint64(basePKFilter.UB)
			}
		case types.T_int8:
			lbVal = types.DecodeInt8(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeInt8(basePKFilter.UB)
			}
		case types.T_int16:
			lbVal = types.DecodeInt16(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeInt16(basePKFilter.UB)
			}
		case types.T_int32:
			lbVal = types.DecodeInt32(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeInt32(basePKFilter.UB)
			}
		case types.T_int64:
			lbVal = types.DecodeInt64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeInt64(basePKFilter.UB)
			}
		case types.T_float32:
			lbVal = types.DecodeFloat32(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeFloat32(basePKFilter.UB)
			}
		case types.T_float64:
			lbVal = types.DecodeFloat64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeFloat64(basePKFilter.UB)
			}
		case types.T_uint8:
			lbVal = types.DecodeUint8(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint8(basePKFilter.UB)
			}
		case types.T_uint16:
			lbVal = types.DecodeUint16(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint16(basePKFilter.UB)
			}
		case types.T_uint32:
			lbVal = types.DecodeUint32(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint32(basePKFilter.UB)
			}
		case types.T_uint64:
			lbVal = types.DecodeUint64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint64(basePKFilter.UB)
			}
		case types.T_date:
			lbVal = types.DecodeDate(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDate(basePKFilter.UB)
			}
		case types.T_time:
			lbVal = types.DecodeTime(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeTime(basePKFilter.UB)
			}
		case types.T_datetime:
			lbVal = types.DecodeDatetime(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDatetime(basePKFilter.UB)
			}
		case types.T_timestamp:
			lbVal = types.DecodeTimestamp(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeTimestamp(basePKFilter.UB)
			}
		case types.T_year:
			lbVal = types.DecodeMoYear(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeMoYear(basePKFilter.UB)
			}
		case types.T_decimal64:
			lbVal = types.DecodeDecimal64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDecimal64(basePKFilter.UB)
			}
		case types.T_decimal128:
			lbVal = types.DecodeDecimal128(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDecimal128(basePKFilter.UB)
			}
		case types.T_decimal256:
			lbVal = types.DecodeDecimal256(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDecimal256(basePKFilter.UB)
			}
		case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary:
			lbVal = basePKFilter.LB
			ubVal = basePKFilter.UB
		case types.T_json:
			lbVal = types.DecodeJson(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeJson(basePKFilter.UB)
			}
		case types.T_enum:
			lbVal = types.DecodeEnum(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeEnum(basePKFilter.UB)
			}
		case types.T_uuid:
			lbVal = types.DecodeUuid(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUuid(basePKFilter.UB)
			}
		default:
			logutil.Warn("encodeMemSortKeySpecs skipped data type",
				zap.Int("expr op", basePKFilter.Op),
				zap.String("data type", basePKFilter.Oid.String()))
			return 0, nil, false, false
		}
	}

	switch basePKFilter.Op {
	case function.EQUAL, function.PREFIX_EQ:
		packed = append(packed, EncodePrimaryKey(lbVal, packer))
		if basePKFilter.Op == function.PREFIX_EQ {
			// TODO Remove this later
			// serial_full(secondary_index, primary_key|fake_pk) => varchar
			// prefix_eq expression only has the prefix(secondary index) in it.
			// there will have an extra zero after the `encodeStringType` done
			// this will violate the rule of prefix_eq, so remove this redundant zero here.
			//
			packed[0] = packed[0][0 : len(packed[0])-1]
		}
		return basePKFilter.Op, packed, false, true

	case function.IN, function.PREFIX_IN:
		packed = EncodePrimaryKeyVector(basePKFilter.Vec, packer)

		if basePKFilter.Op == function.PREFIX_IN {
			for x := range packed {
				packed[x] = packed[x][0 : len(packed[x])-1]
			}
		}
		return basePKFilter.Op, packed, true, true

	case function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL:
		packed = append(packed, EncodePrimaryKey(lbVal, packer))
		return basePKFilter.Op, packed, false, true

	case function.PREFIX_BETWEEN, function.BETWEEN,
		RangeLeftOpen, RangeRightOpen, RangeBothOpen,
		PrefixRangeLeftOpen, PrefixRangeRightOpen, PrefixRangeBothOpen:
		packed = append(packed, EncodePrimaryKey(lbVal, packer))
		packed = append(packed, EncodePrimaryKey(ubVal, packer))
		if basePKFilter.Op == function.PREFIX_BETWEEN ||
			basePKFilter.Op == PrefixRangeLeftOpen ||
			basePKFilter.Op == PrefixRangeRightOpen ||
			basePKFilter.Op == PrefixRangeBothOpen {
			packed[0] = packed[0][0 : len(packed[0])-1]
			packed[1] = packed[1][0 : len(packed[1])-1]
		}
		return basePKFilter.Op, packed, false, true
	default:
		return 0, nil, false, false
	}
}

func (f *MemPKFilter) setFilterHint(tableDef *plan.TableDef, filterHint engine.FilterHint) {
	f.FilterHint = filterHint
	if f.FilterHint.BF != nil && f.FilterHint.BF.Valid() {
		f.HasBF = true
		f.BFSeqNum = -1
		// For IVF entries, origin_pk is the source key domain used to build this
		// filter; the hidden composite PK must remain available only for ordinary
		// PK pruning. Do not infer this from Pkey.Names: copied physical tables
		// can expose only their hidden composite key there.
		for _, col := range tableDef.Cols {
			if strings.EqualFold(col.Name, catalog.IndexTablePrimaryColName) {
				f.BFSeqNum = int16(col.Seqnum)
				break
			}
		}
		// For fulltext index table, use doc_id column for BF filtering.
		if f.BFSeqNum == -1 && catalog.IsFullTextIndexTableType(tableDef.TableType, tableDef.Name) {
			for _, col := range tableDef.Cols {
				if col.Name == catalog.FullTextIndex_TabCol_Id {
					f.BFSeqNum = int16(col.Seqnum)
					break
				}
			}
		}
	}
}

func (f *MemPKFilter) InKind() (int, bool) {
	return len(f.packed), f.op == function.IN || f.op == function.PREFIX_IN
}

func (f *MemPKFilter) Exact() (bool, bool) {
	return f.op == function.EQUAL && len(f.packed) == 1, f.exact.hit
}

func (f *MemPKFilter) RecordExactHit() bool {
	if ok, _ := f.Exact(); !ok {
		return false
	}

	f.exact.hit = true

	return true
}

func (f *MemPKFilter) Must() bool {
	return f.FilterHint.Must
}

func (f *MemPKFilter) String() string {
	var buf bytes.Buffer
	if len(f.disjuncts) > 0 {
		buf.WriteString(fmt.Sprintf("InMemPKFilter{disjuncts: %d, isValid: %v}", len(f.disjuncts), f.isValid))
		return buf.String()
	}
	buf.WriteString(fmt.Sprintf("InMemPKFilter{op: %d, isVec: %v, isValid: %v vals: [", f.op, f.isVec, f.isValid))
	for x := range f.packed {
		buf.WriteString(fmt.Sprintf("%x, ", f.packed[x]))
	}
	buf.WriteString(fmt.Sprintf("][%d]}", len(f.packed)))
	return buf.String()
}

func (f *MemPKFilter) SetNull() {
	f.isValid = false
}

func (f *MemPKFilter) SetFullData(op int, isVec bool, val ...[]byte) {
	f.packed = append(f.packed[:0], val...)
	f.op = op
	f.isVec = isVec
	f.isValid = true
	f.empty = false
	f.inSet = nil
	f.prefixes = nil
	f.disjuncts = nil

	if op == function.IN || op == function.PREFIX_IN {
		sort.Slice(f.packed, func(i, j int) bool {
			return bytes.Compare(f.packed[i], f.packed[j]) < 0
		})
	}
	if op == function.IN {
		f.inSet = make(map[string]struct{}, len(f.packed))
		for _, key := range f.packed {
			f.inSet[string(key)] = struct{}{}
		}
	}
	if op == function.PREFIX_IN {
		f.prefixes = make([][]byte, 0, len(f.packed))
		for _, prefix := range f.packed {
			if len(f.prefixes) > 0 && bytes.HasPrefix(prefix, f.prefixes[len(f.prefixes)-1]) {
				continue
			}
			f.prefixes = append(f.prefixes, prefix)
		}
	}
}

func (f *MemPKFilter) matches(key []byte) bool {
	if len(f.disjuncts) > 0 {
		for idx := range f.disjuncts {
			if f.disjuncts[idx].matches(key) {
				return true
			}
		}
		return false
	}

	switch f.op {
	case function.EQUAL:
		return len(f.packed) == 1 && bytes.Equal(key, f.packed[0])
	case function.PREFIX_EQ:
		return len(f.packed) == 1 && bytes.HasPrefix(key, f.packed[0])
	case function.IN:
		_, ok := f.inSet[string(key)]
		return ok
	case function.PREFIX_IN:
		idx := sort.Search(len(f.prefixes), func(idx int) bool {
			return bytes.Compare(f.prefixes[idx], key) > 0
		}) - 1
		return idx >= 0 && bytes.HasPrefix(key, f.prefixes[idx])
	case function.BETWEEN:
		return len(f.packed) == 2 && bytes.Compare(key, f.packed[0]) >= 0 && bytes.Compare(key, f.packed[1]) <= 0
	case RangeRightOpen:
		return len(f.packed) == 2 && bytes.Compare(key, f.packed[0]) >= 0 && bytes.Compare(key, f.packed[1]) < 0
	case RangeLeftOpen:
		return len(f.packed) == 2 && bytes.Compare(key, f.packed[0]) > 0 && bytes.Compare(key, f.packed[1]) <= 0
	case RangeBothOpen:
		return len(f.packed) == 2 && bytes.Compare(key, f.packed[0]) > 0 && bytes.Compare(key, f.packed[1]) < 0
	case function.PREFIX_BETWEEN:
		return len(f.packed) == 2 && types.PrefixCompare(key, f.packed[0]) >= 0 && types.PrefixCompare(key, f.packed[1]) <= 0
	case PrefixRangeLeftOpen:
		return len(f.packed) == 2 && types.PrefixCompare(key, f.packed[0]) > 0 && types.PrefixCompare(key, f.packed[1]) <= 0
	case PrefixRangeRightOpen:
		return len(f.packed) == 2 && types.PrefixCompare(key, f.packed[0]) >= 0 && types.PrefixCompare(key, f.packed[1]) < 0
	case PrefixRangeBothOpen:
		return len(f.packed) == 2 && types.PrefixCompare(key, f.packed[0]) > 0 && types.PrefixCompare(key, f.packed[1]) < 0
	case function.GREAT_EQUAL:
		return len(f.packed) == 1 && bytes.Compare(key, f.packed[0]) >= 0
	case function.GREAT_THAN:
		return len(f.packed) == 1 && bytes.Compare(key, f.packed[0]) > 0
	case function.LESS_EQUAL:
		return len(f.packed) == 1 && bytes.Compare(key, f.packed[0]) <= 0
	case function.LESS_THAN:
		return len(f.packed) == 1 && bytes.Compare(key, f.packed[0]) < 0
	default:
		return true
	}
}

func (f *MemPKFilter) FilterVector(
	vec *vector.Vector,
	packer *types.Packer,
	skipMask *objectio.Bitmap,
) {
	filterVectorRowsForTest.Add(uint64(vec.Length()))
	keys := EncodePrimaryKeyVector(vec, packer)
	for i := 0; i < len(keys); i++ {
		if !f.matches(keys[i]) {
			skipMask.Add(uint64(i))
		}
	}
}

// filterVectorRowsForTest counts rows that FilterVector encoded. Production
// code never reads it; size-scaling tests use it to assert that per-statement
// work stays bounded as a transaction's workspace grows.
var filterVectorRowsForTest atomic.Uint64

// encodePKRowsForTest counts every row that EncodePrimaryKeyVector encoded:
// legacy FilterVector passes, write-time envelope computation, and exact-key
// index construction all share that encoder, so the counter observes the whole
// per-statement key-encoding work, not just one path.
var encodePKRowsForTest atomic.Uint64

// indexProbesForTest counts reads that answered from a per-batch exact-key
// index; indexBuildsForTest counts index (re)constructions. Deltas separate
// warm probe reuse (probes grow, builds do not) from cold rebuilding after a
// batching mutation. Production code never reads them.
var indexProbesForTest atomic.Uint64
var indexBuildsForTest atomic.Uint64

// FilterVectorRowsForTest returns the number of rows encoded by FilterVector
// so far.
func FilterVectorRowsForTest() uint64 {
	return filterVectorRowsForTest.Load()
}

// ResetFilterVectorRowsForTest zeroes the FilterVector row counter.
func ResetFilterVectorRowsForTest() {
	filterVectorRowsForTest.Store(0)
}

// EncodePKRowsForTest returns the number of rows encoded by
// EncodePrimaryKeyVector so far, regardless of caller.
func EncodePKRowsForTest() uint64 {
	return encodePKRowsForTest.Load()
}

// ResetEncodePKRowsForTest zeroes the shared key-encoding row counter.
func ResetEncodePKRowsForTest() {
	encodePKRowsForTest.Store(0)
}

// IndexProbesForTest returns how many reads have been answered through a
// per-batch exact-key index.
func IndexProbesForTest() uint64 {
	return indexProbesForTest.Load()
}

// ResetIndexProbesForTest zeroes the index-probe counter.
func ResetIndexProbesForTest() {
	indexProbesForTest.Store(0)
}

// RecordIndexProbeForTest records one index-answered read. Called by the
// workspace reader when it answers a batch from the exact-key index.
func RecordIndexProbeForTest() {
	indexProbesForTest.Add(1)
}

// IndexBuildsForTest returns how many exact-key indexes have been built.
func IndexBuildsForTest() uint64 {
	return indexBuildsForTest.Load()
}

// ResetIndexBuildsForTest zeroes the index-build counter.
func ResetIndexBuildsForTest() {
	indexBuildsForTest.Store(0)
}

// RecordIndexBuildForTest records one exact-key index construction.
func RecordIndexBuildForTest() {
	indexBuildsForTest.Add(1)
}

// HasMemberFilter reports whether an independent membership filter is
// attached to this scan and bound to a membership column. While attached,
// keeping a row needs more than a primary-key match: the membership
// predicate can bind a different column (e.g. __mo_index_pri_col) and its
// set can be exact, so callers must keep the FilterVector plus membership
// path instead of answering from a primary-key index alone.
func (f *MemPKFilter) HasMemberFilter() bool {
	return f.HasBF && f.BFSeqNum != -1
}

// ExactPointKey returns the single encoded key when this filter is a valid
// exact single-key point probe (EQUAL with no disjuncts). Callers use it to
// answer batches through per-batch key indexes instead of filtering every row.
func (f *MemPKFilter) ExactPointKey() ([]byte, bool) {
	if !f.isValid || len(f.disjuncts) > 0 {
		return nil, false
	}
	if f.op != function.EQUAL || len(f.packed) != 1 {
		return nil, false
	}
	return f.packed[0], true
}

// MayMatchKeyRange reports whether any canonical-encoded primary key in the
// byte range [lo, hi] could satisfy this filter. Callers pass a batch's
// encoded-key envelope: a false return proves that every key of the batch
// falls outside the filter's matching set, so the batch can be skipped
// without calling FilterVector. The predicate must fail open: for any
// operator whose matching set cannot be proven disjoint from [lo, hi], it
// returns true and the batch takes the unpruned path.
func (f *MemPKFilter) MayMatchKeyRange(lo, hi []byte) bool {
	if !f.isValid || len(lo) == 0 || len(hi) == 0 || bytes.Compare(lo, hi) > 0 {
		return true
	}

	if len(f.disjuncts) > 0 {
		for idx := range f.disjuncts {
			if f.disjuncts[idx].MayMatchKeyRange(lo, hi) {
				return true
			}
		}
		return false
	}

	switch f.op {
	case function.EQUAL:
		// The single key must lie inside the envelope.
		return len(f.packed) == 1 &&
			bytes.Compare(f.packed[0], lo) >= 0 &&
			bytes.Compare(f.packed[0], hi) <= 0

	case function.IN:
		// f.packed is sorted ascending (SetFullData); any set key inside [lo, hi].
		if len(f.packed) == 0 {
			return !f.empty
		}
		idx := sort.Search(len(f.packed), func(i int) bool {
			return bytes.Compare(f.packed[i], lo) >= 0
		})
		return idx < len(f.packed) && bytes.Compare(f.packed[idx], hi) <= 0

	case function.BETWEEN, RangeLeftOpen, RangeRightOpen, RangeBothOpen:
		// A closed envelope [lo, hi] intersects [lb, ub] unless they are
		// fully apart; openness only removes boundary keys, so the closed
		// test stays conservative.
		if len(f.packed) != 2 {
			return true
		}
		return bytes.Compare(hi, f.packed[0]) >= 0 &&
			bytes.Compare(lo, f.packed[1]) <= 0

	case function.GREAT_EQUAL:
		return len(f.packed) == 1 && bytes.Compare(hi, f.packed[0]) >= 0

	case function.GREAT_THAN:
		return len(f.packed) == 1 && bytes.Compare(hi, f.packed[0]) > 0

	case function.LESS_EQUAL:
		return len(f.packed) == 1 && bytes.Compare(lo, f.packed[0]) <= 0

	case function.LESS_THAN:
		return len(f.packed) == 1 && bytes.Compare(lo, f.packed[0]) < 0

	default:
		// PREFIX_* operators compare with column-sensitive prefix rules and
		// any unmodeled operator must keep the unpruned path.
		return true
	}
}

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
	"math"
	"math/bits"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	RangeLeftOpen = iota + math.MaxInt16
	RangeRightOpen
	RangeBothOpen
	PrefixRangeLeftOpen
	PrefixRangeRightOpen
	PrefixRangeBothOpen
)

type BasePKFilter struct {
	Valid bool
	Op    int
	LB    []byte
	UB    []byte
	Vec   *vector.Vector
	Oid   types.T
	// Disjuncts holds OR-ed atomic filters; when non-empty, Op/LB/UB/Vec are ignored.
	Disjuncts []BasePKFilter
	cleanup   *basePKFilterCleanup
	resource  *basePKFilterResource
}

type basePKFilterCleanup struct {
	once sync.Once
	fns  []func()
}

type basePKFilterResource struct {
	once sync.Once
	free func()
}

func (r *basePKFilterResource) release() {
	if r != nil {
		r.once.Do(r.free)
	}
}

func (c *basePKFilterCleanup) add(fn func()) {
	if c != nil && fn != nil {
		c.fns = append(c.fns, fn)
	}
}

func (c *basePKFilterCleanup) run() {
	if c == nil {
		return
	}
	c.once.Do(func() {
		for i := len(c.fns) - 1; i >= 0; i-- {
			c.fns[i]()
		}
		c.fns = nil
	})
}

// Cleanup releases temporary vectors created while merging filter operands.
// ConstructBlockPKFilter consumes this ownership and invokes it through the
// returned BlockReadFilter.Cleanup callback.
func (b *BasePKFilter) Cleanup() {
	if b == nil || b.cleanup == nil {
		return
	}
	b.cleanup.run()
	b.cleanup = nil
}

func (b *BasePKFilter) releaseMergedVector() {
	if b == nil || b.resource == nil {
		return
	}
	b.resource.release()
	b.resource = nil
	b.Vec = nil
}

func (b *BasePKFilter) String() string {
	name := map[int]string{
		function.LESS_EQUAL:     "less_eq",
		function.LESS_THAN:      "less_than",
		function.GREAT_THAN:     "great_than",
		function.GREAT_EQUAL:    "great_eq",
		RangeLeftOpen:           "range_left_open",
		RangeRightOpen:          "range_right_open",
		RangeBothOpen:           "range_both_open",
		PrefixRangeLeftOpen:     "prefix_range_left_open",
		PrefixRangeRightOpen:    "prefix_range_right_open",
		PrefixRangeBothOpen:     "prefix_range_both_open",
		function.EQUAL:          "equal",
		function.IN:             "in",
		function.BETWEEN:        "between",
		function.PREFIX_EQ:      "prefix_eq",
		function.PREFIX_IN:      "prefix_in",
		function.PREFIX_BETWEEN: "prefix_between",
	}

	vecStr := "nil"
	if b.Vec != nil {
		//vecStr = common.MoVectorToString(b.Vec, b.Vec.Length())
		vecStr = b.Vec.String()
	}

	var lb, ub any
	if !b.Oid.IsFixedLen() {
		lb = string(b.LB)
		ub = string(b.UB)
	} else {
		lb = types.DecodeValue(b.LB, b.Oid)
		ub = types.DecodeValue(b.UB, b.Oid)
	}

	return fmt.Sprintf("valid = %v, op = %s, lb = %v, ub = %v, vec = %v, oid = %s",
		b.Valid, name[b.Op],
		lb, ub, vecStr, b.Oid.String())
}

func ConstructBasePKFilter(
	expr *plan.Expr,
	tblDef *plan.TableDef,
	mp *mpool.MPool,
) (filter BasePKFilter, err error) {
	cleanup := &basePKFilterCleanup{}
	filter, err = constructBasePKFilter(expr, tblDef, mp, cleanup)
	if err != nil || !filter.Valid {
		cleanup.run()
		filter.cleanup = nil
		return filter, err
	}
	if len(cleanup.fns) == 0 {
		filter.cleanup = nil
	} else {
		filter.cleanup = cleanup
	}
	return filter, nil
}

func constructBasePKFilter(
	expr *plan.Expr,
	tblDef *plan.TableDef,
	mp *mpool.MPool,
	cleanup *basePKFilterCleanup,
) (filter BasePKFilter, err error) {
	if expr == nil {
		return
	}

	defer func() {
		if tblDef.Pkey.CompPkeyCol != nil {
			filter.Oid = types.T_varchar
		}
		if filter.Valid {
			filter.cleanup = cleanup
		}
	}()

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch name := exprImpl.F.Func.ObjName; name {
		case "and":
			var filters []BasePKFilter
			for idx := range exprImpl.F.Args {
				ff, err := constructBasePKFilter(exprImpl.F.Args[idx], tblDef, mp, cleanup)
				if err != nil {
					return BasePKFilter{}, err
				}
				if ff.Valid {
					filters = append(filters, ff)
				}
			}

			if len(filters) == 0 {
				return BasePKFilter{}, nil
			}

			for idx := 0; idx < len(filters)-1; {
				f1 := &filters[idx]
				f2 := &filters[idx+1]
				f1Vec, f2Vec := f1.Vec, f2.Vec
				ff, err := mergeFilters(f1, f2, function.AND, mp)
				if err != nil {
					return BasePKFilter{}, err
				}

				if !ff.Valid {
					return BasePKFilter{}, nil
				}
				if ff.Vec != f1Vec {
					f1.releaseMergedVector()
				}
				if ff.Vec != f2Vec {
					f2.releaseMergedVector()
				}

				idx++
				filters[idx] = ff
			}

			ret := filters[len(filters)-1]
			return ret, nil

		case "or":
			var (
				filters1 []BasePKFilter
				filters2 []BasePKFilter

				cannotMerge    bool
				hasUnsupported bool
			)

			for idx := range exprImpl.F.Args {
				ff, err := constructBasePKFilter(exprImpl.F.Args[idx], tblDef, mp, cleanup)
				if err != nil {
					return BasePKFilter{}, err
				}
				if !ff.Valid {
					hasUnsupported = true
					continue
				}

				filters1 = append(filters1, toDisjuncts(ff)...)
				filters2 = append(filters2, ff)
			}

			if hasUnsupported {
				return BasePKFilter{}, nil
			}

			if len(filters1) == 0 {
				return BasePKFilter{}, nil
			}

			if len(filters1) == 1 {
				return filters1[0], nil
			}

			for idx := 0; idx < len(filters2)-1; {
				f1 := &filters2[idx]
				f2 := &filters2[idx+1]
				ff, err := mergeFilters(f1, f2, function.OR, mp)
				if err != nil {
					return BasePKFilter{}, nil
				}

				if !ff.Valid {
					//return BasePKFilter{}, nil
					cannotMerge = true
					break
				}
				idx++
				filters2[idx] = ff
			}

			if !cannotMerge {
				ret := filters2[len(filters2)-1]
				releaseUnreferencedMergedVectors(filters1, ret.resource)
				releaseUnreferencedMergedVectors(filters2, ret.resource)
				return ret, nil
			}

			kept := make(map[*basePKFilterResource]struct{}, len(filters1))
			for idx := range filters1 {
				if filters1[idx].resource != nil {
					kept[filters1[idx].resource] = struct{}{}
				}
			}
			for idx := range filters2 {
				if _, ok := kept[filters2[idx].resource]; !ok {
					filters2[idx].releaseMergedVector()
				}
			}
			filter.Valid = true
			filter.Disjuncts = filters1
			return filter, nil

		case ">=":
			//a >= ?
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.GREAT_EQUAL
			filter.LB = vals[0]
			filter.Oid = oid

		case "<=":
			//a <= ?
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.LESS_EQUAL
			filter.LB = vals[0]
			filter.Oid = oid

		case ">":
			//a > ?
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.GREAT_THAN
			filter.LB = vals[0]
			filter.Oid = oid

		case "<":
			//a < ?
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.LESS_THAN
			filter.LB = vals[0]
			filter.Oid = oid

		case "=":
			// a = ?
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.EQUAL
			filter.LB = vals[0]
			filter.Oid = oid

		case "prefix_eq":
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.PREFIX_EQ
			filter.LB = vals[0]
			filter.Oid = oid

		case "in":
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, true, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.IN
			{
				vec, unmarshalErr := unmarshalPKInVector(vals[0])
				if unmarshalErr != nil {
					// The read filter is an optional pruning optimization.  Keep
					// malformed or version-skewed vector data on the residual
					// expression path instead of failing the query.
					return BasePKFilter{}, nil
				}
				filter.Vec = vec
			}
			filter.Oid = oid

		case "prefix_in":
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, true, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.PREFIX_IN
			{
				vec, unmarshalErr := unmarshalPKInVector(vals[0])
				if unmarshalErr != nil {
					return BasePKFilter{}, nil
				}
				filter.Vec = vec
			}
			filter.Oid = oid

		case "in_range":
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok || len(vals) < 3 || len(vals[2]) == 0 {
				return
			}
			switch vals[2][0] {
			case 0:
				filter.Op = function.BETWEEN
			case 1:
				filter.Op = RangeLeftOpen
			case 2:
				filter.Op = RangeRightOpen
			case 3:
				filter.Op = RangeBothOpen
			default:
				return
			}
			filter.Valid = true
			filter.LB = vals[0]
			filter.UB = vals[1]
			filter.Oid = oid

		case "between":
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.BETWEEN
			filter.LB = vals[0]
			filter.UB = vals[1]
			filter.Oid = oid

		case "prefix_between":
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok {
				return
			}
			filter.Valid = true
			filter.Op = function.PREFIX_BETWEEN
			filter.LB = vals[0]
			filter.UB = vals[1]
			filter.Oid = oid

		case "prefix_in_range":
			ok, oid, vals := evalValue(expr, exprImpl, tblDef, false, tblDef.Pkey.PkeyColName)
			if !ok || len(vals) < 3 || len(vals[2]) == 0 {
				return
			}
			switch vals[2][0] {
			case 0:
				filter.Op = function.PREFIX_BETWEEN
			case 1:
				filter.Op = PrefixRangeLeftOpen
			case 2:
				filter.Op = PrefixRangeRightOpen
			case 3:
				filter.Op = PrefixRangeBothOpen
			default:
				return
			}
			filter.Valid = true
			filter.LB = vals[0]
			filter.UB = vals[1]
			filter.Oid = oid

		default:
			//panic(name)
		}
	default:
		//panic(plan2.FormatExpr(expr))
	}

	return
}

// unmarshalPKInVector establishes the ordering invariant required by block
// read filters, zonemap IN evaluation and ordered IN set merging.  SQL NULL can
// never make an IN predicate TRUE for a non-null primary key, so dropping NULL
// entries is both conservative and avoids preserving an otherwise unsorted
// nullable constant vector.  UnmarshalBinary is zero-copy; only mutate an
// a private copy so the shared serialized plan expression remains immutable.
func unmarshalPKInVector(data []byte) (ret *vector.Vector, err error) {
	if err := validatePKInVectorEncoding(data); err != nil {
		return nil, err
	}
	vec := vector.NewVec(types.T_any.ToType())
	if err := vec.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	if err := validatePKInVectorShape(vec); err != nil {
		return nil, err
	}
	if !vec.IsConst() && vec.GetSorted() && !vec.GetNulls().Any() {
		return vec, nil
	}
	owned := vector.NewVec(types.T_any.ToType())
	// Keep the mutable clone on the Go heap: search closures retain it after this
	// function returns, and GC can reclaim it without adding another mpool owner.
	if err := owned.UnmarshalBinary(bytes.Clone(data)); err != nil {
		return nil, err
	}
	normalizePKInVector(owned)
	return owned, nil
}

func validatePKInVectorShape(vec *vector.Vector) (err error) {
	if vec.Length() < 0 {
		return moerr.NewInvalidInputNoCtxf("invalid PK IN vector length %d", vec.Length())
	}
	physicalLength := vec.Length()
	if vec.IsConst() && physicalLength > 0 {
		physicalLength = 1
	}
	if vec.IsConst() && physicalLength > 0 && vec.GetNulls().Any() && !vec.IsNull(0) {
		return moerr.NewInvalidInputNoCtx("invalid PK IN constant null bitmap")
	}
	if !vec.IsConstNull() {
		typeSize := vec.GetType().TypeSize()
		if typeSize <= 0 || physicalLength > len(vec.GetData())/typeSize {
			return moerr.NewInvalidInputNoCtx("invalid PK IN vector data length")
		}
	}
	if vec.GetType().IsVarlen() {
		values := vector.MustFixedColNoTypeCheck[types.Varlena](vec)
		areaLen := uint64(len(vec.GetArea()))
		for row := range values {
			if values[row].IsSmall() {
				continue
			}
			offset, length := values[row].OffsetLen()
			if uint64(offset)+uint64(length) > areaLen {
				return moerr.NewInvalidInputNoCtx("invalid PK IN vector varlen area")
			}
		}
	}
	return nil
}

func validatePKInVectorEncoding(data []byte) error {
	const scalarBytes = 4
	minimum := 1 + types.TSize + scalarBytes*4 + 1
	if len(data) < minimum {
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector encoding")
	}

	class := int(data[0])
	if class != vector.FLAT && class != vector.CONSTANT {
		return moerr.NewInvalidInputNoCtxf("invalid PK IN vector class %d", class)
	}
	pos := 1
	typ := types.DecodeType(data[pos : pos+types.TSize])
	pos += types.TSize
	if !supportedPKInType(typ.Oid) || typ.TypeSize() != typ.Oid.TypeLen() {
		return moerr.NewInvalidInputNoCtxf("invalid PK IN vector type %s", typ.Oid.String())
	}

	logicalLength := uint64(types.DecodeUint32(data[pos : pos+scalarBytes]))
	pos += scalarBytes
	if logicalLength > uint64(math.MaxInt) {
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector length")
	}

	readBytes := func() ([]byte, bool) {
		if len(data)-pos < scalarBytes {
			return nil, false
		}
		length := uint64(types.DecodeUint32(data[pos : pos+scalarBytes]))
		pos += scalarBytes
		if length > uint64(len(data)-pos) {
			return nil, false
		}
		value := data[pos : pos+int(length)]
		pos += int(length)
		return value, true
	}

	if _, ok := readBytes(); !ok { // vector data
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector data encoding")
	}
	if _, ok := readBytes(); !ok { // varlen area
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector area encoding")
	}
	nsp, ok := readBytes()
	if !ok {
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector null encoding")
	}
	if err := validatePKInNullEncoding(nsp, logicalLength); err != nil {
		return err
	}
	if len(data)-pos != 1 || data[pos] > 1 {
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector sorted flag")
	}
	return nil
}

func validatePKInNullEncoding(data []byte, logicalLength uint64) error {
	if len(data) == 0 {
		return nil
	}
	const headerBytes = 24
	if len(data) < headerBytes {
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector null bitmap")
	}
	count := types.DecodeInt64(data[:8])
	bitmapLength := types.DecodeUint64(data[8:16])
	dataBytes := types.DecodeUint64(data[16:24])
	if count < 0 || uint64(count) > logicalLength || dataBytes%8 != 0 ||
		dataBytes != uint64(len(data)-headerBytes) || bitmapLength > dataBytes*8 {
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector null bitmap")
	}

	actualCount := 0
	for pos := headerBytes; pos < len(data); pos += 8 {
		word := types.DecodeUint64(data[pos : pos+8])
		actualCount += bits.OnesCount64(word)
		if logicalLength == 0 && word != 0 {
			return moerr.NewInvalidInputNoCtx("invalid PK IN vector null bitmap")
		}
		if logicalLength > 0 && uint64(pos-headerBytes)/8 == (logicalLength-1)/64 {
			validBits := logicalLength % 64
			if validBits != 0 && word>>validBits != 0 {
				return moerr.NewInvalidInputNoCtx("invalid PK IN vector null bitmap")
			}
		}
		if logicalLength > 0 && uint64(pos-headerBytes)/8 > (logicalLength-1)/64 && word != 0 {
			return moerr.NewInvalidInputNoCtx("invalid PK IN vector null bitmap")
		}
	}
	if int64(actualCount) != count {
		return moerr.NewInvalidInputNoCtx("invalid PK IN vector null bitmap count")
	}
	return nil
}

func normalizePKInVector(vec *vector.Vector) {
	// A constant vector represents a repeated value, while IN is set-valued.
	// Collapse its logical length before encoding it for workspace filtering;
	// otherwise a corrupt or adversarial logical length can force an O(n)
	// expansion (or allocation) for a single physical value.
	if vec.IsConst() {
		if vec.IsConstNull() {
			vec.SetLength(0)
			vec.GetNulls().Reset()
		} else if vec.Length() > 0 {
			vec.SetLength(1)
		}
		vec.SetSorted(true)
		return
	}
	if vec.GetNulls().Any() {
		sels := make([]int64, 0, vec.Length()-vec.GetNulls().Count())
		for row := 0; row < vec.Length(); row++ {
			if !vec.IsNull(uint64(row)) {
				sels = append(sels, int64(row))
			}
		}
		vec.Shrink(sels, false)
	}
	vec.InplaceSortAndCompact()
}

func releaseUnreferencedMergedVectors(filters []BasePKFilter, keep *basePKFilterResource) {
	for idx := range filters {
		if filters[idx].resource != keep {
			filters[idx].releaseMergedVector()
		}
	}
}

func toDisjuncts(f BasePKFilter) []BasePKFilter {
	if len(f.Disjuncts) > 0 {
		return f.Disjuncts
	}
	return []BasePKFilter{f}
}

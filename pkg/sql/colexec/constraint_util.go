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
	"fmt"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// FilterRowIdForDel appends to retBat the row id and primary key of every row
// of srcBat with a non-NULL row id not seen before in this batch. keep is the
// caller's bitmap, reused across batches and freed by the caller.
func FilterRowIdForDel(proc *process.Process, retBat *batch.Batch, srcBat *batch.Batch,
	idx int, primaryKeyIdx int, keep *bitmap.Bitmap) error {
	if err := keep.InitWithSize(int64(srcBat.RowCount()), proc.Mp()); err != nil {
		return err
	}
	rowidVec := retBat.Vecs[0]
	primaryVec := retBat.Vecs[1]
	rowIdMap := make(map[types.Rowid]bool)
	nulls := srcBat.Vecs[idx].GetNulls()
	for i, r := range vector.MustFixedColWithTypeCheck[types.Rowid](srcBat.Vecs[idx]) {
		if !nulls.Contains(uint64(i)) {
			if rowIdMap[r] {
				continue
			}
			rowIdMap[r] = true
			keep.Add(uint64(i))
		}
	}
	err := rowidVec.UnionByMask(srcBat.Vecs[idx], keep, proc.Mp())
	if err != nil {
		return err
	}
	err = primaryVec.UnionByMask(srcBat.Vecs[primaryKeyIdx], keep, proc.Mp())
	if err != nil {
		return err
	}
	retBat.SetRowCount(keep.Count())
	return nil
}

func BatchDataNotNullCheck(vecs []*vector.Vector, attrs []string, tableDef *plan.TableDef, ctx context.Context) error {
	for j := range vecs {
		if vecs[j] == nil {
			continue
		}
		nsp := vecs[j].GetNulls()
		if tableDef.Cols[j].Default != nil && !tableDef.Cols[j].Default.NullAbility && nsp.Any() {
			return moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", attrs[j]))
		}
	}
	return nil
}

// ColForbidsNull reports whether a table column enforces NOT NULL. Both plan
// signals are honored: the explicit flag used by minimal external-insert
// plans and the default-value descriptor the PreInsert check relies on.
func ColForbidsNull(col *plan.ColDef) bool {
	return col != nil && (col.NotNull || (col.Default != nil && !col.Default.NullAbility))
}

// IgnoreFill adapts batches to the IGNORE statement rule for NOT NULL
// columns: each NULL cell is replaced with the implicit default of its column
// type and reported as one ER_BAD_NULL_ERROR warning, matching the value
// adjustment MySQL applies for INSERT/UPDATE/LOAD with IGNORE. The row itself
// is kept. A NOT NULL column whose type has no implicit default keeps the
// constraint-violation error: storing NULL would trade the report for
// corruption.
//
// IgnoreFill never modifies a vector its caller does not own. A column that
// needs an adjustment is copied into a vector owned by the IgnoreFill - one
// per column slot, created once and refilled for every batch - and the copy
// is adjusted and installed into the caller's slot. Its vectors stay valid
// until the next Fill, Free or the pool changes, which is the lifetime of the
// caller's output batch. A vector the caller owns (owned reports so) is
// adjusted in place through the caller's pool.
type IgnoreFill struct {
	mp       *mpool.MPool
	vecs     []*vector.Vector
	dropVecs []*vector.Vector
	view     *batch.Batch
	// keep holds the rows DropNullPkRows keeps, allocated from mp and reused
	// for every batch.
	keep bitmap.Bitmap
}

// View returns the IgnoreFill's reusable batch referencing bat's vectors, for
// a caller that must replace columns of a batch it does not own: Fill and
// DropNullPkRows then replace slots of the view, never of bat. It is valid
// until the next View or Free, and while bat is.
func (f *IgnoreFill) View(bat *batch.Batch) *batch.Batch {
	if f.view == nil {
		f.view = batch.NewWithSize(0)
	}
	bat.ViewInto(f.view)
	return f.view
}

// owns reports whether vec is one of the IgnoreFill's vectors.
func (f *IgnoreFill) owns(vec *vector.Vector) bool {
	return slices.Contains(f.vecs, vec) || slices.Contains(f.dropVecs, vec)
}

// DropNullPkRows removes every row of bat whose pkVec cell is NULL, with one
// ER_BAD_NULL_ERROR warning per dropped row, and reports whether rows were
// dropped. bat must be the caller's own batch (or a View). A vector the
// caller owns (owned reports its slot) or the IgnoreFill owns is shrunk in
// place; any other vector is replaced, in every slot holding it, by the
// IgnoreFill's copy of the kept rows.
func (f *IgnoreFill) DropNullPkRows(
	proc *process.Process,
	pkVec *vector.Vector,
	pkName string,
	bat *batch.Batch,
	owned func(int) bool,
) (bool, error) {
	if pkVec == nil || !pkVec.HasNull() {
		return false, nil
	}
	if mp := proc.Mp(); f.mp != mp {
		f.Free()
		f.mp = mp
	}
	var warnings process.WarningAccumulator
	warnings.SetWarningRetentionForProcess(proc)
	defer warnings.Reset()

	// The kept rows are the complement of the PK's NULL rows; only the NULL
	// rows are visited, each with its warning.
	rowCount := int(bat.RowCount())
	keep := &f.keep
	if err := keep.InitWithSize(int64(rowCount), f.mp); err != nil {
		return false, err
	}
	if rowCount > 0 && !pkVec.IsConstNull() {
		keep.AddRange(0, uint64(rowCount))
	}
	dropped := rowCount - keep.Count()
	if !pkVec.IsConst() {
		itr := pkVec.GetNulls().GetBitmap().IteratorValue()
		for itr.HasNext() {
			row := itr.Next()
			if row >= uint64(rowCount) {
				break
			}
			keep.Remove(row)
			dropped++
		}
	}
	for range dropped {
		warnings.Add(moerr.ER_BAD_NULL_ERROR,
			fmt.Sprintf("Column '%s' cannot be null", pkName))
	}
	warnings.Flush(proc)
	if dropped == 0 {
		return false, nil
	}
	// The row selection must reach every physical vector of the batch exactly
	// once. Columns that share one physical vector (repeated plain source
	// references of INSERT SELECT plans) keep consistent values this way, and
	// no aliased slot applies the selection again to a vector that was already
	// shrunk to the kept rows, which would read past its end.
	done := make(map[*vector.Vector]*vector.Vector, len(bat.Vecs))
	for slot, vec := range bat.Vecs {
		if vec == nil {
			continue
		}
		if replacement, ok := done[vec]; ok {
			bat.Vecs[slot] = replacement
			continue
		}
		if (owned != nil && owned(slot)) || f.owns(vec) {
			if err := vec.ShrinkByMask(keep, false, 0, f.mp); err != nil {
				return false, err
			}
			done[vec] = vec
			continue
		}
		for len(f.dropVecs) <= slot {
			f.dropVecs = append(f.dropVecs, nil)
		}
		dst := f.dropVecs[slot]
		if dst != nil && dst.GetType().Eq(*vec.GetType()) {
			dst.CleanOnlyData()
			dst.SetClass(vector.FLAT)
		} else {
			if dst != nil {
				dst.Free(f.mp)
			}
			dst = vector.NewVec(*vec.GetType(), f.mp)
			f.dropVecs[slot] = dst
		}
		if err := dst.UnionByMask(vec, keep, f.mp); err != nil {
			return false, err
		}
		done[vec] = dst
		bat.Vecs[slot] = dst
	}
	bat.SetRowCount(keep.Count())
	return true, nil
}

// Fill adjusts vecs (aligned with attrs and cols) in place of the caller's
// slots and returns the indices of the adjusted slots. owned, when non-nil,
// reports the slots whose vectors the caller owns and created with proc's
// pool.
func (f *IgnoreFill) Fill(
	vecs []*vector.Vector,
	attrs []string,
	cols []*plan.ColDef,
	owned func(int) bool,
	proc *process.Process,
) ([]int, error) {
	mp := proc.Mp()
	if f.mp != mp {
		// A reused operator may run with another process pool.
		f.Free()
		f.mp = mp
	}
	var warnings process.WarningAccumulator
	warnings.SetWarningRetentionForProcess(proc)
	defer warnings.Reset()

	var filled []int
	for j := range vecs {
		vec := vecs[j]
		if vec == nil || j >= len(cols) || j >= len(attrs) || !ColForbidsNull(cols[j]) {
			continue
		}
		if vec.IsConst() {
			if vec.Length() <= 0 || !vec.IsNull(0) {
				continue
			}
		} else if !vec.GetNulls().Any() {
			continue
		}
		attr := attrs[j]
		target := vec
		if owned == nil || !owned(j) {
			target = f.vector(j, *vec.GetType())
		}
		adjusted, ok, err := fillImplicitNotNullDefaults(target, vec, mp)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, moerr.NewConstraintViolation(proc.Ctx,
				fmt.Sprintf("Column '%s' cannot be null", attr))
		}
		for range adjusted {
			warnings.Add(moerr.ER_BAD_NULL_ERROR,
				fmt.Sprintf("Column '%s' cannot be null", attr))
		}
		vecs[j] = target
		filled = append(filled, j)
	}
	warnings.Flush(proc)
	return filled, nil
}

// vector returns the IgnoreFill's empty reusable vector for slot j.
func (f *IgnoreFill) vector(j int, typ types.Type) *vector.Vector {
	for len(f.vecs) <= j {
		f.vecs = append(f.vecs, nil)
	}
	vec := f.vecs[j]
	if vec != nil && vec.GetType().Eq(typ) {
		vec.CleanOnlyData()
		vec.SetClass(vector.FLAT)
		return vec
	}
	if vec != nil {
		vec.Free(f.mp)
	}
	vec = vector.NewVec(typ, f.mp)
	f.vecs[j] = vec
	return vec
}

// Copy returns the IgnoreFill's vector for slot j holding a copy of src: a
// caller that must change rows of a vector it does not own changes this copy
// and installs it in its own slot instead.
func (f *IgnoreFill) Copy(j int, src *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if mp := proc.Mp(); f.mp != mp {
		f.Free()
		f.mp = mp
	}
	dst := f.vector(j, *src.GetType())
	if err := dst.UnionBatch(src, 0, src.Length(), nil, f.mp); err != nil {
		return nil, err
	}
	return dst, nil
}

// Free releases the IgnoreFill's vectors.
func (f *IgnoreFill) Free() {
	for _, vec := range f.vecs {
		if vec != nil {
			vec.Free(f.mp)
		}
	}
	for _, vec := range f.dropVecs {
		if vec != nil {
			vec.Free(f.mp)
		}
	}
	f.vecs, f.dropVecs = nil, nil
	f.view = nil
	f.keep.Free()
	f.mp = nil
}

// fillImplicitNotNullDefaults makes target hold source's rows with every
// NULL cell replaced by the implicit default of the type, and returns the
// number of adjusted logical rows. target is source itself (owned by the
// caller) or an empty vector of mp. A constant NULL broadcast carries no
// backing storage, so it becomes an equivalent constant of the implicit
// default. It reports false when the type has no implicit default.
func fillImplicitNotNullDefaults(target, source *vector.Vector, mp *mpool.MPool) (int, bool, error) {
	if source.IsConst() {
		rows := source.Length()
		if target == source {
			target.CleanOnlyData()
			target.SetClass(vector.FLAT)
		}
		if err := vector.AppendAny(target, nil, true, mp); err != nil {
			return 0, false, err
		}
		// The appended NULL slot is the cell the shared rule overwrites.
		ok, err := setImplicitNotNullDefault(target, 0, mp)
		if err != nil || !ok {
			return 0, ok, err
		}
		target.GetNulls().Del(0)
		target.ToConst()
		target.SetLength(rows)
		return rows, true, nil
	}
	if target != source {
		if err := target.UnionBatch(source, 0, source.Length(), nil, mp); err != nil {
			return 0, false, err
		}
	}
	adjusted := 0
	for slot := 0; slot < target.Length(); slot++ {
		if !target.IsNull(uint64(slot)) {
			continue
		}
		ok, err := setImplicitNotNullDefault(target, slot, mp)
		if err != nil || !ok {
			return 0, ok, err
		}
		target.GetNulls().Del(uint64(slot))
		adjusted++
	}
	return adjusted, true, nil
}

// setImplicitNotNullDefault overwrites one NULL cell with the implicit default
// of its column type. It reports false when the type has no defined implicit
// default; nothing is modified in that case.
func setImplicitNotNullDefault(vec *vector.Vector, slot int, mp *mpool.MPool) (bool, error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, false)
	case types.T_bit:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, uint64(0))
	case types.T_int8:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, int8(0))
	case types.T_int16:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, int16(0))
	case types.T_int32:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, int32(0))
	case types.T_int64:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, int64(0))
	case types.T_uint8:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, uint8(0))
	case types.T_uint16:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, uint16(0))
	case types.T_uint32:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, uint32(0))
	case types.T_uint64:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, uint64(0))
	case types.T_float32:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, float32(0))
	case types.T_float64:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, float64(0))
	case types.T_bf16:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.BF16(0))
	case types.T_float16:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Float16(0))
	case types.T_float8:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Float8(0))
	case types.T_float4:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Float4(0))
	case types.T_decimal64:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Decimal64(0))
	case types.T_decimal128:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Decimal128{})
	case types.T_decimal256:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Decimal256{})
	case types.T_date:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.ZeroDate)
	case types.T_datetime:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.ZeroDatetime)
	case types.T_time:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Time(0))
	case types.T_timestamp:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.ZeroTimestamp)
	case types.T_year:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.MoYear(0))
	case types.T_enum:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Enum(0))
	case types.T_uuid:
		return true, vector.SetFixedAtWithTypeCheck(vec, slot, types.Uuid{})
	case types.T_binary:
		// BINARY(n) is fixed width: a plain assignment right-pads any
		// shorter value with 0x00 to the declared width - the rule the
		// string->binary cast applies (func_cast strToStr) and
		// dtype/binary.result pins - so the implicit default is n zero
		// bytes, exactly what an ordinary empty-string assignment stores.
		// A width without room cannot be padded and keeps the empty
		// payload, mirroring the cast's len < width guard.
		payload := []byte{}
		if w := int(vec.GetType().Width); w > 0 {
			payload = make([]byte, w)
		}
		return true, vector.SetBytesAt(vec, slot, payload, mp)
	case types.T_char, types.T_varchar, types.T_varbinary, types.T_blob, types.T_text:
		return true, vector.SetBytesAt(vec, slot, []byte{}, mp)
	case types.T_geometry, types.T_geometry32, types.T_datalink:
		// A geometry payload is an interpreted WKB/EWKB value and a datalink
		// value is a URL that ValidateDatalink must parse ((load_file reads
		// it back through its scheme); the empty byte string - the shared
		// string-family fill - is a valid payload for neither, so these
		// families have no implicit default at all. A NULL cell keeps the
		// constraint violation (MySQL 8 rejects the adjusted row as well,
		// with error 3673 for geometry) rather than storing a value the
		// functions cannot read back.
		return false, nil
	case types.T_json:
		// MySQL's implicit default for a NOT NULL JSON column is the JSON
		// null literal, which is a stored value distinct from SQL NULL.
		// JSON vectors keep the binary bytejson serialization, so encode
		// the literal exactly like the cast runtime does.
		payload, err := types.EncodeJson(bytejson.Null)
		if err != nil {
			return false, err
		}
		return true, vector.SetBytesAt(vec, slot, payload, mp)
	default:
		return false, nil
	}
}

// IgnoreNullPkColumnIdx returns the TableDef.Cols index of the one primary
// key column whose supplied NULL takes the pinned per-statement drop under
// INSERT IGNORE: a single-column primary key that really forbids NULL and
// is not auto-incremented. Composite, cluster-by, fake and auto-increment
// keys are not resolvable (-1): those shapes follow the regular fill,
// because several NULL keys are then legal (composite and cluster-by
// keys own no single cell) or the allocator has already assigned a value
// before the fill runs (auto-increment). This is the one resolution rule
// shared by the pre-insert drop and the write-side backstop; trusting a
// single planner-supplied position instead resolved the key to column 0
// for ordinary INSERT plans, which silently moved the rule sideways for
// tables whose primary key is not the first column.
func IgnoreNullPkColumnIdx(tableDef *plan.TableDef) int {
	if tableDef == nil || tableDef.Pkey == nil || len(tableDef.Pkey.Names) != 1 {
		return -1
	}
	name := tableDef.Pkey.Names[0]
	if name == catalog.FakePrimaryKeyColName || name == catalog.CPrimaryKeyColName {
		return -1
	}
	for i, col := range tableDef.Cols {
		if col == nil || (col.Name != name && col.GetOriginCaseName() != name) {
			continue
		}
		if col.Typ.AutoIncr || !ColForbidsNull(col) {
			return -1
		}
		return i
	}
	return -1
}

func getRelationByObjRef(ctx context.Context, proc *process.Process, eg engine.Engine, ref *plan.ObjectRef) (engine.Relation, error) {
	objName := ref.ObjName
	if ses := proc.GetSession(); ses != nil {
		if real, ok := ses.GetTempTable(ref.SchemaName, objName); ok {
			objName = real
		}
	}

	dbSource, err := eg.Database(ctx, ref.SchemaName, proc.GetTxnOperator())
	if err != nil {
		return nil, err
	}
	relation, err := dbSource.Relation(ctx, objName, proc)
	if err != nil {
		return nil, err
	}

	if relation == nil {
		return nil, moerr.NewNoSuchTable(ctx, ref.SchemaName, objName)
	}

	return engine.NewRelationHandle(relation), nil
}

func GetRelAndPartitionRelsByObjRef(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	ref *plan.ObjectRef,
) (source engine.Relation, err error) {
	source, err = getRelationByObjRef(proc.Ctx, proc, eng, ref)
	if err != nil {
		return
	}
	return
}

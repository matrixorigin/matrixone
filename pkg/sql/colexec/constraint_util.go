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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

func FilterRowIdForDel(proc *process.Process, retBat *batch.Batch, srcBat *batch.Batch,
	idx int, primaryKeyIdx int) error {
	sels := vector.GetSels()
	defer vector.PutSels(sels)
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
			sels = append(sels, int64(i))
		}
	}
	err := rowidVec.Union(srcBat.Vecs[idx], sels, proc.Mp())
	if err != nil {
		return err
	}
	err = primaryVec.Union(srcBat.Vecs[primaryKeyIdx], sels, proc.Mp())
	if err != nil {
		return err
	}
	retBat.SetRowCount(len(sels))
	return nil
}

func BatchDataNotNullCheck(vecs []*vector.Vector, attrs []string, tableDef *plan.TableDef, ctx context.Context) error {
	for j := range vecs {
		if vecs[j] == nil {
			continue
		}
		nsp := vecs[j].GetNulls()
		if tableDef.Cols[j].Default != nil && !tableDef.Cols[j].Default.NullAbility && nulls.Any(nsp) {
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

// BatchDataNotNullFillForIgnore adapts one batch to the IGNORE statement rule
// for NOT NULL columns: each NULL cell is replaced with the implicit default
// of its column type and reported as one ER_BAD_NULL_ERROR warning, matching
// the value adjustment MySQL applies for INSERT/UPDATE/LOAD with IGNORE. The
// row itself is kept. vec indices whose vectors were modified are returned so
// selector pipelines can mirror the adjustment into their un-shrunk output.
// A constant NULL broadcast carries no backing storage, so it is replaced by
// an equivalent constant of the implicit default instead of being written
// in place.
// A NOT NULL column whose type has no implicit default keeps the
// constraint-violation error: storing NULL would trade the report for corruption.
func BatchDataNotNullFillForIgnore(
	vecs []*vector.Vector,
	attrs []string,
	cols []*plan.ColDef,
	proc *process.Process,
) ([]int, error) {
	var warnings process.WarningAccumulator
	warnings.SetWarningRetentionForProcess(proc)
	defer warnings.Reset()

	var filled []int
	for j := range vecs {
		vec := vecs[j]
		if vec == nil || j >= len(cols) || j >= len(attrs) || !ColForbidsNull(cols[j]) {
			continue
		}
		attr := attrs[j]
		if vec.IsConst() {
			// A constant vector broadcasts its single physical slot, and a
			// constant NULL owns no backing storage at all (NewConstNull
			// allocates no data), so the in-place setters below would fail
			// on it: LOAD legs project an omitted column's folded NULL
			// default straight into this helper. Replace the broadcast with
			// a constant carrying the implicit default - the sanctioned
			// NewConstFixed shape - so every logical row keeps its row and
			// one warning is published per adjusted row.
			if !vec.IsNull(0) || vec.Length() <= 0 {
				continue
			}
			rows := vec.Length()
			fillVec, ok, err := constImplicitNotNullDefaultVec(vec, rows, proc.Mp())
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, moerr.NewConstraintViolation(proc.Ctx,
					fmt.Sprintf("Column '%s' cannot be null", attr))
			}
			for row := 0; row < rows; row++ {
				warnings.Add(moerr.ER_BAD_NULL_ERROR,
					fmt.Sprintf("Column '%s' cannot be null", attr))
			}
			vecs[j] = fillVec
			filled = append(filled, j)
			continue
		}
		slotCount := vec.Length()
		adjusted := false
		for slot := 0; slot < slotCount; slot++ {
			if !vec.IsNull(uint64(slot)) {
				continue
			}
			ok, err := setImplicitNotNullDefault(vec, slot, proc.Mp())
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, moerr.NewConstraintViolation(proc.Ctx,
					fmt.Sprintf("Column '%s' cannot be null", attr))
			}
			nulls.Del(vec.GetNulls(), uint64(slot))
			adjusted = true
			warnings.Add(moerr.ER_BAD_NULL_ERROR,
				fmt.Sprintf("Column '%s' cannot be null", attr))
		}
		if adjusted {
			filled = append(filled, j)
		}
	}
	warnings.Flush(proc)
	return filled, nil
}

// constImplicitNotNullDefaultVec builds the replacement for a constant NULL
// broadcast: one physical slot carrying the implicit default of the column
// type, broadcast over the original logical length. The value comes from the
// shared per-type rule, so the broadcast fill and the in-place fill can never
// diverge. It reports false when the type has no implicit default.
func constImplicitNotNullDefaultVec(vec *vector.Vector, rows int, mp *mpool.MPool) (*vector.Vector, bool, error) {
	dst := vector.NewVec(*vec.GetType())
	if err := vector.AppendAny(dst, nil, true, mp); err != nil {
		dst.Free(mp)
		return nil, false, err
	}
	// The appended NULL slot is the cell the shared rule overwrites.
	ok, err := setImplicitNotNullDefault(dst, 0, mp)
	if err != nil || !ok {
		dst.Free(mp)
		return nil, ok, err
	}
	nulls.Del(dst.GetNulls(), 0)
	dst.ToConst()
	dst.SetLength(rows)
	return dst, true, nil
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

// DropNullPkRowsForIgnore removes from bat every row whose pkVec cell is
// NULL, publishing one ER_BAD_NULL_ERROR warning per dropped row with
// pkName, and reports whether any row was dropped. It is the shared body of
// the pinned single-column NULL primary-key drop: the pre-insert pass calls
// it before CHECK, unique and foreign-key consumers evaluate the batch, the
// write-side backstop calls it for pipelines without that node (LOAD legs)
// and for rows those consumers let through.
func DropNullPkRowsForIgnore(
	proc *process.Process,
	pkVec *vector.Vector,
	pkName string,
	bat *batch.Batch,
) (bool, error) {
	if pkVec == nil || !pkVec.HasNull() {
		return false, nil
	}
	var warnings process.WarningAccumulator
	warnings.SetWarningRetentionForProcess(proc)
	defer warnings.Reset()

	rowCount := int(bat.RowCount())
	keep := make([]int64, 0, rowCount)
	for row := 0; row < rowCount; row++ {
		if pkVec.IsNull(uint64(row)) {
			warnings.Add(moerr.ER_BAD_NULL_ERROR,
				fmt.Sprintf("Column '%s' cannot be null", pkName))
			continue
		}
		keep = append(keep, int64(row))
	}
	warnings.Flush(proc)
	if len(keep) == rowCount {
		return false, nil
	}
	// The row selection must reach every physical vector of the batch exactly
	// once. Columns that share one physical vector (repeated plain source
	// references of INSERT SELECT plans) keep consistent values this way, and
	// no aliased slot applies the selection again to a vector that was already
	// shrunk to the kept rows, which would read past its end.
	shrank := make(map[*vector.Vector]struct{}, len(bat.Vecs))
	for _, vec := range bat.Vecs {
		if vec == nil {
			continue
		}
		if _, done := shrank[vec]; done {
			continue
		}
		shrank[vec] = struct{}{}
		if err := vec.Shrink(keep, false, proc.Mp()); err != nil {
			return false, err
		}
	}
	bat.SetRowCount(len(keep))
	return true, nil
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

// Copyright 2025 MatrixOne
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

package cdc

import (
	"context"
	"testing"

	"database/sql"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"regexp"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// --------------------------------------------------------------------------------
// Bound-tree builders matching the persisted generated-expression shapes that
// the real binder produces (proven by the instrumented e2e fence dump):
//
//	int g AS (a + 1):
//	  Cast(result=int32, funcid-cast, argc=2) {
//	    Plus(result=int64, funcid-+, argc=2) {
//	      Cast(result=int64) { ColRef(pos of a, typ=int32) ; Expr_T(typ=int64) }
//	    ,
//	      Lit(I64Val 1, typ=int64) ... wrapped entries below follow the same shapes }
//	    , Expr_T(typ=int32) }
//
// The positive pins here keep the real persisted layout: explicit cast nodes,
// pos-only column references (empty names), int64 literals and Expr_T markers.
// --------------------------------------------------------------------------------

func genSigColRef(pos int32, typ int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{ColPos: pos},
		},
		Typ: plan.Type{Id: typ},
	}
}

func genSigI64(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: v},
			},
		},
		Typ: plan.Type{Id: int32(types.T_int64)},
	}
}

func genSigTargetType(typ int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_T{T: &plan.TargetType{}},
		Typ:  plan.Type{Id: typ},
	}
}

func genSigCast(resultTyp int32, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "cast", Obj: 90194313216},
				Args: args,
			},
		},
		Typ: plan.Type{Id: resultTyp},
	}
}

func genSigFunc(name string, resultTyp int32, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: name},
				Args: args,
			},
		},
		Typ: plan.Type{Id: resultTyp},
	}
}

func genSigStrLit(v string, typ int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: v},
			},
		},
		Typ: plan.Type{Id: typ},
	}
}

// genSigPersistedAPlusOne reproduces the persisted shape of int g AS (a+1).
func genSigPersistedAPlusOne(aPos int32) *plan.Expr {
	innerPlus := genSigFunc("+", int32(types.T_int64),
		genSigCast(int32(types.T_int64), genSigColRef(aPos, int32(types.T_int32)), genSigTargetType(int32(types.T_int64))),
		genSigI64(1))
	return genSigCast(int32(types.T_int32), innerPlus, genSigTargetType(int32(types.T_int32)))
}

func TestCDCGeneratedExprSafePersistedShapes(t *testing.T) {
	int32ID := int32(types.T_int32)
	int64ID := int32(types.T_int64)

	// Positive: persisted int arithmetic (a + 1)Stored real layout.
	require.True(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: genSigPersistedAPlusOne(1), IsStored: true},
		2, types.T_int32))

	// Positive: int multiply with the same persisted layout.
	mul := genSigFunc("*", int64ID,
		genSigCast(int64ID, genSigColRef(0, int32ID), genSigTargetType(int64ID)),
		genSigI64(10))
	require.True(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: genSigFunc("+", int64ID, mul, genSigI64(1)), IsStored: true},
		2, types.T_int64))

	// Positive: constant WKT geometry constructor over a string literal.
	geom := genSigFunc("st_geomfromtext", int32(types.T_geometry), genSigStrLit("POINT(1 2)", int32(types.T_varchar)))
	require.True(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: geom, IsStored: true},
		4, types.T_geometry))
	geomSrid := genSigFunc("st_geomfromtext", int32(types.T_geometry), genSigStrLit("POINT(1 2)", int32(types.T_varchar)), genSigI64(0))
	require.True(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: geomSrid, IsStored: true},
		4, types.T_geometry))

	// Negative: session-context functions are fenced regardless of casts:
	// FROM_UNIXTIME reads the session time zone.
	fromUnix := genSigFunc("from_unixtime", int32(types.T_datetime), genSigColRef(0, int32ID))
	withDateCast := genSigCast(int32(types.T_date), fromUnix, genSigTargetType(int32(types.T_date)))
	require.False(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: withDateCast, IsStored: true},
		2, types.T_date))

	// Negative: a cast with a non int-family result stays fenced (the typed
	// DATE(TIMESTAMP) same-instant counterexample cannot slip through a cast).
	tsToDate := genSigCast(int32(types.T_date), genSigColRef(0, int32(types.T_timestamp)), genSigTargetType(int32(types.T_date)))
	require.False(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: tsToDate, IsStored: true},
		2, types.T_date))

	// Negative: date(VARCHAR) resolved overload - non int result cast.
	charToDate := genSigCast(int32(types.T_date), genSigColRef(0, int32(types.T_varchar)), genSigTargetType(int32(types.T_date)))
	require.False(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: charToDate, IsStored: true},
		2, types.T_date))

	// Negative: unknown function.
	require.False(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: genSigFunc("some_future_fn", int32ID, genSigI64(1)), IsStored: true},
		2, types.T_int32))

	// Negative: a reference at or below the first generated ordinal.
	require.False(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: genSigPersistedAPlusOne(2), IsStored: true},
		2, types.T_int32))

	// Negative: missing typed tree is never admitted.
	require.False(t, CDCGeneratedExprSafe(&plan.GeneratedCol{}, 2, types.T_int32))
	require.False(t, CDCGeneratedExprSafe(nil, 2, types.T_int32))

	// Positive: a generated-free definition admits every visible ordinal.
	require.True(t, CDCGeneratedExprSafe(
		&plan.GeneratedCol{Expr: genSigPersistedAPlusOne(1), IsStored: true},
		-1, types.T_int32))
}

func genSigVisible(defs ...*plan.ColDef) []*plan.ColDef {
	return defs
}

func genSigOrdinary(name string, typ int32) *plan.ColDef {
	return &plan.ColDef{Name: name, Typ: plan.Type{Id: typ}}
}

// genSigGeneratedStored builds a stored int generated column of the persisted
// shape used by the positive fixtures.
func genSigGeneratedStored(name string, expr *plan.Expr, typ int32) *plan.ColDef {
	return &plan.ColDef{
		Name:         name,
		Typ:          plan.Type{Id: typ},
		GeneratedCol: &plan.GeneratedCol{Expr: expr, IsStored: true},
	}
}

// -------- classify contract oracles over a mocked admission queryer --------

// TestClassifyCDCGenerationOrdinaryTargetCopiesAllValues proves the W decision
// on an all-ordinary target: every visible source ordinal copies its
// materialized value, generated slots included, and the mapping stays
// insensitive for the per-DML signature fence.
func TestClassifyCDCGenerationOrdinaryTargetCopiesAllValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	sourceGenerated := genSigGeneratedStored("g", genSigPersistedAPlusOne(1), int32(types.T_int32))
	visible := genSigVisible(
		genSigOrdinary("id", int32(types.T_int32)),
		genSigOrdinary("a", int32(types.T_int32)),
		sourceGenerated,
	)

	sigRows := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"})
	for i, col := range visible {
		sigRows.AddRow(int64(i), col.Name, string(CDCColumnOrdinary), int64(1), int64(0))
	}
	mock.ExpectQuery("CALL mo_cdc_target_generation_signature").WillReturnRows(sigRows)

	info := &DbTableInfo{SinkDbName: "sdb", SinkTblName: "t"}
	contract, err := classifyCDCTargetGeneration(context.Background(), db, info, CDCSinkType_MO, visible)
	require.NoError(t, err)
	require.NotNil(t, contract)
	require.Equal(t, []bool{true, true, true}, contract.Copy)
	require.Empty(t, contract.GeneratedOrdinals)
	// The copied source-generated slot makes this mapping projection
	// sensitive: its target layout drifts under the per-DML fence like any
	// schema-dependent projection.
	require.True(t, contract.ProjectionSensitive)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestClassifyCDCGenerationGeneratedTargetAdmitsProofedOmission proves the G
// decision: the ordinary slot keeps copying while the proven compatible
// generated slot omits, with kind, safety and digest all checked.
func TestClassifyCDCGenerationGeneratedTargetAdmitsProofedOmission(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	sourceGenerated := genSigGeneratedStored("g", genSigPersistedAPlusOne(1), int32(types.T_int32))
	visible := genSigVisible(
		genSigOrdinary("id", int32(types.T_int32)),
		genSigOrdinary("a", int32(types.T_int32)),
		sourceGenerated,
	)
	// The guarded call returns the exact canonical typed digest of the
	// target column, byte-equal to the source side by proven DDL identity.
	targetDigest, targetDigestComplete := CDCColumnTypedDigestFromColDef(sourceGenerated, 2)
	require.True(t, targetDigestComplete, "the persisted int arithmetic tree must render completely")

	sigRows := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"})
	sigRows.AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(0))
	sigRows.AddRow(int64(1), "a", string(CDCColumnOrdinary), int64(1), int64(0))
	sigRows.AddRow(int64(2), "g", string(CDCColumnGeneratedStored), int64(1), int64(targetDigest))
	mock.ExpectQuery("CALL mo_cdc_target_generation_signature").WillReturnRows(sigRows)

	info := &DbTableInfo{SinkDbName: "sdb", SinkTblName: "t"}
	contract, err := classifyCDCTargetGeneration(context.Background(), db, info, CDCSinkType_MO, visible)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, false}, contract.Copy)
	require.Equal(t, []int32{2}, contract.GeneratedOrdinals)
	require.True(t, contract.ProjectionSensitive)
	require.True(t, CDCContractMatchesSignature(contract, []byte{CDCColumnOrdinary, CDCColumnOrdinary, CDCColumnGeneratedStored},
		[]uint64{0, 0, targetDigest}))
	require.False(t, CDCContractMatchesSignature(contract, []byte{CDCColumnOrdinary, CDCColumnOrdinary, CDCColumnGeneratedVirt},
		[]uint64{0, 0, targetDigest}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestClassifyCDCGenerationFencesDateAndDrift proves the distinct fenced
// failures: expression drift (digest mismatch), kind drift, session-context
// DATE expressions and the missing metadata boundary.
func TestClassifyCDCGenerationFencesDateAndDrift(t *testing.T) {
	ctx := context.Background()
	int32ID := int32(types.T_int32)

	// Digest mismatch: same name/type/kind with a different bound tree.
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	sourceGenerated := genSigGeneratedStored("g", genSigPersistedAPlusOne(1), int32ID)
	visible := genSigVisible(genSigOrdinary("id", int32ID), genSigOrdinary("a", int32ID), sourceGenerated)
	differentDigest, driftComplete := CDCColumnTypedDigestFromColDef(sourceGenerated, 2)
	require.True(t, driftComplete)
	differentDigest ^= 0x1
	sigRows := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"})
	sigRows.AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(0))
	sigRows.AddRow(int64(1), "a", string(CDCColumnOrdinary), int64(1), int64(0))
	sigRows.AddRow(int64(2), "g", string(CDCColumnGeneratedStored), int64(1), int64(differentDigest))
	mock.ExpectQuery("CALL mo_cdc_target_generation_signature").WillReturnRows(sigRows)
	_, err = classifyCDCTargetGeneration(ctx, db, &DbTableInfo{SinkDbName: "sdb", SinkTblName: "t"}, CDCSinkType_MO, visible)
	require.ErrorContains(t, err, "generated expression differs")
	require.NoError(t, mock.ExpectationsWereMet())
	_ = db.Close()

	// Kind drift: source stored, target virtual with equal digest.
	db2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	storedDigest, kindDriftComplete := CDCColumnTypedDigestFromColDef(sourceGenerated, 2)
	require.True(t, kindDriftComplete)
	rows2 := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"})
	rows2.AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(0))
	rows2.AddRow(int64(1), "a", string(CDCColumnOrdinary), int64(1), int64(0))
	rows2.AddRow(int64(2), "g", string(CDCColumnGeneratedVirt), int64(1), int64(storedDigest))
	mock2.ExpectQuery("CALL mo_cdc_target_generation_signature").WillReturnRows(rows2)
	_, err = classifyCDCTargetGeneration(ctx, db2, &DbTableInfo{SinkDbName: "sdb", SinkTblName: "t"}, CDCSinkType_MO, visible)
	require.ErrorContains(t, err, "generation kind differs")
	require.NoError(t, mock2.ExpectationsWereMet())
	_ = db2.Close()

	// Session-context expression: DATE from FROM_UNIXTIME stays fenced even
	// when the digest claims equality.
	db3, mock3, err := sqlmock.New()
	require.NoError(t, err)
	dateGen := &plan.ColDef{
		Name: "d",
		Typ:  plan.Type{Id: int32(types.T_date)},
		GeneratedCol: &plan.GeneratedCol{
			Expr: genSigCast(int32(types.T_date),
				genSigFunc("from_unixtime", int32(types.T_int64), genSigColRef(0, int32ID)),
				genSigTargetType(int32(types.T_date))),
			IsStored: true,
		},
	}
	digest3, dateComplete := CDCColumnTypedDigestFromColDef(dateGen, 2)
	require.True(t, dateComplete)
	rows3 := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"})
	rows3.AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(0))
	rows3.AddRow(int64(1), "n", string(CDCColumnOrdinary), int64(1), int64(0))
	rows3.AddRow(int64(2), "d", string(CDCColumnGeneratedStored), int64(1), int64(digest3))
	mock3.ExpectQuery("CALL mo_cdc_target_generation_signature").WillReturnRows(rows3)
	_, err = classifyCDCTargetGeneration(ctx, db3, &DbTableInfo{SinkDbName: "sdb", SinkTblName: "t"}, CDCSinkType_MO,
		genSigVisible(genSigOrdinary("id", int32ID), genSigOrdinary("n", int32ID), dateGen))
	require.ErrorContains(t, err, "session-context dependent generated expression")
	require.NoError(t, mock3.ExpectationsWereMet())
	_ = db3.Close()

	// Missing signature row: distinguished from an ordinary column.
	db4, mock4, err := sqlmock.New()
	require.NoError(t, err)
	emptyRows := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"})
	emptyRows.AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(0))
	mock4.ExpectQuery("CALL mo_cdc_target_generation_signature").WillReturnRows(emptyRows)
	_, err = classifyCDCTargetGeneration(ctx, db4, &DbTableInfo{SinkDbName: "sdb", SinkTblName: "t"}, CDCSinkType_MO, visible)
	require.ErrorContains(t, err, "has no generation metadata; the target schema is incomplete or denied")
	require.NoError(t, mock4.ExpectationsWereMet())
	_ = db4.Close()
}

// TestClassifyCDCGenerationMySQLOrdinaryCopyAndGeneratedFence pins the backend
// split: an ordinary MySQL target keeps copying source values (merge-base
// parity is preserved) while a generated target fails closed with the bounded
// alternative, before any DML or acknowledgement.
func TestClassifyCDCGenerationMySQLOrdinaryCopyAndGeneratedFence(t *testing.T) {
	ctx := context.Background()
	int32ID := int32(types.T_int32)
	sourceGenerated := genSigGeneratedStored("g", genSigPersistedAPlusOne(1), int32ID)
	visible := genSigVisible(genSigOrdinary("id", int32ID), genSigOrdinary("a", int32ID), sourceGenerated)

	// Ordinary target: EXTRA empty for every column copies all values.
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	extraRows := sqlmock.NewRows([]string{"column_name", "extra"})
	extraRows.AddRow("id", "")
	extraRows.AddRow("a", "")
	extraRows.AddRow("g", "DEFAULT_GENERATED")
	mock.ExpectQuery("SELECT column_name, extra FROM information_schema.columns").WillReturnRows(extraRows)
	contract, err := classifyCDCTargetGeneration(ctx, db, &DbTableInfo{SinkDbName: "mdb", SinkTblName: "t"}, CDCSinkType_MySQL, visible)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true}, contract.Copy)
	require.Empty(t, contract.GeneratedOrdinals)
	require.NoError(t, mock.ExpectationsWereMet())
	_ = db.Close()

	// Generated target on MySQL: fenced unsupported recomputation.
	db2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	extraRows2 := sqlmock.NewRows([]string{"column_name", "extra"})
	extraRows2.AddRow("id", "")
	extraRows2.AddRow("a", "")
	extraRows2.AddRow("g", "VIRTUAL GENERATED")
	mock2.ExpectQuery("SELECT column_name, extra FROM information_schema.columns").WillReturnRows(extraRows2)
	_, err = classifyCDCTargetGeneration(ctx, db2, &DbTableInfo{SinkDbName: "mdb", SinkTblName: "t"}, CDCSinkType_MySQL, visible)
	require.ErrorContains(t, err, "generated recomputation is not supported")
	require.NoError(t, mock2.ExpectationsWereMet())
	_ = db2.Close()
}

// TestClassifyCDCGenerationWritableDecodeUnsupportedFences proves an ordinary
// writable target never silently accepts an undecodable generated payload: a
// geometry slot replicated into a writable column fails admission.
func TestClassifyCDCGenerationWritableDecodeUnsupportedFences(t *testing.T) {
	ctx := context.Background()
	geomGen := &plan.ColDef{
		Name: "gs",
		Typ:  plan.Type{Id: int32(types.T_geometry)},
		GeneratedCol: &plan.GeneratedCol{
			Expr:     genSigFunc("st_geomfromtext", int32(types.T_geometry), genSigStrLit("POINT(1 2)", int32(types.T_varchar))),
			IsStored: true,
		},
	}
	visible := genSigVisible(genSigOrdinary("id", int32(types.T_int32)), geomGen)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	sigRows := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"})
	sigRows.AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(0))
	sigRows.AddRow(int64(1), "gs", string(CDCColumnOrdinary), int64(1), int64(0))
	mock.ExpectQuery("CALL mo_cdc_target_generation_signature").WillReturnRows(sigRows)
	_, err = classifyCDCTargetGeneration(ctx, db, &DbTableInfo{SinkDbName: "sdb", SinkTblName: "t"}, CDCSinkType_MO, visible)
	require.ErrorContains(t, err, "generated payload type cannot be decoded")
	require.NoError(t, mock.ExpectationsWereMet())
	_ = db.Close()
}

// TestCDCColumnTypedDigestFailClosed pins the shared depth bound: a tree the
// canonicalizer cannot render completely refuses (no aliasing between two
// erased deep trees) and is never session-free-proven either.
func TestCDCColumnTypedDigestFailClosed(t *testing.T) {
	var tail *plan.Expr = genSigI64(1)
	tail = genSigCast(int32(types.T_int64), tail, genSigTargetType(int32(types.T_int64)))
	for i := 0; i < 40; i++ {
		tail = genSigFunc("+", int32(types.T_int64), tail, genSigI64(1))
		tail = genSigCast(int32(types.T_int64), tail, genSigTargetType(int32(types.T_int64)))
	}
	deep := genSigGeneratedStored("deep", tail, int32(types.T_int64))
	digest, complete := CDCColumnTypedDigestFromColDef(deep, 1)
	require.False(t, complete, "a too-deep tree must refuse the digest")
	require.Zero(t, digest)
	require.False(t, CDCGeneratedExprSafe(deep.GetGeneratedCol(), 2, types.T_int64),
		"a too-deep tree is never session-free-proven")
}

// TestCDCColumnTypedDigestDiscriminatesTypeSemantics proves value-relevant
// type attributes render into the digest: equal names with different
// charsets never collide, and complete ordinary columns stay matchable.
func TestCDCColumnTypedDigestDiscriminatesTypeSemantics(t *testing.T) {
	one := genSigOrdinary("s", int32(types.T_varchar))
	one.Typ.Charset = 1
	two := genSigOrdinary("s", int32(types.T_varchar))
	two.Typ.Charset = 2
	d1, ok1 := CDCColumnTypedDigestFromColDef(one, 0)
	d2, ok2 := CDCColumnTypedDigestFromColDef(two, 0)
	require.True(t, ok1)
	require.True(t, ok2)
	require.NotEqual(t, d1, d2)
	shallow := genSigGeneratedStored("g", genSigPersistedAPlusOne(1), int32(types.T_int32))
	d3, ok3 := CDCColumnTypedDigestFromColDef(shallow, 2)
	require.True(t, ok3)
	require.NotEqual(t, d3, d1)
}

// TestRevalidateCDCTargetGenerationRoutes pins the transaction dispatcher:
// a MatrixOne-sensitive contract re-reads the guarded signature CALL inside
// the sinker transaction, every other backend re-reads the catalog layout and
// must hold the admitted names and generation classes, and an insensitive
// contract never queries at all.
func TestRevalidateCDCTargetGenerationRoutes(t *testing.T) {
	ctx := context.Background()
	newCase := func(sinkType string, sensitive bool) (*sql.DB, sqlmock.Sqlmock, *DbTableInfo) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		return db, mock, &DbTableInfo{
			SinkDbName:     "dst",
			SinkTblName:    "t",
			TargetSinkType: sinkType,
			GenerationContract: &CDCTargetGenerationContract{
				Copy:                []bool{true, true},
				TargetClass:         []byte{CDCColumnOrdinary, CDCColumnOrdinary},
				TargetNames:         []string{"id", "v"},
				TargetDigest:        []uint64{42, 43},
				ProjectionSensitive: sensitive,
			},
		}
	}
	begin := func(t *testing.T, db *sql.DB) *sql.Tx {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		return tx
	}
	layoutRows := func(names ...[2]string) *sqlmock.Rows {
		rows := sqlmock.NewRows([]string{"column_name", "extra"})
		for _, pair := range names {
			rows.AddRow(pair[0], pair[1])
		}
		return rows
	}

	db, mock, info := newCase(CDCSinkType_MySQL, true)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, extra FROM information_schema.columns").
		WithArgs("dst", "t").WillReturnRows(layoutRows([2]string{"id", ""}, [2]string{"v", ""}))
	require.NoError(t, revalidateCDCTargetGeneration(ctx, begin(t, db), info))
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, db.Close())

	db, mock, info = newCase(CDCSinkType_MO, true)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("CALL mo_cdc_target_generation_signature('dst', 't')")).
		WillReturnRows(sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"}).
			AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(42)).
			AddRow(int64(1), "v", string(CDCColumnOrdinary), int64(1), int64(43)))
	require.NoError(t, revalidateCDCTargetGeneration(ctx, begin(t, db), info))
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, db.Close())

	for _, drift := range []struct {
		name  string
		rows  *sqlmock.Rows
		fence string
	}{
		{"renamed layout column", layoutRows([2]string{"id", ""}, [2]string{"drifted", ""}), "unexpected column layout"},
		{"generation class drift", layoutRows([2]string{"id", ""}, [2]string{"v", "VIRTUAL GENERATED"}), "changed generation class"},
		{"short layout", layoutRows([2]string{"id", ""}), "incomplete"},
	} {
		db, mock, info = newCase(CDCSinkType_MySQL, true)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT column_name, extra FROM information_schema.columns").
			WithArgs("dst", "t").WillReturnRows(drift.rows)
		err := revalidateCDCTargetGeneration(ctx, begin(t, db), info)
		require.ErrorContains(t, err, drift.fence, drift.name)
		require.NoError(t, mock.ExpectationsWereMet())
		require.NoError(t, db.Close())
	}

	db, mock, info = newCase(CDCSinkType_MO, false)
	mock.ExpectBegin()
	require.NoError(t, revalidateCDCTargetGeneration(ctx, begin(t, db), info),
		"an insensitive contract must not query at per-DML cost")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, db.Close())
}

// TestClassifyLegacyMatrixOneFallbackMatrix pins the bounded older-target
// compatibility: ONLY an explicit not-supported/unknown-routine error naming
// the typed signature call takes the legacy branch, which must prove ordinary
// through the older catalog channel (mo_columns.attr_generated is non-empty
// exactly for generated columns). Generated, missing and denied metadata all
// fail closed, and any other error on the routine never reaches the fallback
// (the catalog query is never issued).
func TestClassifyShowColumnsOrdinaryMatrix(t *testing.T) {
	ctx := context.Background()
	headers := []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	headersNoExtra := []string{"Field", "Type", "Null", "Key", "Default", "Comment"}
	visible := genSigVisible(genSigOrdinary("id", int32(types.T_int32)), genSigOrdinary("v", int32(types.T_int32)))
	generatedVisible := genSigVisible(genSigOrdinary("id", int32(types.T_int32)),
		&plan.ColDef{Name: "v", Typ: plan.Type{Id: int32(types.T_int32)}, GeneratedCol: &plan.GeneratedCol{IsStored: true}})

	lowPrecGenVisible := genSigVisible(genSigOrdinary("id", int32(types.T_int32)),
		&plan.ColDef{Name: "gb", Typ: plan.Type{Id: int32(types.T_bf16)}, GeneratedCol: &plan.GeneratedCol{IsStored: true}},
		&plan.ColDef{Name: "gh", Typ: plan.Type{Id: int32(types.T_float16)}, GeneratedCol: &plan.GeneratedCol{IsStored: true}},
		&plan.ColDef{Name: "g8", Typ: plan.Type{Id: int32(types.T_float8)}, GeneratedCol: &plan.GeneratedCol{IsStored: true}},
		&plan.ColDef{Name: "g4", Typ: plan.Type{Id: int32(types.T_float4)}, GeneratedCol: &plan.GeneratedCol{IsStored: true}})
	jsonGenVisible := genSigVisible(genSigOrdinary("id", int32(types.T_int32)),
		&plan.ColDef{Name: "v", Typ: plan.Type{Id: int32(types.T_json)}, GeneratedCol: &plan.GeneratedCol{IsStored: true}})
	ordinaryTypedRows := func(names ...string) *sqlmock.Rows {
		rows := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"})
		rows.AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(7))
		for i, n := range names {
			rows.AddRow(int64(i+1), n, string(CDCColumnOrdinary), int64(1), int64(8+i))
		}
		return rows
	}

	mixedCaseVisible := genSigVisible(genSigOrdinary("id", int32(types.T_int32)),
		&plan.ColDef{Name: "myval", OriginName: "MyVal", Typ: plan.Type{Id: int32(types.T_int32)}})

	fullRows := func(pairs ...[2]string) *sqlmock.Rows {
		rows := sqlmock.NewRows(headers)
		for _, p := range pairs {
			rows.AddRow(p[0], "int(11)", "", "YES", "", nil, p[1], "select,insert,update,references", "")
		}
		return rows
	}

	routes := []struct {
		name        string
		visible     []*plan.ColDef
		rows        *sqlmock.Rows
		showErr     error
		expectTyped bool
		check       func(t *testing.T, c *CDCTargetGenerationContract, err error)
	}{
		{"ordinary states all pass", visible, fullRows([2]string{"id", ""}, [2]string{"v", "DEFAULT_GENERATED"}), nil, false,
			func(t *testing.T, c *CDCTargetGenerationContract, err error) {
				require.NoError(t, err)
				require.Equal(t, []bool{true, true}, c.Copy)
				require.Equal(t, []byte{CDCColumnOrdinary, CDCColumnOrdinary}, c.TargetClass)
				require.False(t, c.ProjectionSensitive, "an ordinary proven layout keeps zero per-DML reads")
			}},
		{"auto_increment stays ordinary", visible, fullRows([2]string{"id", "auto_increment"}, [2]string{"v", ""}), nil, false,
			func(t *testing.T, c *CDCTargetGenerationContract, err error) {
				require.NoError(t, err, "the auto-increment Extra state must not mask or invent generation")
				require.False(t, c.ProjectionSensitive)
			}},
		{"stored generated target fails closed", visible, fullRows([2]string{"id", ""}, [2]string{"v", "STORED GENERATED"}), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "is generated while the source column is not")
			}},
		{"virtual generated target fails closed", visible, fullRows([2]string{"id", ""}, [2]string{"v", "VIRTUAL GENERATED"}), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "is generated while the source column is not")
			}},
		{"null extra state fails closed", visible, sqlmock.NewRows(headers).
			AddRow("id", "int(11)", "", "YES", "", nil, nil, "select,insert,update,references", "").
			AddRow("v", "int(11)", "", "YES", "", nil, nil, "select,insert,update,references", ""), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "unknown SHOW generation state")
			}},
		{"conflicting column identity fails closed", visible, fullRows([2]string{"id", ""}, [2]string{"other", ""}), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "conflicting column identity")
			}},
		{"incomplete layout fails closed", visible, fullRows([2]string{"id", ""}), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "SHOW metadata is incomplete")
			}},
		{"header variant without Extra fails closed", visible, sqlmock.NewRows(headersNoExtra).
			AddRow("id", "int(11)", "YES", "", nil, "").
			AddRow("v", "int(11)", "YES", "", nil, ""), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "SHOW metadata header variant is unsupported")
			}},
		{"repeated Extra header fails closed before any row is trusted", visible, sqlmock.NewRows([]string{"Field", "Extra", "Extra"}).
			AddRow("id", "", "STORED GENERATED").
			AddRow("v", "", ""), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "repeats the Extra header")
			}},
		{"repeated Field header fails closed", visible, sqlmock.NewRows([]string{"Field", "Field", "Extra"}).
			AddRow("id", "v", ""), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "repeats the Field header")
			}},
		{"mixed-case generated lexeme is not admitted", visible, fullRows([2]string{"id", ""}, [2]string{"v", "stored generated"}), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "unsupported SHOW state")
			}},
		{"unrecognized Extra lexeme is not admitted", visible, fullRows([2]string{"id", ""}, [2]string{"v", "on update current_timestamp"}), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "unsupported SHOW state")
			}},
		{"NULL Field fails closed", visible, sqlmock.NewRows(headers).
			AddRow(nil, "int(11)", "", "YES", "", nil, "", "select,insert,update,references", "").
			AddRow("v", "int(11)", "", "YES", "", nil, "", "select,insert,update,references", ""), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "invalid column field")
			}},
		{"mixed-case Field matches the declared spelling", mixedCaseVisible,
			fullRows([2]string{"id", ""}, [2]string{"MyVal", "auto_increment"}), nil, false,
			func(t *testing.T, c *CDCTargetGenerationContract, err error) {
				require.NoError(t, err, "the SHOW Field must compare against the source's original-case contract")
				require.Equal(t, []bool{true, true}, c.Copy)
				require.Equal(t, "MyVal", c.TargetNames[1])
				require.False(t, c.ProjectionSensitive)
			}},
		{"case-drifted Field is a conflicting identity", mixedCaseVisible,
			fullRows([2]string{"id", ""}, [2]string{"myval", ""}), nil, false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "conflicting column identity")
			}},
		{"denied or absent target fails closed with no inference", visible, nil,
			moerr.NewInternalErrorf(ctx, "Error 20101: do not have privilege to execute the statement"), false,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.Error(t, err, "a denied or absent SHOW must fail closed")
			}},
		{name: "low-precision generated payloads copy into an ordinary writable target",
			visible:     lowPrecGenVisible,
			expectTyped: true,
			rows:        ordinaryTypedRows("gb", "gh", "g8", "g4"),
			check: func(t *testing.T, c *CDCTargetGenerationContract, err error) {
				require.NoError(t, err, "the four proven-decodable low-precision payloads must admit the W copy through the real classifier")
				require.Equal(t, []bool{true, true, true, true, true}, c.Copy)
				require.Equal(t, []byte{CDCColumnOrdinary, CDCColumnOrdinary, CDCColumnOrdinary, CDCColumnOrdinary, CDCColumnOrdinary}, c.TargetClass)
			}},
		{name: "unsupported generated payload keeps the fail-closed fence",
			visible:     jsonGenVisible,
			expectTyped: true,
			rows:        ordinaryTypedRows("v"),
			check: func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				require.ErrorContains(t, err, "cannot be decoded for replication")
			}},
		{"generated source never touches SHOW metadata", generatedVisible, nil, nil, true,
			func(t *testing.T, _ *CDCTargetGenerationContract, err error) {
				// The typed guarded path owns the outcome here - a fence for an
				// expression-less fixture contract is exactly right - while
				// ExpectationsWereMet proves the SHOW read was never issued.
				require.Error(t, err, "the expression-less fixture contract must stay on the typed path")
				require.NotContains(t, err.Error(), "SHOW", "the generation-free SHOW route must never serve a generated source")
			}},
	}
	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			if tc.expectTyped {
				// The shared `rows` field carries this route's typed CALL rows
				// when it supplies them; otherwise the fixture default applies.
				typedRows := sqlmock.NewRows([]string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"}).
					AddRow(int64(0), "id", string(CDCColumnOrdinary), int64(1), int64(7)).
					AddRow(int64(1), "v", string(CDCColumnGeneratedStored), int64(1), int64(8))
				if tc.rows != nil {
					typedRows = tc.rows
				}
				mock.ExpectQuery(regexp.QuoteMeta("CALL mo_cdc_target_generation_signature('dst', 't')")).
					WillReturnRows(typedRows)
			} else if tc.showErr != nil {
				mock.ExpectQuery(regexp.QuoteMeta("SHOW FULL COLUMNS FROM `dst`.`t`")).WillReturnError(tc.showErr)
			} else {
				mock.ExpectQuery(regexp.QuoteMeta("SHOW FULL COLUMNS FROM `dst`.`t`")).WillReturnRows(tc.rows)
			}
			c, err := classifyCDCTargetGeneration(ctx, db, &DbTableInfo{SinkDbName: "dst", SinkTblName: "t"}, CDCSinkType_MO, tc.visible)
			tc.check(t, c, err)
			require.NoError(t, mock.ExpectationsWereMet())
			_ = db.Close()
		})
	}
}

// TestClassifyResumedOrdinaryTargetRequiresTheGuardedRead pins the
// durable-resume shape: an empty pre-identity with an already-acknowledged
// target identity is NOT fresh creation, so the guarded metadata read must
// prove the resumed target ordinary - and a generated column on it fails
// closed before the mapping becomes ready.
func TestClassifyResumedOrdinaryTargetRequiresTheGuardedRead(t *testing.T) {
	ctx := context.Background()
	visible := genSigVisible(genSigOrdinary("id", int32(types.T_int32)), genSigOrdinary("v", int32(types.T_int32)))
	resumed := &DbTableInfo{SinkDbName: "dst", SinkTblName: "t", TargetPreIdentity: "", TargetIdentity: "mo:1:9"}

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta("SHOW FULL COLUMNS FROM `dst`.`t`")).
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}).
			AddRow("id", "int(11)", "", "YES", "", nil, "", "select,insert,update,references", "").
			AddRow("v", "int(11)", "", "YES", "", nil, "", "select,insert,update,references", ""))
	contract, err := classifyCDCTargetGeneration(ctx, db, resumed, CDCSinkType_MO, visible)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, contract.Copy)
	require.False(t, contract.ProjectionSensitive, "a proven ordinary resumed target keeps zero per-DML cost")
	require.NoError(t, mock.ExpectationsWereMet())
	_ = db.Close()

	db2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	mock2.ExpectQuery(regexp.QuoteMeta("SHOW FULL COLUMNS FROM `dst`.`t`")).
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}).
			AddRow("id", "int(11)", "", "YES", "", nil, "", "select,insert,update,references", "").
			AddRow("v", "int(11)", "", "YES", "", nil, "STORED GENERATED", "select,insert,update,references", ""))
	_, err = classifyCDCTargetGeneration(ctx, db2, resumed, CDCSinkType_MO, visible)
	require.ErrorContains(t, err, "is generated while the source column is not")
	require.NoError(t, mock2.ExpectationsWereMet())
	_ = db2.Close()
}

// TestClassifyGeneratedFreeMOVerifiedOrdinaryKeepsPerDmlCost: a generated-free
// ordinary mapping is classified from the guarded metadata read (exclusive
// creation is not provable - a stale absent observation can be followed by
// CREATE and a foreign replacement of equal names and types). A target
// verified ordinary copies every column and keeps ZERO per-DML reads; a
// generated column reported for the same observed-absent target fences at
// admission, which is exactly the raced-replacement discriminator.
func TestClassifyGeneratedFreeMOVerifiedOrdinaryKeepsPerDmlCost(t *testing.T) {
	ctx := context.Background()
	visible := genSigVisible(genSigOrdinary("id", int32(types.T_int32)), genSigOrdinary("v", int32(types.T_int32)))
	observedAbsent := &DbTableInfo{SinkDbName: "dst", SinkTblName: "t", TargetPreIdentity: absentCDCTargetIdentity}

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta("SHOW FULL COLUMNS FROM `dst`.`t`")).
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}).
			AddRow("id", "int(11)", "", "YES", "", nil, "", "select,insert,update,references", "").
			AddRow("v", "int(11)", "", "YES", "", nil, "", "select,insert,update,references", ""))
	contract, err := classifyCDCTargetGeneration(ctx, db, observedAbsent, CDCSinkType_MO, visible)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, contract.Copy)
	require.Equal(t, []byte{CDCColumnOrdinary, CDCColumnOrdinary}, contract.TargetClass)
	require.False(t, contract.ProjectionSensitive,
		"an ordinary mapping verified under the guard keeps zero per-DML reads")
	require.NoError(t, mock.ExpectationsWereMet())
	_ = db.Close()

	db2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	mock2.ExpectQuery(regexp.QuoteMeta("SHOW FULL COLUMNS FROM `dst`.`t`")).
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}).
			AddRow("id", "int(11)", "", "YES", "", nil, "", "select,insert,update,references", "").
			AddRow("v", "int(11)", "", "YES", "", nil, "STORED GENERATED", "select,insert,update,references", ""))
	_, err = classifyCDCTargetGeneration(ctx, db2, observedAbsent, CDCSinkType_MO, visible)
	require.ErrorContains(t, err, "is generated while the source column is not",
		"a generated replacement after a stale absent observation must fence at admission")
	require.NoError(t, mock2.ExpectationsWereMet())
	_ = db2.Close()
}

// TestCDCSourceHasGeneratedColumn pins the unverified-mapping gate: only a
// generated visible column forbids the legacy omit-all fallback.
func TestCDCSourceHasGeneratedColumn(t *testing.T) {
	require.False(t, CDCSourceHasGeneratedColumn(nil))
	require.False(t, CDCSourceHasGeneratedColumn([]*plan.ColDef{
		genSigOrdinary("id", int32(types.T_int32)),
	}))
	require.True(t, CDCSourceHasGeneratedColumn([]*plan.ColDef{
		genSigOrdinary("id", int32(types.T_int32)),
		genSigGeneratedStored("g", genSigPersistedAPlusOne(1), int32(types.T_int32)),
	}))
}

// TestCDCGeneratedExprFirstOrdinal pins the first generated visible ordinal
// helper on mixed layouts.
func TestCDCGeneratedExprFirstOrdinal(t *testing.T) {
	require.Equal(t, int32(-1), CDCGeneratedExprFirstOrdinal(genSigVisible(
		genSigOrdinary("id", int32(types.T_int32)), genSigOrdinary("a", int32(types.T_int64)))))
	gen := genSigGeneratedStored("g", genSigPersistedAPlusOne(1), int32(types.T_int32))
	require.Equal(t, int32(1), CDCGeneratedExprFirstOrdinal(genSigVisible(
		genSigOrdinary("id", int32(types.T_int32)), gen, genSigOrdinary("b", int32(types.T_int32)))))
}

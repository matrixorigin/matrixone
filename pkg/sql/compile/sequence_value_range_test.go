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

package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// Issue #31908: CREATE/ALTER SEQUENCE must reject explicit START WITH /
// MINVALUE / MAXVALUE magnitudes and signs that do not fit the declared
// sequence integer domain before converting them. A silent Go narrowing
// persisted a wrapped value the user never wrote (smallint START WITH 70000
// came back from nextval as 4464; smallint unsigned START WITH -1 as 65535;
// int MINVALUE -4294967296 became 0), and out-of-range errors reported the
// wrapped value instead of the user's input.
func parseCreateSequence(t *testing.T, sql string) *tree.CreateSequence {
	t.Helper()
	stmt, err := parsers.ParseOne(testutil.NewProc(t).Ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	create, ok := stmt.(*tree.CreateSequence)
	require.True(t, ok, "%q did not parse as CreateSequence", sql)
	// The statement comes from the reuse pool. Final use is inside this
	// (sub)test, so returning it to the pool only at cleanup keeps the pooled
	// AST alive for every assertion without freeing prematurely (#31908
	// review: register Free after final use, not in the helper's return path).
	t.Cleanup(create.Free)
	return create
}

// The rejection matrix: every rejected literal must name the CLAUSE, the
// VALUE THE USER WROTE (never a wrapped one), and the declared type. The
// parser feeds the magnitude as int64 (<= math.MaxInt64) or uint64, with the
// sign split into a separate token, so 9223372036854775808 exercises the
// uint64 path.
func TestMakeSequenceParamRejectsOutOfDomainValues(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	for _, tc := range []struct {
		name string
		sql  string
		frag string
		run  func(ctx context.Context, stmt *tree.CreateSequence) error
	}{
		{"smallint start",
			"CREATE SEQUENCE s AS SMALLINT START WITH 70000",
			"STARTVALUE (70000) is out of range for sequence data type SMALLINT",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[int16](ctx, stmt)
				return err
			}},
		{"smallint min",
			"CREATE SEQUENCE s AS SMALLINT MINVALUE -32769",
			"MINVALUE (-32769) is out of range for sequence data type SMALLINT",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[int16](ctx, stmt)
				return err
			}},
		{"smallint max",
			"CREATE SEQUENCE s AS SMALLINT MAXVALUE 99999",
			"MAXVALUE (99999) is out of range for sequence data type SMALLINT",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[int16](ctx, stmt)
				return err
			}},
		{"int start",
			"CREATE SEQUENCE s AS INT START WITH 2147483648",
			"STARTVALUE (2147483648) is out of range for sequence data type INT",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[int32](ctx, stmt)
				return err
			}},
		{"int min",
			"CREATE SEQUENCE s AS INT MINVALUE -4294967296",
			"MINVALUE (-4294967296) is out of range for sequence data type INT",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[int32](ctx, stmt)
				return err
			}},
		{"bigint start uint64 literal",
			"CREATE SEQUENCE s AS BIGINT START WITH 9223372036854775808",
			"STARTVALUE (9223372036854775808) is out of range for sequence data type BIGINT",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[int64](ctx, stmt)
				return err
			}},
		{"bigint min",
			"CREATE SEQUENCE s AS BIGINT MINVALUE -9223372036854775809",
			"MINVALUE (-9223372036854775809) is out of range for sequence data type BIGINT",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[int64](ctx, stmt)
				return err
			}},
		{"smallint unsigned start negative",
			"CREATE SEQUENCE s AS SMALLINT UNSIGNED START WITH -1",
			"STARTVALUE (-1) is out of range for sequence data type SMALLINT UNSIGNED",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[uint16](ctx, stmt)
				return err
			}},
		{"int unsigned start",
			"CREATE SEQUENCE s AS INT UNSIGNED START WITH 4294967296",
			"STARTVALUE (4294967296) is out of range for sequence data type INT UNSIGNED",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[uint32](ctx, stmt)
				return err
			}},
		{"bigint unsigned min negative",
			"CREATE SEQUENCE s AS BIGINT UNSIGNED MINVALUE -1",
			"MINVALUE (-1) is out of range for sequence data type BIGINT UNSIGNED",
			func(ctx context.Context, stmt *tree.CreateSequence) error {
				_, _, _, _, err := makeSequenceParam[uint64](ctx, stmt)
				return err
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(ctx, parseCreateSequence(t, tc.sql))
			require.ErrorContains(t, err, tc.frag, tc.sql)
		})
	}
}

func TestMakeSequenceParamAcceptsDomainBoundariesAndRejectsOverflow(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	t.Run("smallint boundaries accepted", func(t *testing.T) {
		stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS SMALLINT MINVALUE -32768 MAXVALUE 32767 START WITH 32767")
		incr, minV, maxV, start, err := makeSequenceParam[int16](ctx, stmt)
		require.NoError(t, err)
		require.Equal(t, int64(1), incr)
		require.Equal(t, int16(-32768), minV)
		require.Equal(t, int16(32767), maxV)
		require.Equal(t, int16(32767), start)
	})
	t.Run("smallint lower boundary as start", func(t *testing.T) {
		stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS SMALLINT START WITH -32768")
		_, _, _, start, err := makeSequenceParam[int16](ctx, stmt)
		require.NoError(t, err)
		require.Equal(t, int16(-32768), start)
	})
	t.Run("smallint uint64-literal boundary rejected", func(t *testing.T) {
		stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS BIGINT START WITH 9223372036854775808")
		_, _, _, _, err := makeSequenceParam[int64](ctx, stmt)
		require.ErrorContains(t, err, "STARTVALUE (9223372036854775808) is out of range for sequence data type BIGINT")
	})
	t.Run("bigint negative limit accepted", func(t *testing.T) {
		stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS BIGINT MINVALUE -9223372036854775808")
		_, minV, _, _, err := makeSequenceParam[int64](ctx, stmt)
		require.NoError(t, err)
		require.Equal(t, int64(-9223372036854775808), minV)
	})
	t.Run("int boundaries accepted", func(t *testing.T) {
		stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS INT MINVALUE -2147483648 MAXVALUE 2147483647 START WITH 2147483647")
		_, minV, maxV, start, err := makeSequenceParam[int32](ctx, stmt)
		require.NoError(t, err)
		require.Equal(t, int32(-2147483648), minV)
		require.Equal(t, int32(2147483647), maxV)
		require.Equal(t, int32(2147483647), start)
	})
	t.Run("smallint unsigned boundaries accepted", func(t *testing.T) {
		stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS SMALLINT UNSIGNED MINVALUE 0 MAXVALUE 65535 START WITH 65535")
		_, minV, maxV, start, err := makeSequenceParam[uint16](ctx, stmt)
		require.NoError(t, err)
		require.Equal(t, uint16(0), minV)
		require.Equal(t, uint16(65535), maxV)
		require.Equal(t, uint16(65535), start)
	})
	t.Run("int unsigned boundaries accepted", func(t *testing.T) {
		stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS INT UNSIGNED MINVALUE 0 MAXVALUE 4294967295 START WITH 4294967295")
		_, minV, maxV, start, err := makeSequenceParam[uint32](ctx, stmt)
		require.NoError(t, err)
		require.Equal(t, uint32(0), minV)
		require.Equal(t, uint32(4294967295), maxV)
		require.Equal(t, uint32(4294967295), start)
	})
	t.Run("bigint unsigned boundaries accepted", func(t *testing.T) {
		stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS BIGINT UNSIGNED MINVALUE 0 MAXVALUE 18446744073709551615 START WITH 18446744073709551615")
		_, minV, maxV, start, err := makeSequenceParam[uint64](ctx, stmt)
		require.NoError(t, err)
		require.Equal(t, uint64(0), minV)
		require.Equal(t, uint64(18446744073709551615), maxV)
		require.Equal(t, uint64(18446744073709551615), start)
	})
}

// The full init-batch owner: an accepted boundary persists exact typed values
// (last/min/max/start stored as the declared type, increment as int64), and
// a rejected literal produces only the error — no batch to persist.
func TestSequenceInitBatchRejectsOutOfDomainStart(t *testing.T) {
	rejected := []struct {
		name string
		oid  types.T
		sql  string
		frag string
	}{
		{"smallint", types.T_int16, "CREATE SEQUENCE s AS SMALLINT START WITH 70000",
			"STARTVALUE (70000) is out of range for sequence data type SMALLINT"},
		{"smallint unsigned negative", types.T_uint16, "CREATE SEQUENCE s AS SMALLINT UNSIGNED START WITH -1",
			"STARTVALUE (-1) is out of range for sequence data type SMALLINT UNSIGNED"},
		{"int min", types.T_int32, "CREATE SEQUENCE s AS INT MINVALUE -4294967296 MAXVALUE 10",
			"MINVALUE (-4294967296) is out of range for sequence data type INT"},
		{"bigint uint64 literal", types.T_int64, "CREATE SEQUENCE s AS BIGINT START WITH 9223372036854775808",
			"STARTVALUE (9223372036854775808) is out of range for sequence data type BIGINT"},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			table := &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(tc.oid)}}}}
			bat, err := makeSequenceInitBatch(proc.Ctx, parseCreateSequence(t, tc.sql), table, proc)
			require.ErrorContains(t, err, tc.frag)
			require.Nil(t, bat)
			require.Equal(t, baseline, proc.Mp().CurrNB(), "rejected input must not retain a partial batch")
		})
	}
}

func TestSequenceInitBatchPersistsExactInt16Boundaries(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	baseline := proc.Mp().CurrNB()
	table := &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int16)}}}}
	stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS SMALLINT MINVALUE -32768 MAXVALUE 32767 START WITH 32767 INCREMENT BY 2")
	bat, err := makeSequenceInitBatch(proc.Ctx, stmt, table, proc)
	require.NoError(t, err)
	require.NotNil(t, bat)
	defer func() {
		bat.Clean(proc.Mp())
		require.Equal(t, baseline, proc.Mp().CurrNB(), "all batch vectors must be released")
	}()
	require.Equal(t, []string{"last_seq_num", "min_value", "max_value", "start_value", "increment_value", "cycle", "is_called"}, bat.Attrs)
	for i, want := range []int16{32767, -32768, 32767, 32767} {
		require.Equal(t, types.T_int16, bat.Vecs[i].GetType().Oid)
		require.Equal(t, []int16{want}, vector.MustFixedColWithTypeCheck[int16](bat.Vecs[i]))
	}
	require.Equal(t, []int64{2}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[4]))
	require.Equal(t, []bool{false}, vector.MustFixedColWithTypeCheck[bool](bat.Vecs[5]))
	require.Equal(t, []bool{false}, vector.MustFixedColWithTypeCheck[bool](bat.Vecs[6]))
}

// ALTER gets the same domain validation; a rejected ALTER leaves the stored
// row untouched (the caller never issues an update from a rejected batch).
func TestMakeAlterSequenceParamRejectsOutOfDomainValues(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	t.Run("smallint start", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s START WITH 70000")
		row := sequenceRow[int16](10, 1, 100, 10, 1, false, true)
		_, _, _, _, _, _, _, err := makeAlterSequenceParam[int16](ctx, stmt, row, "10")
		require.ErrorContains(t, err, "STARTVALUE (70000) is out of range for sequence data type SMALLINT")
		require.Equal(t, sequenceRow[int16](10, 1, 100, 10, 1, false, true), row, "computing the row must not mutate the stored state")
	})
	t.Run("smallint min", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s MINVALUE -32769")
		row := sequenceRow[int16](10, 1, 100, 10, 1, false, true)
		_, _, _, _, _, _, _, err := makeAlterSequenceParam[int16](ctx, stmt, row, "10")
		require.ErrorContains(t, err, "MINVALUE (-32769) is out of range for sequence data type SMALLINT")
	})
	t.Run("smallint unsigned start", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s START WITH -1")
		row := sequenceRow[uint16](10, 0, 100, 10, 1, false, true)
		_, _, _, _, _, _, _, err := makeAlterSequenceParam[uint16](ctx, stmt, row, "10")
		require.ErrorContains(t, err, "STARTVALUE (-1) is out of range for sequence data type SMALLINT UNSIGNED")
	})
}

func TestSequenceAlterBatchRejectsOutOfDomainValues(t *testing.T) {
	for _, tc := range []struct {
		name, sql, frag string
		oid             types.T
	}{
		{"smallint start", "ALTER SEQUENCE s START WITH 70000",
			"STARTVALUE (70000) is out of range for sequence data type SMALLINT", types.T_int16},
		{"smallint uint magnitude", "ALTER SEQUENCE s MAXVALUE 70000",
			"MAXVALUE (70000) is out of range for sequence data type SMALLINT", types.T_int16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			row := sequenceRow[int16](10, 1, 100, 10, 1, false, true)
			table := &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(tc.oid)}}}}
			bat, _, err := makeSequenceAlterBatch(proc.Ctx, parseAlterSequence(t, tc.sql), table, proc, row, "")
			require.ErrorContains(t, err, tc.frag)
			require.Nil(t, bat)
			require.Equal(t, baseline, proc.Mp().CurrNB(), "rejected input must not retain a partial batch")
			require.Equal(t, sequenceRow[int16](10, 1, 100, 10, 1, false, true), row)
		})
	}
}

// Control pin, not a regression: INCREMENT is persisted with its own
// documented int64 width and is deliberately NOT narrowed to the sequence
// type (#31908 scope: only START/MINVALUE/MAXVALUE). Any literal the parser
// feeds as int64 stays accepted for every sequence type; only uint64
// magnitudes beyond int64 are rejected by the existing well-formedness check.
func TestSequenceIncrementKeepsInt64Width(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	stmt := parseCreateSequence(t, "CREATE SEQUENCE s AS SMALLINT INCREMENT BY 4294967297")
	incr, _, _, _, err := makeSequenceParam[int16](ctx, stmt)
	require.NoError(t, err)
	require.Equal(t, int64(4294967297), incr)

	alter := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY 65537")
	row := sequenceRow[int16](10, 1, 100, 10, 1, false, true)
	_, incr2, _, _, _, _, _, err := makeAlterSequenceParam[int16](ctx, alter, row, "10")
	require.NoError(t, err)
	require.Equal(t, int64(65537), incr2)
	require.Equal(t, int16(10), row[0], "an ACCEPTED alter never mutates its input row; persistence happens via the batch")
}

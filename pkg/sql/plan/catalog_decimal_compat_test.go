// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// These TableDefs were bound and protobuf-serialized by the real CREATE TABLE
// planner at base 63a964a82f, not made by injecting obsolete casts into a new plan.
func legacyDecimalCatalogFixtures(t *testing.T) map[string][]byte {
	t.Helper()
	data, err := os.ReadFile("testdata/legacy_decimal_catalog_63a964a82f.json")
	require.NoError(t, err)
	var fixtures map[string][]byte
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.Len(t, fixtures, 10)
	return fixtures
}

func legacyDecimalPredicate(def *pb.TableDef) *pb.Expr {
	if len(def.Checks) > 0 {
		return def.Checks[0].Check
	}
	return def.Cols[2].GeneratedCol.Expr
}

func executeLegacyDecimalExpression(t *testing.T, def *pb.TableDef, expression *pb.Expr, value uint64) (*vector.Vector, error) {
	t.Helper()
	proc := newPlanTestProcess(t)
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(makeTypeByPlan2Type(def.Cols[0].Typ))
	input.Vecs[1] = vector.NewVec(makeTypeByPlan2Type(def.Cols[1].Typ))
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	if makeTypeByPlan2Type(def.Cols[0].Typ).IsInt() {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(value), false, proc.Mp()))
	} else {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
	}
	require.NoError(t, vector.AppendFixed(input.Vecs[1], types.Time(0), false, proc.Mp()))
	input.SetRowCount(1)
	exec, err := colexec.NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	result, err := exec.Eval(proc, []*batch.Batch{input}, nil)
	if err != nil {
		return nil, err
	}
	require.False(t, result.IsNull(0))
	return result, nil
}

func executeLegacyDecimalPredicate(t *testing.T, def *pb.TableDef, value uint64) (bool, error) {
	t.Helper()
	result, err := executeLegacyDecimalExpression(t, def, legacyDecimalPredicate(def), value)
	if err != nil {
		return false, err
	}
	return vector.GetFixedAtWithTypeCheck[bool](result, 0), nil
}

func TestPersistedDecimalCoercionsSurviveCatalogUpgrade(t *testing.T) {
	fixtures := legacyDecimalCatalogFixtures(t)
	for _, name := range []string{"signed_compare", "unsigned_compare", "bit_compare", "arithmetic", "fractional_arithmetic", "generated_compare", "ranged_compare"} {
		t.Run(name, func(t *testing.T) {
			stored := new(pb.TableDef)
			require.NoError(t, stored.Unmarshal(fixtures[name]))
			wire, err := stored.Marshal()
			require.NoError(t, err)
			// Lower control works before normalization; the old cast at 10^18
			// demonstrates why fresh-schema-only testing misses the regression.
			lower := uint64(999999999999999999)
			if name == "fractional_arithmetic" {
				lower = 1
			}
			got, err := executeLegacyDecimalPredicate(t, stored, lower)
			require.NoError(t, err)
			require.True(t, got)
			_, err = executeLegacyDecimalPredicate(t, stored, 1000000000000000000)
			require.Error(t, err)
			owned := CloneTableDefForPlan(stored, true)
			proc := newPlanTestProcess(t)
			require.NoError(t, MigrateLegacyCatalogTableDef(proc, owned))
			for _, value := range []uint64{999999999999999999, 1000000000000000000, math.MaxInt64} {
				got, err = executeLegacyDecimalPredicate(t, owned, value)
				require.NoError(t, err, "value=%d", value)
				require.True(t, got)
			}
			if name == "unsigned_compare" || name == "bit_compare" {
				got, err = executeLegacyDecimalPredicate(t, owned, math.MaxUint64)
				require.NoError(t, err)
				require.True(t, got)
			}
			first, err := owned.Marshal()
			require.NoError(t, err)
			require.NoError(t, MigrateLegacyCatalogTableDef(proc, owned))
			second, err := owned.Marshal()
			require.NoError(t, err)
			require.Equal(t, first, second, "normalization must be idempotent")
			after, err := stored.Marshal()
			require.NoError(t, err)
			require.Equal(t, wire, after, "catalog bytes must stay unchanged")
		})
	}
}

func TestPersistedDecimalNormalizationPreservesDeclaredAssignment(t *testing.T) {
	stored := new(pb.TableDef)
	require.NoError(t, stored.Unmarshal(legacyDecimalCatalogFixtures(t)["declared_assignment"]))
	owned := CloneTableDefForPlan(stored, true)
	require.NoError(t, MigrateLegacyCatalogTableDef(newPlanTestProcess(t), owned))
	require.Same(t, stored.Cols[2], owned.Cols[2], "a declared assignment is not an operator coercion")
	result, err := executeLegacyDecimalExpression(t, owned, legacyDecimalPredicate(owned), 999999999999999999)
	require.NoError(t, err)
	require.Equal(t, "999999999999999999", formatDecimalResult(result))
	_, err = executeLegacyDecimalExpression(t, owned, legacyDecimalPredicate(owned), 1000000000000000000)
	require.Error(t, err, "actual DECIMAL(18,0) assignment must remain checked")
}

func TestPersistedDecimalNormalizationMatchesFreshBinding(t *testing.T) {
	stored := new(pb.TableDef)
	require.NoError(t, stored.Unmarshal(legacyDecimalCatalogFixtures(t)["signed_compare"]))
	proc := newPlanTestProcess(t)
	owned := CloneTableDefForPlan(stored, true)
	require.NoError(t, MigrateLegacyCatalogTableDef(proc, owned))
	stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "create table legacy_cmp(i bigint, tm time(0), constraint ck check(i > tm))", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	fresh, err := BuildPlan(NewMockCompilerContext(false, proc), stmt, false)
	require.NoError(t, err)
	def := fresh.GetDdl().GetCreateTable().GetTableDef()
	require.Equal(t, legacyDecimalPredicate(def), legacyDecimalPredicate(owned))
	before := def.Checks[0]
	require.NoError(t, MigrateLegacyCatalogTableDef(proc, def))
	require.Same(t, before, def.Checks[0], "fresh plans must be a no-op")
	for _, value := range []int64{math.MinInt64, -1000000000000000000, -1, 0, 1, 999999999999999999, 1000000000000000000, math.MaxInt64} {
		oldResult, err := executeLegacyDecimalPredicate(t, owned, uint64(value))
		require.NoError(t, err)
		newResult, err := executeLegacyDecimalPredicate(t, def, uint64(value))
		require.NoError(t, err)
		require.Equal(t, value > 0, oldResult)
		require.Equal(t, newResult, oldResult)
	}
}

func TestPersistedDecimalNormalizationOwnsOnlyChangedWrappers(t *testing.T) {
	stored := new(pb.TableDef)
	require.NoError(t, stored.Unmarshal(legacyDecimalCatalogFixtures(t)["signed_compare"]))
	expr := stored.Checks[0].Check
	// Reuse a genuine old bound expression to exercise all resolver wrapper
	// ownership paths; these wrappers are an aliasing test, not extra DDL fixtures.
	stored.Cols[0].Default = &pb.Default{Expr: expr, OriginString: "preserve default text", NullAbility: true}
	stored.Cols[0].OnUpdate = &pb.OnUpdate{Expr: expr, OriginString: "preserve update text"}
	stored.Cols[0].GeneratedCol = &pb.GeneratedCol{Expr: expr, OriginString: "preserve generated text", IsStored: true}
	before, err := stored.Marshal()
	require.NoError(t, err)
	for i := range 8 {
		t.Run(fmt.Sprintf("reader_%d", i), func(t *testing.T) {
			t.Parallel()
			owned := CloneTableDefForPlan(stored, true)
			require.NoError(t, MigrateLegacyCatalogTableDef(newPlanTestProcess(t), owned))
			require.NotSame(t, stored.Checks[0], owned.Checks[0])
			require.NotSame(t, stored.Cols[0], owned.Cols[0])
			require.Same(t, stored.Cols[1], owned.Cols[1])
			require.NotSame(t, stored.Cols[0].Default, owned.Cols[0].Default)
			require.NotSame(t, stored.Cols[0].OnUpdate, owned.Cols[0].OnUpdate)
			require.NotSame(t, stored.Cols[0].GeneratedCol, owned.Cols[0].GeneratedCol)
			// Once expression pointers are restored, every metadata byte agrees.
			owned.Checks[0].Check = expr
			owned.Cols[0].Default.Expr = expr
			owned.Cols[0].OnUpdate.Expr = expr
			owned.Cols[0].GeneratedCol.Expr = expr
			after, err := owned.Marshal()
			require.NoError(t, err)
			require.Equal(t, before, after)
			after, err = stored.Marshal()
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestPersistedDecimalNormalizationCancellation(t *testing.T) {
	stored := new(pb.TableDef)
	require.NoError(t, stored.Unmarshal(legacyDecimalCatalogFixtures(t)["signed_compare"]))
	before, err := stored.Marshal()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, changed, err := normalizeCatalogDecimalExpr(ctx, stored.Checks[0].Check)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, changed)
	after, err := stored.Marshal()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, MigrateLegacyCatalogTableDef(nil, nil))
}

func TestPersistedDecimalNormalizationPreservesExplicitCasts(t *testing.T) {
	fixtures := legacyDecimalCatalogFixtures(t)
	for _, name := range []string{"explicit_decimal", "nested_explicit"} {
		t.Run(name, func(t *testing.T) {
			stored := new(pb.TableDef)
			require.NoError(t, stored.Unmarshal(fixtures[name]))
			owned := CloneTableDefForPlan(stored, true)
			require.NoError(t, MigrateLegacyCatalogTableDef(newPlanTestProcess(t), owned))
			got, err := executeLegacyDecimalPredicate(t, owned, 999999999999999999)
			require.NoError(t, err)
			require.True(t, got)
			// Explicit CAST has its own clipping/warning policy; migration must
			// retain its target and overload, not turn it into an implicit widen.
			var findExplicit func(*pb.Expr) *pb.Expr
			findExplicit = func(expr *pb.Expr) *pb.Expr {
				if fn := expr.GetF(); fn != nil {
					if isCastOverload(expr, 1) && expr.Typ.Width == 18 {
						return expr
					}
					for _, arg := range fn.Args {
						if found := findExplicit(arg); found != nil {
							return found
						}
					}
				}
				return nil
			}
			before, after := findExplicit(legacyDecimalPredicate(stored)), findExplicit(legacyDecimalPredicate(owned))
			require.NotNil(t, before)
			require.NotNil(t, after)
			require.Equal(t, before.Typ, after.Typ)
			require.Equal(t, before.GetF().Func, after.GetF().Func)
			result, err := executeLegacyDecimalExpression(t, owned, after, 1000000000000000000)
			require.NoError(t, err)
			require.Equal(t, "999999999999999999", formatDecimalResult(result))
		})
	}
}

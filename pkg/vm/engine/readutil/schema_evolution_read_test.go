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

package readutil

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestSchemaEvolutionReaderAndFastPruningOracle(t *testing.T) {
	ctx := t.Context()
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mp := proc.Mp()
	fs, err := fileservice.NewLocalFS(ctx, "schema-evolution", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	idType, flagType := types.T_int64.ToType(), types.T_bool.ToType()
	table := &plan.TableDef{Name: "oracle", FeatureFlag: features.SchemaEvolved,
		Name2ColIndex: map[string]int32{"id": 0, "flag": 1}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Cols: []*plan.ColDef{{Name: "id", Typ: idType.PlanType(), Primary: true}, {Name: "flag", Seqnum: 1, Typ: flagType.PlanType(),
			Default: &plan.Default{NullAbility: true, Expr: plan2.MakePlan2BoolConstExprWithType(true), HasOriginDefault: true,
				OriginDefault: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}}}
	write := func(ids []int64, withFlag bool) objectio.ObjectStats {
		t.Helper()
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(idType)
		defer input.Clean(mp)
		seqs := []uint16{0}
		if withFlag {
			input.Vecs = append(input.Vecs, vector.NewVec(flagType))
			seqs = append(seqs, 1)
		}
		for _, id := range ids {
			require.NoError(t, vector.AppendFixed(input.Vecs[0], id, false, mp))
			if withFlag {
				require.NoError(t, vector.AppendFixed(input.Vecs[1], true, id == 4, mp))
			}
		}
		input.SetRowCount(len(ids))
		writer := ioutil.ConstructWriter(0, seqs, -1, false, false, fs)
		_, err := writer.WriteBatch(input)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		return writer.GetObjectStats()
	}
	old, current := write([]int64{1}, false), write([]int64{3, 4}, true)
	t.Run("remote ordered scan uses the statement schema", func(t *testing.T) {
		nonNullDef := plan2.DeepCopyTableDef(table, true)
		nonNullDef.Cols[1].Typ.NotNullable = true
		nonNullDef.Cols[1].Default.NullAbility = false
		data := NewBlockListRelationData(0)
		for _, stats := range []objectio.ObjectStats{write([]int64{3}, true), old} {
			blk := stats.ConstructBlockInfo(0)
			data.AppendBlockInfo(&blk)
		}
		source := NewRemoteDataSource(ctx, fs, timestamp.Timestamp{}, data)
		reader, err := NewReader(ctx, mp, nil, fs, nonNullDef, timestamp.Timestamp{}, nil, source, 0, engine.FilterHint{})
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()
		key := MakeColExprForTest(1, types.T_bool, "flag")
		key.Typ.NotNullable = true
		reader.SetOrderBy([]*plan.OrderBySpec{{Expr: key}})
		out := batch.NewWithSize(2)
		out.Vecs[0], out.Vecs[1] = vector.NewVec(idType), vector.NewVec(flagType)
		defer out.Clean(mp)
		end, err := reader.Read(ctx, []string{"id", "flag"}, nil, mp, out)
		require.NoError(t, err)
		require.False(t, end)
		require.Len(t, source.orderedBlocks, 2)
		require.Equal(t, false, source.orderedBlocks[0].zm.GetMin())
		require.Equal(t, false, source.orderedBlocks[0].zm.GetMax())
		require.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0]))
	})
	for i, obj := range []objectio.ObjectStats{old, current} {
		for _, late := range []bool{false, true} {
			t.Run(fmt.Sprintf("object%d/late=%t", i, late), func(t *testing.T) {
				src := &singlePersistedBlockSource{info: obj.ConstructBlockInfo(0)}
				r, err := NewReader(ctx, mp, nil, fs, table, timestamp.Timestamp{}, nil, src, 0, engine.FilterHint{})
				require.NoError(t, err)
				defer func() { require.NoError(t, r.Close()) }()
				out := batch.NewWithSize(2)
				defer out.Clean(mp)
				out.Vecs[0], out.Vecs[1] = vector.NewVec(idType), vector.NewVec(flagType)
				cols := []string{"id", "flag"}
				var end bool
				if late {
					end, err = r.ReadWithFilter(ctx, cols, []int{0}, &engine.ReaderFilterRequest{
						Apply: func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
							require.Equal(t, []int{0}, loaded)
							sels := []int64{0}
							if err := bat.Vecs[0].Shrink(sels, false, mp); err != nil {
								return engine.ReaderFilterResult{}, err
							}
							bat.SetRowCount(1)
							return engine.ReaderFilterResult{Sels: sels}, nil
						},
					}, mp, out)
				} else {
					end, err = r.Read(ctx, cols, nil, mp, out)
				}
				require.NoError(t, err)
				require.False(t, end)
				ids := []int64{1}
				if i == 1 {
					ids = []int64{3, 4}
				}
				if late {
					ids = ids[:1]
				}
				require.Equal(t, ids, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0]))
				for pos, id := range ids {
					require.Equal(t, id == 4, out.Vecs[1].IsNull(uint64(pos)))
					if id != 4 {
						require.Equal(t, id == 3, vector.GetFixedAtNoTypeCheck[bool](out.Vecs[1], pos))
					}
				}
			})
		}
	}

	oldLoc := old.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &oldLoc, false, fs)
	require.NoError(t, err)
	block := meta.MustDataMeta().GetBlockMeta(0)
	foldedBool := func(value bool) *plan.Expr {
		return &plan.Expr{Typ: flagType.PlanType(), Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
			IsConst: true, Data: types.EncodeBool(&value),
		}}}
	}
	for _, tc := range []struct {
		name           string
		hasNullDefault bool
		args           []*plan.Expr
		want           bool
	}{
		{"=", false, []*plan.Expr{foldedBool(false)}, true},
		{"=", false, []*plan.Expr{foldedBool(true)}, false},
		{"isnull", false, nil, false}, {"isnull", true, nil, true},
		{"isnotnull", false, nil, true}, {"isnotnull", true, nil, false},
	} {
		t.Run(fmt.Sprintf("%s/%v/null=%t", tc.name, tc.want, tc.hasNullDefault), func(t *testing.T) {
			def := plan2.DeepCopyTableDef(table, true)
			if tc.hasNullDefault {
				def.Cols[1].Default.OriginDefault = &plan.Literal{Isnull: true}
			}
			args := append([]*plan.Expr{MakeColExprForTest(1, types.T_bool, "flag")}, tc.args...)
			expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, tc.name, args)
			require.NoError(t, err)
			_, _, objFilter, blkFilter, _, compiled, _ := CompileFilterExpr(expr, def, fs)
			require.True(t, compiled)
			ok, err := objFilter(meta, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
			_, ok, err = blkFilter(0, block, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
		})
	}
}

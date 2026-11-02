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

package table_scan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBooleanFilterSelection(t *testing.T) {
	for _, rows := range []int{0, 1, 5, 255, 256, 257} {
		for _, shape := range []string{"true", "false", "mixed", "nullable", "allnull", "const_true", "const_false", "const_null"} {
			for _, late := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/%s/late=%t", rows, shape, late), func(t *testing.T) {
					proc := testutil.NewProc(t)
					boolType, intType, textType := types.T_bool.ToType(), types.T_int32.ToType(), types.T_text.ToType()
					predicate := lateTestCol(0, boolType)
					constant := shape == "const_true" || shape == "const_false" || shape == "const_null"
					if constant {
						predicate = &pb.Expr{Typ: pb.Type{Id: int32(types.T_bool)}, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Isnull: shape == "const_null", Value: &pb.Literal_Bval{Bval: shape == "const_true"}}}}
					}
					scan := &TableScan{Attrs: []string{"mask", "payload", "deferred"}, Types: []pb.Type{{Id: int32(boolType.Oid)}, {Id: int32(intType.Oid)}, {Id: int32(textType.Oid)}}, FilterExprs: []*pb.Expr{predicate}}
					t.Cleanup(func() { scan.Free(proc, false, nil) })
					require.NoError(t, scan.Prepare(proc))
					input := batch.NewWithSize(3)
					t.Cleanup(func() { input.Clean(proc.Mp()) })
					input.Vecs[0] = vector.NewVec(boolType)
					input.Vecs[1] = vector.NewVec(intType)
					input.Vecs[2] = vector.NewVec(textType)
					var want []int32
					var sels []int64
					for i := 0; i < rows; i++ {
						value := shape == "true" || shape == "const_true" || (shape == "mixed" || shape == "nullable") && i%3 != 1
						isNull := shape == "allnull" || shape == "nullable" && i%4 == 0
						require.NoError(t, vector.AppendFixed(input.Vecs[0], value, isNull, proc.Mp()))
						require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(i), false, proc.Mp()))
						if value && !isNull {
							want = append(want, int32(i))
							sels = append(sels, int64(i))
						}
					}
					input.SetRowCount(rows)
					var loaded []int
					if late {
						loaded = []int{0, 1}
					}
					result, err := scan.evalFilter(proc, input, loaded)
					require.NoError(t, err)
					require.Equal(t, len(want), input.RowCount())
					got := vector.MustFixedColNoTypeCheck[int32](input.Vecs[1])[:input.RowCount()]
					require.Len(t, got, len(want))
					if len(want) > 0 {
						require.Equal(t, want, got)
					}
					require.Zero(t, input.Vecs[2].Length())
					if late {
						if len(want) == rows {
							require.True(t, result.All)
							require.Empty(t, result.Sels)
						} else {
							require.False(t, result.All)
							if len(sels) == 0 {
								require.Empty(t, result.Sels)
							} else {
								require.Equal(t, sels, result.Sels)
							}
						}
					}
				})
			}
		}
	}
}

func TestBooleanFilterRepresentationReuse(t *testing.T) {
	proc := testutil.NewProc(t)
	boolType := types.T_bool.ToType()
	scan := &TableScan{Attrs: []string{"mask"}, Types: []pb.Type{{Id: int32(types.T_bool)}}, FilterExprs: []*pb.Expr{lateTestCol(0, boolType)}}
	t.Cleanup(func() { scan.Free(proc, false, nil) })
	require.NoError(t, scan.Prepare(proc))
	for _, shape := range []string{"const_true", "flat", "const_null", "nullable", "const_false", "const_true"} {
		func() {
			input := batch.NewWithSize(1)
			defer input.Clean(proc.Mp())
			want := 0
			switch shape {
			case "const_true", "const_false":
				v, err := vector.NewConstFixed(boolType, shape == "const_true", 5, proc.Mp())
				require.NoError(t, err)
				input.Vecs[0] = v
				if shape == "const_true" {
					want = 5
				}
			case "const_null":
				input.Vecs[0] = vector.NewConstNull(boolType, 5, proc.Mp())
			default:
				input.Vecs[0] = vector.NewVec(boolType)
				for i := range 5 {
					null := shape == "nullable" && i == 0
					require.NoError(t, vector.AppendFixed(input.Vecs[0], i%2 == 0, null, proc.Mp()))
					if i%2 == 0 && !null {
						want++
					}
				}
			}
			input.SetRowCount(5)
			_, err := scan.evalFilter(proc, input, nil)
			require.NoError(t, err, shape)
			require.Equal(t, want, input.RowCount(), shape)
		}()
	}
}

func TestBooleanFilterErrorOrder(t *testing.T) {
	for _, first := range []bool{false, true} {
		t.Run(fmt.Sprint(first), func(t *testing.T) {
			proc := testutil.NewProc(t)
			scan := &TableScan{Attrs: []string{"mask", "invalid"}, Types: []pb.Type{{Id: int32(types.T_bool)}, {Id: int32(types.T_int32)}}, FilterExprs: []*pb.Expr{lateTestCol(0, types.T_bool.ToType()), lateTestCol(1, types.T_int32.ToType())}}
			t.Cleanup(func() { scan.Free(proc, false, nil) })
			require.NoError(t, scan.Prepare(proc))
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
			input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
			require.NoError(t, vector.AppendFixed(input.Vecs[0], first, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(7), false, proc.Mp()))
			input.SetRowCount(1)
			_, err := scan.evalFilter(proc, input, nil)
			if first {
				require.ErrorContains(t, err, "filter condition is not boolean")
			} else {
				require.NoError(t, err)
				require.Zero(t, input.RowCount())
			}
		})
	}
}

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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestAnyNotEqualZoneMap(t *testing.T) {
	key := makeInternalVarcharZoneMap("key")
	keep := makeInternalVarcharZoneMap("keep")

	res, ok := anyNotEqualZoneMap(key, keep)
	require.True(t, ok)
	require.True(t, res)

	res, ok = anyNotEqualZoneMap(key, key)
	require.True(t, ok)
	require.False(t, res)

	res, ok = anyNotEqualZoneMap(makeInternalVarcharZoneMap("key", "keep"), key)
	require.True(t, ok)
	require.True(t, res)

	_, ok = anyNotEqualZoneMap(key, objectio.NewZM(types.T_varchar, 0))
	require.False(t, ok)
}

func TestFoldBooleanZoneMapUnknownPaths(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	trueExpr := makeInternalBoolLitExpr(true, 0)
	falseExpr := makeInternalBoolLitExpr(false, 1)
	unknownExpr := &plan.Expr{
		Typ:   plan.Type{Id: int32(types.T_bool)},
		AuxId: 2,
	}

	zms := make([]objectio.ZoneMap, 4)
	vecs := make([]*vector.Vector, 4)
	require.False(t, foldAndZoneMap(ctx, proc, nil, nil, zms, vecs, []*plan.Expr{trueExpr, unknownExpr}, 3))
	require.False(t, zms[3].IsInited())

	zms = make([]objectio.ZoneMap, 4)
	vecs = make([]*vector.Vector, 4)
	require.False(t, foldOrZoneMap(ctx, proc, nil, nil, zms, vecs, []*plan.Expr{falseExpr, unknownExpr}, 3))
	require.False(t, zms[3].IsInited())

	zms = make([]objectio.ZoneMap, 4)
	vecs = make([]*vector.Vector, 4)
	require.True(t, foldOrZoneMap(ctx, proc, nil, nil, zms, vecs, []*plan.Expr{falseExpr}, 3))
	require.True(t, zms[3].IsInited())
	require.False(t, types.DecodeBool(zms[3].GetMaxBuf()))
}

func makeInternalBoolLitExpr(value bool, auxID int32) *plan.Expr {
	return &plan.Expr{
		Typ:   plan.Type{Id: int32(types.T_bool)},
		AuxId: auxID,
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: value},
			},
		},
	}
}

func makeInternalVarcharZoneMap(values ...string) objectio.ZoneMap {
	zm := index.NewZM(types.T_varchar, 0)
	for _, value := range values {
		index.UpdateZM(zm, []byte(value))
	}
	return zm
}

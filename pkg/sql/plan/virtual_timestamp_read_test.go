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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestVirtualTimestampReadAndIndexEligibility(t *testing.T) {
	ref := func(pos int32, typ types.T) *pb.Expr {
		return &pb.Expr{Typ: pb.Type{Id: int32(typ)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos}}}
	}
	table := &pb.TableDef{
		Cols: []*pb.ColDef{
			{Name: "ts", Typ: pb.Type{Id: int32(types.T_timestamp)}},
			{Name: "d", Typ: pb.Type{Id: int32(types.T_date)}, GeneratedCol: &pb.GeneratedCol{Expr: ref(0, types.T_date)}},
			{Name: "child", Typ: pb.Type{Id: int32(types.T_date)}, GeneratedCol: &pb.GeneratedCol{Expr: ref(1, types.T_date)}},
			{Name: "stored", Typ: pb.Type{Id: int32(types.T_date)}, GeneratedCol: &pb.GeneratedCol{Expr: ref(0, types.T_date), IsStored: true}},
			{Name: "stable", Typ: pb.Type{Id: int32(types.T_date)}, GeneratedCol: &pb.GeneratedCol{Expr: ref(3, types.T_date)}},
		},
		Name2ColIndex: map[string]int32{"ts": 0, "d": 1, "child": 2, "stored": 3, "stable": 4},
	}
	require.True(t, virtualTimestampColumn(table, 1, make(map[int32]bool)))
	require.True(t, virtualTimestampColumn(table, 2, make(map[int32]bool)))
	require.False(t, virtualTimestampColumn(table, 3, make(map[int32]bool)))
	require.False(t, virtualTimestampColumn(table, 4, make(map[int32]bool)))
	require.True(t, indexUsesVirtualTimestamp(&pb.IndexDef{Parts: []string{"stored", "child"}}, table))
	require.False(t, indexUsesVirtualTimestamp(&pb.IndexDef{Parts: []string{"stable"}}, table))

	bound := expandVirtualTimestampColumn(table, 2, 17, 0, make(map[int32]bool))
	require.NotNil(t, bound)
	require.Equal(t, int32(17), bound.GetCol().RelPos)
	require.Equal(t, int32(0), bound.GetCol().ColPos)
	correlated := expandVirtualTimestampColumn(table, 2, 17, 1, make(map[int32]bool))
	require.NotNil(t, correlated)
	require.Equal(t, int32(17), correlated.GetCorr().RelPos)
	require.Equal(t, int32(0), correlated.GetCorr().ColPos)
	require.Equal(t, int32(1), correlated.GetCorr().Depth)
}

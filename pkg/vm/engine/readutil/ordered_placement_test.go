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

package readutil

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestOrderHeadNullsLeadPlacement pins the shared effective-NULL-placement
// helper, including the both-bits combination: NULLS FIRST is consulted
// first and wins over NULLS LAST, mirroring the Order comparator, and the
// default trails NULLs only under DESC.
func TestOrderHeadNullsLeadPlacement(t *testing.T) {
	require.True(t, OrderHeadNullsLead(plan.OrderBySpec_ASC), "ASC default leads with NULLs")
	require.False(t, OrderHeadNullsLead(plan.OrderBySpec_DESC), "DESC default trails NULLs")
	require.True(t, OrderHeadNullsLead(plan.OrderBySpec_NULLS_FIRST))
	require.True(t, OrderHeadNullsLead(plan.OrderBySpec_NULLS_FIRST|plan.OrderBySpec_DESC))
	require.False(t, OrderHeadNullsLead(plan.OrderBySpec_NULLS_LAST))
	require.False(t, OrderHeadNullsLead(plan.OrderBySpec_NULLS_LAST|plan.OrderBySpec_DESC))
	require.True(t, OrderHeadNullsLead(plan.OrderBySpec_NULLS_FIRST|plan.OrderBySpec_NULLS_LAST),
		"NULLS FIRST wins when both bits are present")
	require.True(t, OrderHeadNullsLead(plan.OrderBySpec_NULLS_FIRST|plan.OrderBySpec_NULLS_LAST|plan.OrderBySpec_DESC),
		"both bits with DESC still resolve as NULLS FIRST")
	require.True(t, OrderHeadNullsLead(plan.OrderBySpec_UNIQUE), "irrelevant bits do not change placement")
}

// TestRemoteDataSourceKeepsNullableOrderByGate pins the retained remote
// fallback of the #32469 change map: RemoteDataSource.SetOrderBy drops
// single-key nullable orders, remote scans stay exact-but-unpruned, and
// (because SetFilterZM never stores an order) nullable plans cannot consume
// a bound remotely until the prior approved design's multi-CN validation
// matrix exists.
func TestRemoteDataSourceKeepsNullableOrderByGate(t *testing.T) {
	nullableKey := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k"}},
	}
	nnKey := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k"}},
	}
	rs := &RemoteDataSource{}
	rs.SetOrderBy([]*plan.OrderBySpec{{Flag: plan.OrderBySpec_ASC, Expr: nnKey}})
	require.Len(t, rs.GetOrderBy(), 1, "NOT NULL single-key orders are still admitted")

	rs.SetOrderBy([]*plan.OrderBySpec{{Flag: plan.OrderBySpec_ASC, Expr: nullableKey}})
	require.Empty(t, rs.GetOrderBy(), "nullable single-key orders stay excluded remotely")
}

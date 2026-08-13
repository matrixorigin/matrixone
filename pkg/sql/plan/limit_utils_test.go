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
	"math"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBuildCandidateLimit(t *testing.T) {
	tests := []struct {
		name       string
		limit      *planpb.Expr
		offset     *planpb.Expr
		want       uint64
		wantUsable bool
	}{
		{name: "limit only", limit: makePlan2Uint64ConstExprWithType(10), want: 10, wantUsable: true},
		{name: "limit plus offset", limit: makePlan2Uint64ConstExprWithType(10), offset: makePlan2Uint64ConstExprWithType(5), want: 15, wantUsable: true},
		{name: "zero offset", limit: makePlan2Uint64ConstExprWithType(10), offset: makePlan2Uint64ConstExprWithType(0), want: 10, wantUsable: true},
		{name: "overflow", limit: makePlan2Uint64ConstExprWithType(math.MaxUint64), offset: makePlan2Uint64ConstExprWithType(1)},
		{name: "dynamic offset", limit: makePlan2Uint64ConstExprWithType(10), offset: &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}},
		{name: "dynamic limit without offset", limit: &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}, wantUsable: true},
		{name: "null literal offset", limit: makePlan2Uint64ConstExprWithType(10), offset: &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true, Value: &planpb.Literal_U64Val{U64Val: 5}}}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, usable := buildCandidateLimit(tc.limit, tc.offset)
			require.Equal(t, tc.wantUsable, usable)
			if !usable {
				require.Nil(t, got)
				return
			}
			if value, literal := getLiteralUint64(got); literal {
				require.Equal(t, tc.want, value)
			} else {
				require.NotSame(t, tc.limit, got)
				require.NotNil(t, got.GetP())
			}
		})
	}
}

func TestShouldPushFulltextCandidateLimit(t *testing.T) {
	tests := []struct {
		name              string
		fulltextStreams   int
		residualFilters   int
		prefilterPushdown bool
		exactPrefilter    bool
		want              bool
	}{
		{name: "single stream without residual filter", fulltextStreams: 1, want: true},
		{name: "single stream with exact pushed residual filter", fulltextStreams: 1, residualFilters: 1, prefilterPushdown: true, exactPrefilter: true, want: true},
		{name: "single stream with approximate pushed residual filter", fulltextStreams: 1, residualFilters: 1, prefilterPushdown: true, want: false},
		{name: "single stream with unpushed residual filter", fulltextStreams: 1, residualFilters: 1, want: false},
		{name: "multiple streams without residual filter", fulltextStreams: 2, want: false},
		{name: "multiple streams with exact pushed residual filter", fulltextStreams: 2, residualFilters: 1, prefilterPushdown: true, exactPrefilter: true, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldPushFulltextCandidateLimit(tc.fulltextStreams, tc.residualFilters, tc.prefilterPushdown, tc.exactPrefilter))
		})
	}
}

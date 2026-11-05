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
	"github.com/stretchr/testify/require"
	"testing"
)

func localFenceTestFunction(id, overload int32, args ...*Expr) *Expr {
	return &Expr{Expr: &Expr_F{F: &Function{Func: &ObjectRef{Obj: int64(id)<<32 | int64(overload)}, Args: args}}}
}

func TestLocalSourceFencePrepassUsesCompleteOwner(t *testing.T) {
	trigger := localFenceTestFunction(237, 0)
	jsonArg := &Expr{Typ: Type{Id: planJSONTypeID}, Expr: &Expr_Col{Col: &ColRef{}}}
	min := localFenceTestFunction(139, 0, jsonArg)
	min.GetF().Func.Obj = int64(uint64(min.GetF().Func.Obj) | uint64(1)<<63)
	for _, owner := range []any{
		nil, &Plan{}, &Query{Nodes: []*Node{{FilterList: []*Expr{localFenceTestFunction(8, 0)}}}},
		trigger, min, localFenceTestFunction(384, 1),
		&TableDef{Cols: []*ColDef{{Default: &Default{Expr: trigger}}}},
		&TableDef{Cols: []*ColDef{{OnUpdate: &OnUpdate{Expr: trigger}}}},
		&Query{Nodes: []*Node{{PreInsertCtx: &PreInsertCtx{TrackOdkuResult: true}}}},
		&DedupJoinCtx{OdkuOrdinalCol: &ColRef{}, OdkuGeneratedCol: &ColRef{}},
	} {
		full, err := RequiredRemoteExpressionFeatures(owner)
		require.NoError(t, err)
		may, err := MayRequireLocalSourceFence(owner)
		require.NoError(t, err)
		require.Equal(t, full.RequiresLocalSourceFence(), may)
	}
}

func TestLocalSourceFencePrepassPreservesCollectionErrors(t *testing.T) {
	bad := []*Expr{
		localFenceTestFunction(202, 1),
		localFenceTestFunction(21, 5),
		localFenceTestFunction(262, 0, nil, &Expr{Typ: Type{Id: 23}}),
	}
	for _, expr := range bad {
		for _, triggerFirst := range []bool{false, true} {
			list := []*Expr{expr}
			if triggerFirst {
				list = append([]*Expr{localFenceTestFunction(237, 0)}, list...)
			}
			owner := &Query{Nodes: []*Node{{ProjectList: list}}}
			_, fullErr := RequiredRemoteExpressionFeatures(owner)
			require.Error(t, fullErr)
			_, fastErr := MayRequireLocalSourceFence(owner)
			require.IsType(t, fullErr, fastErr)
			require.EqualError(t, fastErr, fullErr.Error())
		}
	}
	// Preserve the first malformed expression, even if another family follows.
	owner := &Node{FilterList: bad}
	_, fullErr := RequiredRemoteExpressionFeatures(owner)
	_, fastErr := MayRequireLocalSourceFence(owner)
	require.EqualError(t, fastErr, fullErr.Error())
}

type localFenceGetterOwner struct {
	Calls *int
	Expr  *Expr
}

func (owner *localFenceGetterOwner) GetTrackOdkuResult() bool { *owner.Calls++; return true }
func TestLocalSourceFencePrepassDoesNotInvokeMetadataGetters(t *testing.T) {
	calls := 0
	owner := &localFenceGetterOwner{Calls: &calls, Expr: localFenceTestFunction(21, 5)}
	may, err := MayRequireLocalSourceFence(owner)
	require.NoError(t, err)
	require.True(t, may)
	require.Zero(t, calls)
	_, err = RequiredRemoteExpressionFeatures(owner)
	require.ErrorContains(t, err, "CAST arity")
	require.Equal(t, 1, calls)
}

func BenchmarkLocalSourceFenceAnalysis(b *testing.B) {
	negative := &Query{Nodes: []*Node{{FilterList: []*Expr{localFenceTestFunction(8, 0, &Expr{Typ: Type{Id: 52}, Expr: &Expr_Col{Col: &ColRef{}}}, localFenceTestFunction(89, 0))}}}}
	positive := &Query{Nodes: []*Node{{ProjectList: []*Expr{localFenceTestFunction(237, 0)}}}}
	for _, input := range []struct {
		name  string
		owner *Query
	}{{"negative", negative}, {"positive", positive}} {
		for _, full := range []bool{false, true} {
			name := "prepass_and_fallback"
			if full {
				name = "complete"
			}
			b.Run(input.name+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if full {
						_, _ = RequiredRemoteExpressionFeatures(input.owner)
					} else if may, err := MayRequireLocalSourceFence(input.owner); err == nil && may {
						_, _ = RequiredRemoteExpressionFeatures(input.owner)
					}
				}
			})
		}
	}
}

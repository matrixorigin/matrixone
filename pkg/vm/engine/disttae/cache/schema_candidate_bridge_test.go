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

//go:build catalog_candidate_probe

package cache

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCandidateRealAlterIsolation(t *testing.T) {
	item := &TableItem{Name: "t"}
	for _, name := range []string{"a", "b"} {
		item.Defs = append(item.Defs, &engine.AttributeDef{Attr: engine.Attribute{Name: name, Type: types.T_int64.ToType(), Default: &pb.Default{NullAbility: true}}})
	}
	item.Defs = append(item.Defs, &engine.AttributeDef{Attr: engine.Attribute{Name: "c", Type: types.T_varchar.ToType(), Default: &pb.Default{OriginString: "complex", XXX_unrecognized: []byte{0xa0, 0x06, 0x07}}}})
	candidate := newCandidateSchema(item)
	old := newCandidateSchema(item)
	require.Same(t, candidate.planner.Cols[0].Default, candidate.planner.Cols[1].Default)
	otherRelation := candidate.planner
	altered, err := plan2.CandidateAlterProbe(candidate.planner, "a")
	require.NoError(t, err)
	require.False(t, altered.Cols[0].Default.NullAbility)
	require.True(t, altered.Cols[1].Default.NullAbility)
	require.NotSame(t, altered.Cols[0].Default, altered.Cols[1].Default)
	require.True(t, candidate.planner.Cols[0].Default.NullAbility)
	require.True(t, otherRelation.Cols[1].Default.NullAbility)
	require.True(t, old.planner.Cols[0].Default.NullAbility)
	require.Equal(t, "complex", candidate.planner.Cols[2].Default.OriginString)
	require.Equal(t, []byte{0xa0, 0x06, 0x07}, candidate.planner.Cols[2].Default.XXX_unrecognized)
	altered.Cols[1].Default.NullAbility = false
	altered.Cols[2].Default.OriginString = "changed"
	require.True(t, otherRelation.Cols[1].Default.NullAbility)
	require.Equal(t, "complex", otherRelation.Cols[2].Default.OriginString)
	require.Equal(t, "complex", old.planner.Cols[2].Default.OriginString)
	require.Equal(t, []byte{0xa0, 0x06, 0x07}, old.planner.Cols[2].Default.XXX_unrecognized)
}

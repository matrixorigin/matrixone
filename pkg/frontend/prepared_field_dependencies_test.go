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

package frontend

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldDependenciesChanged(t *testing.T) {
	previous := &plan.ObjectRef{Db: 1, Obj: 2, Server: 3, SchemaName: "d", ObjName: "t"}
	current := proto.Clone(previous).(*plan.ObjectRef)
	require.NotSame(t, previous, current)
	other := &plan.ObjectRef{Db: 1, Obj: 4, Server: 5, SchemaName: "d", ObjName: "other"}
	require.False(t, preparedFieldDependenciesChanged(
		[]*plan.ObjectRef{previous, other}, []*plan.ObjectRef{other, current}), "optimizer order is not a type change")
	require.True(t, preparedFieldDependenciesChanged(
		[]*plan.ObjectRef{previous, previous}, []*plan.ObjectRef{current, other}), "one dependency cannot satisfy two entries")
	require.False(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{previous}, []*plan.ObjectRef{current}))
	require.False(t, preparedFieldDependenciesChanged(nil, []*plan.ObjectRef{}))
	require.False(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{nil}, []*plan.ObjectRef{nil}))
	require.True(t, preparedFieldDependenciesChanged(nil, []*plan.ObjectRef{current}))
	require.True(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{previous}, nil))
	require.True(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{previous}, []*plan.ObjectRef{nil}))
	for _, tc := range []struct {
		name   string
		mutate func(*plan.ObjectRef)
	}{
		{"table replacement or temporary shadow", func(r *plan.ObjectRef) { r.Obj++ }},
		{"schema version", func(r *plan.ObjectRef) { r.Server++ }},
		{"database replacement", func(r *plan.ObjectRef) { r.Db++ }},
		{"logical name resolution", func(r *plan.ObjectRef) { r.ObjName = "other" }},
		{"subscription", func(r *plan.ObjectRef) { r.SubscriptionName = "sub" }},
		{"snapshot", func(r *plan.ObjectRef) { r.Snapshot = &plan.Snapshot{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := proto.Clone(previous).(*plan.ObjectRef)
			tc.mutate(next)
			require.True(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{previous}, []*plan.ObjectRef{next}))
			require.Equal(t, int64(2), previous.Obj, "published dependency must stay immutable")
		})
	}
}

// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWaterliner(t *testing.T) {
	table := mockTable(1, 2, 3)
	id := TableID(table.String())
	want := mockTimestamp(100, 101)
	waterline := want.Next()

	w := NewWaterliner()
	// table not registered
	_, ok := w.Waterline(id)
	require.False(t, ok)

	/* ---- 1. register table with waterline ---- */
	w.Register(id, table, waterline)
	line, ok := w.Waterline(id)
	require.True(t, ok)
	require.Equal(t, waterline, line)
	// only one table subcribed
	require.Equal(t, 1, len(w.ListSubscribedTable()))

	/* ---- 2. register table repeatedly ---- */
	w.Register(id, table, want)
	// table registerd, waterline not updated
	line, ok = w.Waterline(id)
	require.True(t, ok)
	require.Equal(t, waterline, line)
	// there's only one table subcribed
	require.Equal(t, 1, len(w.ListSubscribedTable()))

	/* ---- 3. unregister table ---- */
	w.Unregister(id)
	// there should be still one table subscribed
	require.Equal(t, 1, len(w.ListSubscribedTable()))

	/* ---- 4. unregister table again ---- */
	w.Unregister(id)
	// there should be still one table subscribed
	require.Equal(t, 0, len(w.ListSubscribedTable()))

	/* ---- 5. register table, then promote waterline ---- */
	w.Register(id, table, waterline)
	newWaterline := waterline.Next()
	w.Advance(newWaterline)
	// check the promoted waterline
	line, ok = w.Waterline(id)
	require.True(t, ok)
	require.Equal(t, newWaterline, line)

	/* ---- 6. unregister non-exist table ---- */
	w.Unregister("non-exist")
	// there's no impact on the registered table
	require.Equal(t, 1, len(w.ListSubscribedTable()))
}

func TestTableInfo(t *testing.T) {
	table := mockTable(1, 2, 3)
	ts := mockTimestamp(10, 11)
	id := TableID(table.String())

	info := newTableInfo(id, table, ts)

	cases := []struct {
		op     func() int32
		expect int32
	}{
		{
			op:     info.Ref,
			expect: 1,
		},
		{
			op:     info.Ref,
			expect: 2,
		},
		{
			op:     info.Ref,
			expect: 3,
		},
		{
			op:     info.Deref,
			expect: 2,
		},
		{
			op:     info.Deref,
			expect: 1,
		},
		{
			op:     info.Deref,
			expect: 0,
		},
	}

	for _, c := range cases {
		require.Equal(t, c.op(), c.expect)
	}
}

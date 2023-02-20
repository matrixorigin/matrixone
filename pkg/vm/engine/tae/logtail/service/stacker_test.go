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

func TestTableStacker(t *testing.T) {
	s := NewTableStacker()
	require.Equal(t, 0, len(s.ListTable()))

	table := mockTable(1, 2, 3)
	id := TableID(table.String())

	/* ---- 1. register table ---- */
	s.Register(id, table)
	// only one table
	require.Equal(t, 1, len(s.ListTable()))

	/* ---- 2. register table repeatedly ---- */
	s.Register(id, table)
	// there's only one table subcribed
	require.Equal(t, 1, len(s.ListTable()))

	/* ---- 3. unregister table ---- */
	s.Unregister(id)
	// there should be still one table subscribed
	require.Equal(t, 1, len(s.ListTable()))

	/* ---- 4. unregister table again ---- */
	s.Unregister(id)
	// there should be no table subscribed
	require.Equal(t, 0, len(s.ListTable()))
}

func TestTableInfo(t *testing.T) {
	table := mockTable(1, 2, 3)
	id := TableID(table.String())

	info := newTableInfo(id, table)

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

// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreCreate(t *testing.T) {
	dsn := "dump:111@tcp(127.0.0.1:6001)/zx"
	s, err := NewSQLStore(dsn)
	require.NoError(t, err)
	require.NoError(t, s.Create(context.Background(), 1, []AutoColumn{
		{
			TableID:  1,
			ColName:  "col1",
			ColIndex: 0,
			Offset:   0,
			Step:     1,
		},
	}))
}

// func TestStoreAlloc(t *testing.T) {
// 	dsn := "dump:111@tcp(127.0.0.1:6001)/zx"
// 	s, err := NewSQLStore(dsn)
// 	require.NoError(t, err)

// 	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, 9000001)
// 	from, to, err := s.Alloc(ctx, "k1", 1)
// 	require.NoError(t, err)
// 	assert.Equal(t, uint64(101), from)
// 	assert.Equal(t, uint64(102), to)
// }

func TestGetNext(t *testing.T) {
	v := uint64(9223372036854776807)
	t.Fatalf("%d", v+uint64(1000))
}

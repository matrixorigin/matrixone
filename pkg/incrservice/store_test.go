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

// func TestStoreCreate(t *testing.T) {
// 	dsn := "de2938e4-a154-4c6f-b010-f0c2c17c15ee:1bed630d-60a6-4830-9e1e-4dd669d2fbea@tcp(127.0.0.1:6001)/mo_catalog"
// 	s, err := NewSQLStore(dsn)
// 	require.NoError(t, err)
// 	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, 9000001)
// 	require.NoError(t, s.Create(ctx, "k1", 100, 1))
// }

// func TestStoreAlloc(t *testing.T) {
// 	dsn := "de2938e4-a154-4c6f-b010-f0c2c17c15ee:1bed630d-60a6-4830-9e1e-4dd669d2fbea@tcp(127.0.0.1:6001)/mo_catalog"
// 	s, err := NewSQLStore(dsn)
// 	require.NoError(t, err)

// 	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, 9000001)
// 	from, to, err := s.Alloc(ctx, "k1", 1)
// 	require.NoError(t, err)
// 	assert.Equal(t, uint64(101), from)
// 	assert.Equal(t, uint64(102), to)
// }

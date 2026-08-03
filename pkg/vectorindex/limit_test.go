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

package vectorindex

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClampSearchLimit(t *testing.T) {
	require.Equal(t, uint64(5), ClampSearchLimit(10, 5, math.MaxUint32))
	require.Equal(t, uint64(10), ClampSearchLimit(10, 20, math.MaxUint32))
	if uint64(^uint(0)) > math.MaxUint32 {
		require.Equal(t, uint64(math.MaxUint32), ClampSearchLimit(^uint(0), math.MaxUint64, math.MaxUint32))
	}
	require.Equal(t, uint64(math.MaxUint64), SaturatingAddUint64(math.MaxUint64-1, 2))
	require.Equal(t, 1<<20, SearchResultPreallocate(math.MaxUint64))
	require.Equal(t, 10, SearchResultPreallocate(10))
}

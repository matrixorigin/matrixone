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

package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFixedZoneOffset(t *testing.T) {
	for _, offset := range []int{-14 * 3600, -61, -60, 0, 61, 14 * 3600} {
		loc := time.FixedZone("custom", offset)
		got, ok := FixedZoneOffset(loc)
		require.True(t, ok)
		require.Equal(t, offset, got)
	}
	got, ok := FixedZoneOffset(time.UTC)
	require.True(t, ok)
	require.Zero(t, got)
	_, ok = FixedZoneOffset(nil)
	require.False(t, ok)
	for _, name := range []string{"America/New_York", "Antarctica/Troll", "Pacific/Apia"} {
		loc, err := time.LoadLocation(name)
		require.NoError(t, err)
		_, ok := FixedZoneOffset(loc)
		require.False(t, ok, name)
	}
}

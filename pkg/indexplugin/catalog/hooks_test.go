// Copyright 2026 Matrix Origin
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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlterTableCloneBehavior_ZeroValue(t *testing.T) {
	// The zero value documents "no special behaviour" — both lookup
	// methods must return false for every key, including the empty
	// string.
	var b AlterTableCloneBehavior
	require.False(t, b.ContainsDelete(""))
	require.False(t, b.ContainsDelete("metadata"))
	require.False(t, b.ContainsSkipWhenAsync(""))
	require.False(t, b.ContainsSkipWhenAsync("entries"))
}

func TestAlterTableCloneBehavior_ContainsDelete(t *testing.T) {
	b := AlterTableCloneBehavior{
		DeleteBeforeClone: []string{"metadata", "centroids", "entries"},
	}
	require.True(t, b.ContainsDelete("metadata"))
	require.True(t, b.ContainsDelete("centroids"))
	require.True(t, b.ContainsDelete("entries"))
	require.False(t, b.ContainsDelete("unknown"))
	require.False(t, b.ContainsDelete(""))
}

func TestAlterTableCloneBehavior_ContainsSkipWhenAsync(t *testing.T) {
	b := AlterTableCloneBehavior{
		SkipWhenAsync: []string{"entries"},
	}
	require.True(t, b.ContainsSkipWhenAsync("entries"))
	require.False(t, b.ContainsSkipWhenAsync("metadata"))
	require.False(t, b.ContainsSkipWhenAsync(""))
}

func TestAlterTableCloneBehavior_IndependentFields(t *testing.T) {
	// A non-empty DeleteBeforeClone must not bleed into
	// ContainsSkipWhenAsync, and vice versa. Confirms the two
	// fields are checked in isolation.
	b := AlterTableCloneBehavior{
		DeleteBeforeClone: []string{"metadata"},
		SkipWhenAsync:     []string{"entries"},
	}
	require.True(t, b.ContainsDelete("metadata"))
	require.False(t, b.ContainsDelete("entries"))
	require.True(t, b.ContainsSkipWhenAsync("entries"))
	require.False(t, b.ContainsSkipWhenAsync("metadata"))
}

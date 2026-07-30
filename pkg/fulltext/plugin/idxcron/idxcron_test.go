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

package idxcron

import (
	"testing"

	"github.com/stretchr/testify/require"

	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
)

// TestFulltextUpdatable_AlwaysOK: fulltext doesn't participate in
// scheduled rebuilds today; the hook is unreachable but must satisfy
// the interface. Trivial-true ensures any future wiring doesn't
// surprise-skip.
func TestFulltextUpdatable_AlwaysOK(t *testing.T) {
	ok, reason, err := Hooks{}.Updatable(idxcronplugin.UpdatableInput{})
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, reason)
}

// TestFulltextUpdatable_SatisfiesInterface: compile-time interface check.
func TestFulltextUpdatable_SatisfiesInterface(t *testing.T) {
	var _ idxcronplugin.Hooks = Hooks{}
}

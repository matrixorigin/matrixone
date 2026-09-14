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

package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The empty string is a real relkind -- a hidden index table carries it -- so "carried" and
// "not carried" cannot be distinguished by the value alone. An option that reported them the
// same way would let an ALTER promote a hidden table to an ordinary one.
func TestKeepRelKindDistinguishesEmptyFromUnset(t *testing.T) {
	_, ok := StatementOption{}.KeepRelKind()
	require.False(t, ok, "unset")

	kind, ok := StatementOption{}.WithKeepRelKind("").KeepRelKind()
	require.True(t, ok, "an explicitly carried empty kind is set")
	require.Equal(t, "", kind)

	kind, ok = StatementOption{}.WithKeepRelKind("hnsw_meta").KeepRelKind()
	require.True(t, ok)
	require.Equal(t, "hnsw_meta", kind)
}

// The option is value-semantic like its siblings: deriving one must not mutate the original.
func TestKeepRelKindIsValueSemantic(t *testing.T) {
	base := StatementOption{}
	derived := base.WithKeepRelKind("cagra_meta")

	_, ok := base.KeepRelKind()
	require.False(t, ok, "the receiver is unchanged")

	kind, ok := derived.KeepRelKind()
	require.True(t, ok)
	require.Equal(t, "cagra_meta", kind)

	// and it composes with the option it mirrors
	both := derived.WithKeepLogicalId(42)
	require.Equal(t, uint64(42), both.KeepLogicalId())
	kind, ok = both.KeepRelKind()
	require.True(t, ok)
	require.Equal(t, "cagra_meta", kind)
}

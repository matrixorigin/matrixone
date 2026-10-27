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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestBroadcastSpillAdmissionGateCanRecoverNextAttempt(t *testing.T) {
	join := &hashjoin.HashJoin{JoinMapTag: 41, SpillScopeID: 1, SpillExpectedScopes: []uint32{1}}
	build := &hashbuild.HashBuild{JoinMapTag: 41, CanSpill: true, SpillExpectedScopes: []uint32{1}}
	compiler := &Compile{addr: "local", scopes: []*Scope{{RootOp: join}, {RootOp: build}}}
	compiler.beginSpillEpochs()
	require.Empty(t, join.SpillExpectedScopes)
	require.Empty(t, build.SpillExpectedScopes)
	require.True(t, build.CanSpill, "legacy compatibility applies to this attempt, not permanent plan eligibility")
	compiler.importMemory = &process.ImportMemory{}
	compiler.beginSpillEpochs()
	require.Equal(t, []uint32{1}, join.SpillExpectedScopes)
	require.Equal(t, join.SpillExpectedScopes, build.SpillExpectedScopes)
}

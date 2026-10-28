// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestDataBranchFencingRetireSubstrateFromRuntimeDegradeNoPanic pins the
// D168(B)/D176 degrade contract at the producer: a context that cannot
// assemble a full CN substrate (mock-driven procs without a lock service,
// or a runtime without the internal SQL executor) must surface the plain
// nil degrade, never a nil-receiver method-chain panic. Regression: the
// unguarded constructor chain SIGSEGVed mock-proc drop lifecycles
// (TestDropDatabaseRelationsStopsAfterMemberFailure covers the paired
// drop path end to end).
func TestDataBranchFencingRetireSubstrateFromRuntimeDegradeNoPanic(t *testing.T) {
	require.Nil(t, DataBranchFencingRetireSubstrateFromRuntime(nil, nil))

	// A bare process has no txn client, no lock service and no runtime
	// executor, so the constructor degrades to nil; the generation-key
	// chain must not run on that nil.
	require.Nil(t, DataBranchFencingRetireSubstrateFromRuntime(testutil.NewProcess(t), nil))
}

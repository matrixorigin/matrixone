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

package process

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The decimal boundary registry's guarded miss paths keep every
// registry-consuming channel byte-identical on statements that never
// entered a planning generation (issue #30938, design
// docs/design/20261029-decimal-multiplication-storage-boundary.md,
// revision 41, decision log entries 49-58): recording, querying, barrier
// membership, CTAS marking and claiming must all answer the empty result
// on a registry-free process instead of panicking or inventing state.
func TestDecimalBoundaryRegistryGuardedMissesIssue30938(t *testing.T) {
	t.Run("record without a generation is a no-op", func(t *testing.T) {
		proc := &Process{}
		proc.RecordDecimalBoundaryDecision("k", "v", "sig", true)
		_, ok := proc.DecimalBoundaryDecision("k")
		require.False(t, ok, "no ledger exists without an entered generation")
	})

	t.Run("decision query without a generation misses", func(t *testing.T) {
		proc := &Process{}
		entry, ok := proc.DecimalBoundaryDecision("k")
		require.False(t, ok)
		require.Nil(t, entry)
	})

	t.Run("barrier membership without a generation misses", func(t *testing.T) {
		proc := &Process{}
		require.False(t, proc.DecimalBoundaryBarrierContains("sig"))
		require.False(t, proc.DecimalBoundaryBarrierActive())
	})

	t.Run("prepared-CTAS mark without a generation is a no-op", func(t *testing.T) {
		proc := &Process{}
		proc.MarkDecimalBoundaryPreparedCTAS()
		release := EnterDecimalBoundaryGeneration(proc)
		_, ok := proc.ClaimDecimalBoundaryEvidence()
		release()
		require.False(t, ok, "a mark made outside the generation never reaches the claim slot")
	})

	t.Run("generation nonce reads through a nil and a live receiver", func(t *testing.T) {
		var nilProc *Process
		require.Zero(t, nilProc.DecimalBoundaryGenerationNonce())
		proc := &Process{}
		require.Zero(t, proc.DecimalBoundaryGenerationNonce())
	})
}

// An unbalanced release against a registry that is no longer installed
// does nothing instead of corrupting a later generation (design decision
// log 49's lifecycle discipline).
func TestDecimalBoundaryRegistryUnbalancedReleaseIssue30938(t *testing.T) {
	proc := &Process{}
	releaseDecimalBoundaryGeneration(proc, &DecimalBoundaryRegistry{depth: 1})
	require.Zero(t, proc.DecimalBoundaryGenerationDepth(),
		"the mismatched release leaves the process untouched")

	release := EnterDecimalBoundaryGeneration(proc)
	release()
	release()
	require.Zero(t, proc.DecimalBoundaryGenerationDepth(),
		"a double release unwinds exactly one entry")
}

// The read-once claim is keyed by the generation nonce: a claim slot
// published under a nonce the process has since left behind declines, so a
// stale ledger can never be attributed to a later statement (design
// decision log 64).
func TestDecimalBoundaryRegistryStaleNonceClaimIssue30938(t *testing.T) {
	proc := &Process{}
	release := EnterDecimalBoundaryGeneration(proc)
	proc.MarkDecimalBoundaryPreparedCTAS()
	proc.RecordDecimalBoundaryDecision("k", "v", "sig", true)
	release()

	proc.decimalBoundaryNonce++
	ledger, ok := proc.ClaimDecimalBoundaryEvidence()
	require.False(t, ok, "the stale nonce claim declines")
	require.Nil(t, ledger)
	require.True(t, proc.HasUnclaimedDecimalBoundaryEvidence(),
		"a declined claim leaves the stale slot untouched until the next generation or a nonce-matched claim")
}

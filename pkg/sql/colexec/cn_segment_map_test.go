// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

// newSegmentMapTestServer installs a fresh server for the default runtime
// and restores whatever server the process had before, so segment-map
// cases never observe state another test left behind.
func newSegmentMapTestServer(t *testing.T) *Server {
	t.Helper()
	rt := runtime.ServiceRuntime("")
	previous, existed := rt.GetGlobalVariables(runtime.ColexecServer)
	server := NewServer("")
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(runtime.ColexecServer, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.ColexecServer, server)
		}
	})
	return server
}

// TestTxnWorkspaceSegmentsRetireExactlyOwnRegistrations pins the issue
// #25590 review finding P2 contract on the segment map itself: a
// workspace's retirement deletes exactly the entries the workspace
// registered — never entries registered by other workspaces, including
// workspaces that share the same transaction ID (fragments of one
// distributed transaction do exactly that), and never extra entries of
// other transactions. The pre-fix per-txnID prefix sweep violated both
// halves: it cost a whole-CN map scan under the shared mutex on every
// fragment settlement, and a shared-txnID fragment settlement removed
// another live fragment's registrations.
func TestTxnWorkspaceSegmentsRetireExactlyOwnRegistrations(t *testing.T) {
	srv := newSegmentMapTestServer(t)

	txnID := []byte("shared-user-txn")
	foreignTxnID := []byte("unrelated-txn")
	const tableA = uint64(42)
	const tableB = uint64(43)

	writerA := &TxnWorkspaceSegments{}
	writerB := &TxnWorkspaceSegments{}
	foreign := &TxnWorkspaceSegments{}

	sidA := objectio.NewSegmentid()
	sidB := objectio.NewSegmentid()
	sidForeign := objectio.NewSegmentid()

	// Two workspaces of the SAME user transaction register distinct
	// segments; an unrelated transaction registers a third entry.
	srv.PutCnSegment(writerA, txnID, tableA, sidA, TxnWorkspaceUnCommitType)
	srv.PutCnSegment(writerB, txnID, tableB, sidB, TxnWorkspaceUnCommitType)
	srv.PutCnSegment(foreign, foreignTxnID, tableA, sidForeign, TxnWorkspaceUnCommitType)

	require.Equal(t, int32(TxnWorkspaceUnCommitType),
		srv.GetCnSegmentType(sidA, tableA, txnID))
	require.Equal(t, int32(TxnWorkspaceUnCommitType),
		srv.GetCnSegmentType(sidB, tableB, txnID))
	require.Equal(t, int32(TxnWorkspaceUnCommitType),
		srv.GetCnSegmentType(sidForeign, tableA, foreignTxnID))

	// A repeated registration of the same segment dedupes in the ledger,
	// so retirement stays proportional to distinct registrations.
	srv.PutCnSegment(writerA, txnID, tableA, sidA, TxnWorkspaceUnCommitType)

	// Retire writerA: exactly its own segment disappears. Pre-fix this
	// swept writerB's and foreign's entries away too, because every entry
	// with the same txnID prefix — and, for the full-scan cost, every
	// foreign entry — was compared under the shared mutex.
	srv.RetireTxnWorkspaceSegments(writerA)

	require.Zero(t, srv.GetCnSegmentType(sidA, tableA, txnID),
		"the retiring workspace's own registration must be removed at its terminal")
	require.Equal(t, int32(TxnWorkspaceUnCommitType),
		srv.GetCnSegmentType(sidB, tableB, txnID),
		"another fragment workspace of the SAME user transaction keeps its registration")
	require.Equal(t, int32(TxnWorkspaceUnCommitType),
		srv.GetCnSegmentType(sidForeign, tableA, foreignTxnID),
		"an unrelated transaction's registration keeps untouched")

	// The ledger is cleared, so a second terminal — a double settlement —
	// is a no-op, and the remaining writer's own terminal removes its
	// entry exactly.
	srv.RetireTxnWorkspaceSegments(writerA)
	require.Equal(t, int32(TxnWorkspaceUnCommitType),
		srv.GetCnSegmentType(sidB, tableB, txnID))
	srv.RetireTxnWorkspaceSegments(writerB)
	require.Zero(t, srv.GetCnSegmentType(sidB, tableB, txnID))

	// A zero-value ledger — the ordinary read-only remote fragment case —
	// retires with nothing to remove.
	srv.RetireTxnWorkspaceSegments(&TxnWorkspaceSegments{})

	// The remaining consumers still see the surviving entries.
	require.Equal(t, int32(TxnWorkspaceUnCommitType),
		srv.GetCnSegmentType(sidForeign, tableA, foreignTxnID))
}

// TestTxnWorkspaceSegmentsRetireEmptyNeverOpensSegmentMutex proves the
// read-fragment half of the issue #25590 review finding P2: a workspace
// that registered no segment retires WITHOUT opening the shared segment
// mutex, so an ordinary remote SELECT fragment's settlement keeps no
// coupling to unrelated writers' registrations and lookups on the CN.
// The test holds the mutex while an empty-ledger retirement runs; an
// implementation that locked — like the removed whole-map prefix sweep —
// blocks until the test's watchdog.
func TestTxnWorkspaceSegmentsRetireEmptyNeverOpensSegmentMutex(t *testing.T) {
	srv := newSegmentMapTestServer(t)

	empty := &TxnWorkspaceSegments{}
	srv.cnSegmentMap.Lock()
	done := make(chan struct{})
	go func() {
		srv.RetireTxnWorkspaceSegments(empty)
		close(done)
	}()
	select {
	case <-done:
		// Completed while the mutex was held: the empty workspace's
		// retirement never opened it.
	case <-time.After(30 * time.Second):
		srv.cnSegmentMap.Unlock()
		t.Fatal("empty-ledger retirement blocked on the segment mutex")
	}
	srv.cnSegmentMap.Unlock()
}

// BenchmarkRetireTxnWorkspaceSegments measures the settlement-side cost
// of segment retirement against the foreign-segment population the
// issue #25590 review found on live CNs (its diagnostic overlay measured
// whole-CN sweeps of ~449µs at 50k and ~8.1ms at 200k foreign segments).
// The ledger retirement must stay flat in the foreign population.
func BenchmarkRetireTxnWorkspaceSegments(b *testing.B) {
	rt := runtime.ServiceRuntime("")
	previous, existed := rt.GetGlobalVariables(runtime.ColexecServer)
	server := NewServer("")
	defer func() {
		if existed {
			rt.SetGlobalVariables(runtime.ColexecServer, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.ColexecServer, server)
		}
	}()

	txnID := []byte("owned-txn")
	const table = uint64(42)
	const ownRegistrations = 16

	for _, foreignPopulation := range []int{0, 50_000, 200_000} {
		b.Run(fmt.Sprintf("foreign=%d/empty-ledger", foreignPopulation), func(b *testing.B) {
			fillForeignSegments(server, foreignPopulation)
			var readFragment TxnWorkspaceSegments
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				server.RetireTxnWorkspaceSegments(&readFragment)
			}
			b.StopTimer()
			clearForeignSegments(server, foreignPopulation)
		})
		b.Run(fmt.Sprintf("foreign=%d/own-%d-keys", foreignPopulation, ownRegistrations), func(b *testing.B) {
			fillForeignSegments(server, foreignPopulation)
			writer := &TxnWorkspaceSegments{}
			sids := make([]*objectio.Segmentid, ownRegistrations)
			for j := range sids {
				sids[j] = objectio.NewSegmentid()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, sid := range sids {
					server.PutCnSegment(writer, txnID, table, sid, TxnWorkspaceUnCommitType)
				}
				server.RetireTxnWorkspaceSegments(writer)
			}
			b.StopTimer()
			clearForeignSegments(server, foreignPopulation)
			server.RetireTxnWorkspaceSegments(writer)
		})
	}
}

// foreignSegmentKey deterministically derives the map key of the i-th
// dummy foreign entry, so fill and clear address exactly the same key.
func foreignSegmentKey(i int) string {
	var sid objectio.Segmentid
	binary.BigEndian.PutUint64(sid[:8], uint64(i))
	return segmentKey([]byte(fmt.Sprintf("foreign-%d", i)), uint64(i), &sid)
}

// fillForeignSegments populates the map with foreign dummy entries the
// way unrelated live transactions' registrations accumulate on a busy CN.
func fillForeignSegments(srv *Server, population int) {
	if population == 0 {
		return
	}
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	for i := 0; i < population; i++ {
		srv.cnSegmentMap.mp[foreignSegmentKey(i)] = TxnWorkspaceUnCommitType
	}
}

// clearForeignSegments removes exactly the dummy entries a matching
// fillForeignSegments call created.
func clearForeignSegments(srv *Server, population int) {
	if population == 0 {
		return
	}
	srv.cnSegmentMap.Lock()
	defer srv.cnSegmentMap.Unlock()
	for i := 0; i < population; i++ {
		delete(srv.cnSegmentMap.mp, foreignSegmentKey(i))
	}
}

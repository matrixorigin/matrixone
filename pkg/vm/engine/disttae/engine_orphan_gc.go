// Copyright 2021 - 2026 Matrix Origin
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

package disttae

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"go.uber.org/zap"
)

// durableOrphanGCRegistry is the cleanup owner of last resort for
// unpublished object names whose reclamation has not been CONFIRMED
// (created for r30 review F3 / D128; bounded and re-keyed for r31 review
// F2/F3 / D130/D131). Before D128, a close-time GC drain handed names to
// an asynchronous delete and dropped its own copy first: a submission
// failure (pool closed/unavailable) or the admitted task's storage
// failure were only logged, and the unpublished file — already absent
// from every workspace registration — lost its last cleanup owner at
// transaction close.
//
// Ownership is established BEFORE the first fallible reclamation step,
// survives the registering transaction's close and any number of failed
// submission or storage attempts, and is released ONLY by a confirmed
// DeleteUnpublishedObjects success. Convergence comes from triggers on
// the registering close itself and on every later gcFiles admission. A
// process exit before the first successful deletion forfeits the
// in-memory retention — the same terminal boundary the pre-D128
// fire-and-forget submissions had.
//
// r31 review F3 (D131) — ownership outlives any single Engine:
//
// The registry keys retained inventory by the storage identity (the
// FileService the names live on), never by *Engine. A failed deletion
// followed by Engine.Close therefore keeps its cleanup contract: the
// closing engine runs one synchronous terminal pass
// (retireEngineDurableOrphanGC), and any OTHER live engine over the same
// storage drives later sweeps through its ordinary gcFiles admissions.
// Neither the registry entry nor the retained names hold an *Engine, so
// a retired engine is fully releasable while its unconfirmed names
// remain owned.
//
// r31 review F2 (D130) — bounded retention and bounded retry work:
//
//   - Retention admission has a capacity (capacity, default
//     defaultDurableOrphanGCCapacity names per storage). Names beyond
//     capacity first get an immediate SYNCHRONOUS reclaim attempt by the
//     registering caller; only names whose direct attempt also fails are
//     retained beyond the nominal capacity (soft overflow, loudly
//     logged). Ownership is thus preserved exactly when capacity is
//     exhausted — no name is ever dropped silently.
//   - One sweep submits at most sweepNameBudget names (default
//     4xGCBatchOfFileCount), oldest registration first; the remainder
//     stays retained for later triggers/chains.
//   - Sweeps are single-flight per storage: a trigger arriving while a
//     sweep is in flight coalesces into sweepPending (no duplicate
//     submissions, no concurrent batches over the same inventory), and
//     the completing sweep chains exactly one follow-up when pending
//     work remains.
//
// r32 review F1/F2/F3 (D132/D133/D134) harden that lifecycle again:
//
//   - An overflow admission pins its registry entry across the
//     synchronous reclaim I/O (pendingAdmissions);
//     maybeReleaseEntryLocked requires the pin count to be zero, so a
//     concurrent drain can never drop the entry underneath a pending
//     admission's post-reclaim re-registration (D132).
//   - Producers book cleanup ownership BEFORE persisting orphanable
//     files (reserveDurableOrphanOwnership), bounded by the
//     per-storage hardCapacity ceiling over retained+inflight+reserved.
//     A reservation that cannot find headroom after bounded synchronous
//     reclaim rounds fails the producing statement before any file
//     exists — sustained storage failure applies backpressure to the
//     producer instead of growing retained state without bound (D133).
//   - The engine-close terminal pass runs ONE bounded flight over a
//     FINITE snapshot, excluding an active sweep's in-flight names and
//     never touching that sweep's flight state; the pass never
//     re-snapshots, so a peer engine registering new names cannot hold
//     Engine.Close open, and leftovers stay owned (D134).
//
// r33 review F1/F2 (D135/D136) close the two remaining reservation
// lifecycle terminals:
//
//   - A reservation transferred into registerDurableOrphanGC settles
//     exactly once no matter which terminal the name takes: retention
//     adopts it (D133's conversion) or the direct overflow reclaim
//     confirms its deletion — the confirmed names count into the same
//     anonymous settlement, so a successful reclaim can never leave a
//     phantom slot pinning the entry or taxing later producers (D135).
//   - A rollback resurrection releases the reservations of the retired
//     files it returns to workspace ownership, so repeated
//     retirement/rollback cycles cannot exhaust the ceiling with
//     healthy deletion (D136; implemented in pk_move_link_fixup.go).
type durableOrphanGCRegistry struct {
	mu      sync.Mutex
	entries map[fileservice.FileService]*durableOrphanGCEntry
	// capacity bounds retained names per storage; sweepNameBudget bounds
	// the names one sweep submits; hardCapacity is the per-storage
	// ceiling over retained+inflight+reserved names that reservations
	// enforce BEFORE production (r32 review F2, D133);
	// reserveReclaimRounds bounds the synchronous reclaim rounds one
	// reservation runs to free headroom. All are registry fields (not
	// constants) so tests can pin small values deterministically; the
	// production defaults are applied by the package-level instance.
	capacity             int
	sweepNameBudget      int
	hardCapacity         int
	reserveReclaimRounds int
	// reliefBatchSize chunks the synchronous overflow relief attempts
	// at registration (r33 review F1, D135): production keeps
	// GCBatchOfFileCount, and the field is test-pinnable like the
	// knobs above so mixed-result relief batches are deterministic.
	reliefBatchSize int
}

// durableOrphanGCEntry is one storage's retained inventory. order keeps
// the FIFO registration order (deregistered names die in place and are
// filtered at sweep-compaction time), set is the live membership, and
// inflight names the batches of the currently active sweep — the only
// names a sweep's completion may deregister.
type durableOrphanGCEntry struct {
	order    []string
	set      map[string]struct{}
	inflight map[string]struct{}
	// sweepActive marks an in-flight asynchronous sweep; sweepPending
	// records that at least one trigger arrived during it, so the
	// completing sweep chains one follow-up instead of every trigger
	// submitting its own duplicate work (D130).
	sweepActive  bool
	sweepPending bool
	// reserved counts ownership slots producers booked BEFORE persisting
	// orphanable files (r32 review F2, D133). Registering a produced name
	// converts one reserved slot, so in-contract production keeps
	// retained+inflight+reserved <= hardCapacity without any name
	// attribution; releasing covers files that never became orphanable.
	reserved int
	// pendingAdmissions counts overflow admissions currently inside
	// their synchronous reclaim I/O (r32 review F1, D132):
	// maybeReleaseEntryLocked requires it to be zero, so an admission's
	// entry cannot be dropped by a concurrent drain while its storage
	// call is pending — the post-reclaim re-registration always lands
	// in a live entry instead of dereferencing nil.
	pendingAdmissions int
}

const (
	// defaultDurableOrphanGCCapacity bounds the retained inventory per
	// storage: 2^16 names of object paths (~a few MB of strings). It is
	// a relief-valve bound, not a drop policy — names that would exceed
	// it get an immediate synchronous reclaim attempt and only then,
	// if still failing, a loudly logged retention beyond the bound.
	defaultDurableOrphanGCCapacity = 1 << 16
	// defaultDurableOrphanGCHardCapacity is the per-storage ceiling over
	// retained+inflight+reserved names (r32 review F2, D133). Reserved
	// production can never cross it: reservations beyond it either free
	// headroom through bounded synchronous reclaim rounds or fail the
	// producing statement before any file exists.
	defaultDurableOrphanGCHardCapacity = 8 * defaultDurableOrphanGCCapacity
	// defaultDurableOrphanGCReserveReclaimRounds bounds the synchronous
	// reclaim rounds one reservation attempt runs to free headroom
	// before returning its exhaustion error.
	defaultDurableOrphanGCReserveReclaimRounds = 4
	// defaultDurableOrphanGCSweepNameBudget bounds one sweep's submitted
	// work to four ordinary GC batches.
	defaultDurableOrphanGCSweepNameBudget = 4 * GCBatchOfFileCount
)

var durableOrphanGC = &durableOrphanGCRegistry{
	entries:              make(map[fileservice.FileService]*durableOrphanGCEntry),
	capacity:             defaultDurableOrphanGCCapacity,
	sweepNameBudget:      defaultDurableOrphanGCSweepNameBudget,
	hardCapacity:         defaultDurableOrphanGCHardCapacity,
	reserveReclaimRounds: defaultDurableOrphanGCReserveReclaimRounds,
	reliefBatchSize:      GCBatchOfFileCount,
}

// registerDurableOrphanGC records ownership of names for the storage fs.
// Registration is synchronous and (capacity-relief aside) infallible, so
// callers can transfer ownership ahead of every fallible reclamation
// step. Names beyond the capacity bound get one immediate synchronous
// reclaim attempt here; only a still-failing name is retained past the
// bound — ownership never dies silently at admission (D130).
func registerDurableOrphanGC(fs fileservice.FileService, names ...string) {
	if fs == nil || len(names) == 0 {
		return
	}
	var overflow []string
	durableOrphanGC.mu.Lock()
	ent := durableOrphanGC.entries[fs]
	if ent == nil {
		ent = &durableOrphanGCEntry{
			set:      make(map[string]struct{}),
			inflight: make(map[string]struct{}),
		}
		durableOrphanGC.entries[fs] = ent
	}
	seen := make(map[string]struct{}, len(names))
	added := 0
	for _, name := range names {
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		if _, ok := ent.set[name]; ok {
			continue
		}
		if len(ent.set) < durableOrphanGC.capacity {
			ent.set[name] = struct{}{}
			ent.order = append(ent.order, name)
			added++
		} else {
			overflow = append(overflow, name)
		}
	}
	// D133: every newly owned name converts one reserved slot the
	// producer booked before persisting its files (anonymous
	// per-storage accounting).
	if take := added; take > 0 {
		if take > ent.reserved {
			take = ent.reserved
		}
		ent.reserved -= take
	}
	if len(overflow) > 0 {
		// D132: pin the entry across the reclaim I/O below — a
		// concurrent drain must not release it mid-admission.
		ent.pendingAdmissions++
	}
	durableOrphanGC.mu.Unlock()

	if len(overflow) == 0 {
		return
	}
	// Capacity relief: the registering caller runs the reclaim
	// synchronously, in ordinary GC-sized chunks. Confirmed deletions
	// never enter the registry; names that still fail register past the
	// bound with an explicit log — retained inventory can exceed capacity
	// only while storage keeps failing (D130's ownership guarantee).
	failed := make([]string, 0, len(overflow))
	for start := 0; start < len(overflow); start += durableOrphanGC.reliefBatchSize {
		end := min(start+durableOrphanGC.reliefBatchSize, len(overflow))
		batch := overflow[start:end]
		if _, err := ioutil.DeleteUnpublishedObjects(context.Background(), fs, batch...); err != nil {
			failed = append(failed, batch...)
		}
	}
	if len(failed) > 0 {
		logutil.Warn("orphan-gc admission over capacity after direct reclaim — ownership retained past the bound",
			zap.Int("failed", len(failed)),
			zap.Int("capacity", durableOrphanGC.capacity))
	}
	durableOrphanGC.mu.Lock()
	defer durableOrphanGC.mu.Unlock()
	ent.pendingAdmissions--
	// D135: every reservation transferred with these names settles
	// exactly once, no matter which terminal the name took — the
	// confirm-deleted overflow names count into the same anonymous
	// conversion as the names retention adopts below, so a successful
	// direct reclaim can never leave a phantom slot pinning the entry
	// or taxing later producers (r33 review F1).
	settled := len(overflow) - len(failed)
	if len(failed) == 0 {
		// Every overflow name confirmed deleted: nothing registers —
		// settle the confirmed names' reservations, then drop the pin
		// and the (possibly empty) shell.
		if take := settled; take > 0 {
			if take > ent.reserved {
				take = ent.reserved
			}
			ent.reserved -= take
		}
		maybeReleaseEntryLocked(fs, ent)
		return
	}
	// D132's pin guarantees this entry is still live:
	// maybeReleaseEntryLocked cannot release an entry with pending
	// admissions, and entries are never replaced under a live fs key.
	ent = durableOrphanGC.entries[fs]
	added = 0
	for _, name := range failed {
		if _, ok := ent.set[name]; ok {
			continue
		}
		ent.set[name] = struct{}{}
		ent.order = append(ent.order, name)
		added++
	}
	if take := added + settled; take > 0 {
		if take > ent.reserved {
			take = ent.reserved
		}
		ent.reserved -= take
	}
	// D133's defensive terminal: in-contract production reserved these
	// names before persisting them, so crossing the hard ceiling here
	// means a producer persisted files without booking ownership. Loud
	// — and ownership still wins over the bound, never a silent drop.
	if len(ent.set)+len(ent.inflight)+ent.reserved > durableOrphanGC.hardCapacity {
		logutil.Error("orphan-gc retained inventory crossed the hard bound — unreserved orphan production",
			zap.Int("retained", len(ent.set)),
			zap.Int("inflight", len(ent.inflight)),
			zap.Int("reserved", ent.reserved),
			zap.Int("hardCapacity", durableOrphanGC.hardCapacity))
	}
}

// reserveDurableOrphanOwnership books cleanup ownership for n
// not-yet-persisted orphanable files on storage fs (r32 review F2,
// D133). Producers call it BEFORE any persist: while
// retained+inflight+reserved+n fits the per-storage hardCapacity the
// reservation takes immediately; otherwise the call runs up to
// reserveReclaimRounds bounded synchronous rounds — each reclaiming the
// oldest retained GC batch that no active sweep owns — to free
// headroom, and returns the stable exhaustion error when headroom still
// does not exist. The producer then fails BEFORE any file exists, so
// sustained deletion failure applies backpressure to the producing
// statement instead of growing retained state without bound.
func reserveDurableOrphanOwnership(fs fileservice.FileService, n int) error {
	if fs == nil || n <= 0 {
		return nil
	}
	for round := 0; ; round++ {
		durableOrphanGC.mu.Lock()
		ent := durableOrphanGC.entries[fs]
		used := 0
		if ent != nil {
			used = len(ent.set) + len(ent.inflight) + ent.reserved
		}
		if used+n <= durableOrphanGC.hardCapacity {
			if ent == nil {
				ent = &durableOrphanGCEntry{
					set:      make(map[string]struct{}),
					inflight: make(map[string]struct{}),
				}
				durableOrphanGC.entries[fs] = ent
			}
			ent.reserved += n
			durableOrphanGC.mu.Unlock()
			return nil
		}
		if round >= durableOrphanGC.reserveReclaimRounds || ent == nil {
			durableOrphanGC.mu.Unlock()
			return errOrphanOwnershipExhausted(n)
		}
		// Reclaim candidates: oldest retained names no active sweep owns.
		sel := make([]string, 0, GCBatchOfFileCount)
		for _, name := range ent.order {
			if len(sel) >= GCBatchOfFileCount {
				break
			}
			if _, ok := ent.set[name]; !ok {
				continue
			}
			if _, ok := ent.inflight[name]; ok {
				continue
			}
			sel = append(sel, name)
		}
		durableOrphanGC.mu.Unlock()
		if len(sel) == 0 {
			// Everything retained is in flight with a live sweep: more
			// rounds cannot free headroom this pass.
			return errOrphanOwnershipExhausted(n)
		}
		if _, err := ioutil.DeleteUnpublishedObjects(context.Background(), fs, sel...); err != nil {
			logutil.Warn("orphan-gc reservation reclaim round failed — backpressure continues",
				zap.Int("round", round), zap.Int("names", len(sel)), zap.Error(err))
			continue
		}
		durableOrphanGC.mu.Lock()
		if ent := durableOrphanGC.entries[fs]; ent != nil {
			deregisterBatchLocked(ent, sel)
		}
		durableOrphanGC.mu.Unlock()
	}
}

// errOrphanOwnershipExhausted is the D133 backpressure contract: the
// registry is at its hard ceiling and bounded synchronous reclamation
// could not confirm headroom for n new orphanable files.
func errOrphanOwnershipExhausted(n int) error {
	return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
		"durable orphan ownership exhausted: cannot reserve cleanup ownership for %d orphanable files at the retention ceiling — storage reclamation is not confirming deletions",
		n))
}

// releaseDurableOrphanReservation returns booked ownership for files
// that never became orphanable — the wave published its replacements,
// or the retirement never covered them (r32 review F2, D133).
// Underflow means a producer's accounting broke: loud, clamped, never
// silently negative.
func releaseDurableOrphanReservation(fs fileservice.FileService, n int) {
	if fs == nil || n <= 0 {
		return
	}
	durableOrphanGC.mu.Lock()
	defer durableOrphanGC.mu.Unlock()
	ent := durableOrphanGC.entries[fs]
	if ent == nil {
		logutil.Error("orphan-gc reservation release with no registry entry — producer accounting violated",
			zap.Int("names", n))
		return
	}
	if n > ent.reserved {
		logutil.Error("orphan-gc reservation release exceeds the booked count — producer accounting violated",
			zap.Int("release", n), zap.Int("reserved", ent.reserved))
		ent.reserved = 0
	} else {
		ent.reserved -= n
	}
	maybeReleaseEntryLocked(fs, ent)
}

// deregisterBatchLocked drops names from the live membership and the
// active sweep's inflight bookkeeping. Callers hold durableOrphanGC.mu.
func deregisterBatchLocked(ent *durableOrphanGCEntry, names []string) {
	for _, name := range names {
		delete(ent.set, name)
		delete(ent.inflight, name)
	}
}

// failBatchLocked returns in-flight names to plain retention after an
// unconfirmed attempt (submit error or storage failure). Callers hold
// durableOrphanGC.mu.
func failBatchLocked(ent *durableOrphanGCEntry, names []string) {
	for _, name := range names {
		delete(ent.inflight, name)
	}
}

// compactOrderLocked rebuilds the FIFO order from the live membership,
// dropping deregistered slots. Runs at sweep end (single-flight, so no
// other sweep mutates concurrently; registrations only append under the
// same lock and are preserved).
func compactOrderLocked(ent *durableOrphanGCEntry) {
	kept := ent.order[:0]
	for _, name := range ent.order {
		if _, ok := ent.set[name]; ok {
			kept = append(kept, name)
		}
	}
	ent.order = kept
}

// maybeReleaseEntryLocked removes the storage entry once nothing is
// retained, nothing is reserved or in flight, no admission is pending,
// and no sweep is running — the registry never retains an empty shell.
// The reserved (D133) and pendingAdmissions (D132) guards keep the
// entry alive across pre-persisted production and across an overflow
// admission's synchronous reclaim I/O. Callers hold durableOrphanGC.mu.
func maybeReleaseEntryLocked(fs fileservice.FileService, ent *durableOrphanGCEntry) {
	if len(ent.set) == 0 && len(ent.inflight) == 0 && ent.reserved == 0 &&
		ent.pendingAdmissions == 0 && !ent.sweepActive {
		delete(durableOrphanGC.entries, fs)
	}
}

// durableOrphanGCNames snapshots the retained inventory of the storage
// behind eng (test and diagnostic surface; also the terminal pass's
// source).
func durableOrphanGCNames(eng *Engine) []string {
	if eng == nil || eng.fs == nil {
		return nil
	}
	durableOrphanGC.mu.Lock()
	defer durableOrphanGC.mu.Unlock()
	ent := durableOrphanGC.entries[eng.fs]
	if ent == nil {
		return nil
	}
	names := make([]string, 0, len(ent.set))
	for name := range ent.set {
		names = append(names, name)
	}
	return names
}

// retryDurableOrphanGC submits one asynchronous reclaim sweep over the
// names still owned for the storage behind eng, through eng's GC pool
// and close discipline. Ownership survives every failure mode of the
// attempt: a closed engine, an unavailable pool, a submission error, or
// the admitted task's storage error all retain the registration
// untouched and are logged; only a confirmed deletion deregisters.
//
// The sweep is bounded (sweepNameBudget oldest names) and single-flight
// per storage (D130): a trigger arriving while a sweep runs coalesces
// into sweepPending, and the completing sweep chains one follow-up. A
// retry driven through a CLOSED engine returns early with ownership
// retained — any other live engine over the same storage sweeps the same
// inventory (D131).
func retryDurableOrphanGC(eng *Engine) {
	if eng == nil || eng.fs == nil {
		return
	}
	fs := eng.fs
	durableOrphanGC.mu.Lock()
	ent := durableOrphanGC.entries[fs]
	if ent == nil || len(ent.set) == 0 {
		durableOrphanGC.mu.Unlock()
		return
	}
	if ent.sweepActive {
		ent.sweepPending = true
		durableOrphanGC.mu.Unlock()
		return
	}
	if eng.dynamicCtx.closed.Load() {
		names := len(ent.set)
		durableOrphanGC.mu.Unlock()
		logutil.Warn("orphan-gc retry: engine closing, ownership retained",
			zap.Int("names", names))
		return
	}
	if eng.gcPool == nil {
		names := len(ent.set)
		durableOrphanGC.mu.Unlock()
		logutil.Warn("orphan-gc retry: engine GC pool unavailable, ownership retained",
			zap.Int("names", names))
		return
	}
	sel := make([]string, 0, min(durableOrphanGC.sweepNameBudget, len(ent.set)))
	for _, name := range ent.order {
		if len(sel) >= durableOrphanGC.sweepNameBudget {
			break
		}
		if _, ok := ent.set[name]; !ok {
			continue
		}
		if _, ok := ent.inflight[name]; ok {
			continue
		}
		sel = append(sel, name)
	}
	if len(sel) == 0 {
		durableOrphanGC.mu.Unlock()
		return
	}
	ent.sweepActive = true
	for _, name := range sel {
		ent.inflight[name] = struct{}{}
	}
	durableOrphanGC.mu.Unlock()

	var remaining int64
	nBatches := (len(sel) + GCBatchOfFileCount - 1) / GCBatchOfFileCount
	remaining = int64(nBatches)
	for start := 0; start < len(sel); start += GCBatchOfFileCount {
		end := min(start+GCBatchOfFileCount, len(sel))
		batch := append([]string(nil), sel[start:end]...)
		if err := eng.gcPool.Submit(func() {
			// Mirroring gcFiles' task: on a cancellation-free context and
			// bounded by engine close waiting for the actual return.
			if _, err := ioutil.DeleteUnpublishedObjects(context.Background(), fs, batch...); err != nil {
				logutil.Warnf("orphan-gc retry delete failed:%v, err:%v — ownership retained", batch, err)
				durableOrphanGC.mu.Lock()
				if ent := durableOrphanGC.entries[fs]; ent != nil {
					failBatchLocked(ent, batch)
				}
				durableOrphanGC.mu.Unlock()
			} else {
				durableOrphanGC.mu.Lock()
				if ent := durableOrphanGC.entries[fs]; ent != nil {
					deregisterBatchLocked(ent, batch)
				}
				durableOrphanGC.mu.Unlock()
			}
			finishDurableOrphanGCSweep(eng, fs, &remaining)
		}); err != nil {
			logutil.Warn("orphan-gc retry submit failed — ownership retained",
				zap.Strings("names", batch), zap.Error(err))
			durableOrphanGC.mu.Lock()
			if ent := durableOrphanGC.entries[fs]; ent != nil {
				failBatchLocked(ent, batch)
			}
			durableOrphanGC.mu.Unlock()
			finishDurableOrphanGCSweep(eng, fs, &remaining)
		}
	}
}

// finishDurableOrphanGCSweep completes one batch of the active sweep.
// The last batch's completion closes the sweep: retained work selected
// or registered during it gets exactly one chained follow-up sweep,
// never a duplicate of the still-open one (D130).
func finishDurableOrphanGCSweep(eng *Engine, fs fileservice.FileService, remaining *int64) {
	if atomic.AddInt64(remaining, -1) != 0 {
		return
	}
	durableOrphanGC.mu.Lock()
	ent := durableOrphanGC.entries[fs]
	if ent == nil {
		durableOrphanGC.mu.Unlock()
		return
	}
	ent.sweepActive = false
	pending := ent.sweepPending
	ent.sweepPending = false
	compactOrderLocked(ent)
	left := len(ent.set)
	maybeReleaseEntryLocked(fs, ent)
	durableOrphanGC.mu.Unlock()
	if pending && left > 0 {
		// Chain one follow-up; the engines on the chain may have closed
		// by now, which retryDurableOrphanGC handles by retaining.
		go retryDurableOrphanGC(eng)
	}
}

// retireEngineDurableOrphanGC is the terminal cleanup pass of D131,
// bounded and sweep-coordinated by D134 (r32 review F3): Engine.Close
// runs ONE bounded flight over a FINITE snapshot — the names retained at
// close, excluding any active sweep's in-flight set — and confirms or
// retains each once. The pass never re-snapshots and never loops:
// names another live engine registers DURING the flight stay owned
// under the storage key for that engine's later sweeps instead of
// holding Engine.Close open, and a fresh registration after every
// confirmed deletion can no longer keep the close pass alive forever.
//
// Sweep coordination (D130's single-flight contract): when no sweep is
// active this pass owns the flight itself — it marks its snapshot
// in-flight so concurrent triggers and reservation reclaim rounds skip
// those names, and clears only its own flight state at the end. When a
// sweep IS active, that flight belongs to another engine (or to a chain
// still draining after gcPool release); this pass deletes around its
// in-flight names and leaves its sweepActive/sweepPending bookkeeping
// untouched. Overlap with such a sweep is safe anyway:
// DeleteUnpublishedObjects treats ErrFileNotFound as success, so a name
// confirmed by either side deregisters for both.
func retireEngineDurableOrphanGC(eng *Engine) {
	if eng == nil || eng.fs == nil {
		return
	}
	fs := eng.fs

	durableOrphanGC.mu.Lock()
	ent := durableOrphanGC.entries[fs]
	if ent == nil || (len(ent.set) == 0 && ent.reserved == 0) {
		if ent != nil {
			maybeReleaseEntryLocked(fs, ent)
		}
		durableOrphanGC.mu.Unlock()
		return
	}
	ownFlight := !ent.sweepActive
	if ownFlight {
		ent.sweepActive = true
		ent.sweepPending = false
	}
	names := make([]string, 0, len(ent.set))
	for _, name := range ent.order {
		if _, ok := ent.set[name]; !ok {
			continue
		}
		if _, ok := ent.inflight[name]; ok {
			// The active sweep owns this name's confirmation.
			continue
		}
		names = append(names, name)
	}
	if ownFlight {
		for _, name := range names {
			ent.inflight[name] = struct{}{}
		}
	}
	durableOrphanGC.mu.Unlock()

	for start := 0; start < len(names); start += GCBatchOfFileCount {
		end := min(start+GCBatchOfFileCount, len(names))
		batch := names[start:end]
		if _, err := ioutil.DeleteUnpublishedObjects(context.Background(), fs, batch...); err != nil {
			logutil.Warn("orphan-gc close pass could not confirm deletions — ownership retained under the storage key",
				zap.Strings("names", batch), zap.Error(err))
			durableOrphanGC.mu.Lock()
			if ent := durableOrphanGC.entries[fs]; ent != nil {
				failBatchLocked(ent, batch)
			}
			durableOrphanGC.mu.Unlock()
			continue
		}
		durableOrphanGC.mu.Lock()
		if ent := durableOrphanGC.entries[fs]; ent != nil {
			deregisterBatchLocked(ent, batch)
		}
		durableOrphanGC.mu.Unlock()
	}

	durableOrphanGC.mu.Lock()
	defer durableOrphanGC.mu.Unlock()
	ent = durableOrphanGC.entries[fs]
	if ent == nil {
		// The active sweep's completion drained and released the entry
		// while this pass ran — nothing left to coordinate.
		return
	}
	if ownFlight {
		// Release only this pass's flight state; leftover names
		// (storage failures and post-snapshot registrations alike)
		// stay retained for the next trigger, and the reservation
		// reclaim rounds may select them again.
		ent.sweepActive = false
		compactOrderLocked(ent)
	}
	left := len(ent.set)
	maybeReleaseEntryLocked(fs, ent)
	if left > 0 {
		logutil.Warn("orphan-gc close pass retains unconfirmed names under the storage key",
			zap.Int("names", left))
	}
}

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

package databranchutils

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

// This file is the fault-framework envelope certification of the pk-move
// retention-ledger fencing witness subsystem (design D183(B.1), refined by
// D184(B.1), repaired by D185(B.1)). One certification executes per fencing
// helper lane, on the lane's own driver thread, as two envelope checks:
// CHECK A at lane admission (before any write is staged) and CHECK B on the
// same thread immediately BEFORE the stage transaction's commit is invoked.
// Both checks read ONLY the fault package's exported, non-mutating envelope
// predicate fault.Status() (fault.go:359-365): ONE atomic enabled flag load
// answering "is the fault framework enabled at all". No fault point is ever
// probed: zero invocation-counter mutation, zero frequency or probability
// evaluation, zero action execution, zero channel round-trip — genuinely
// wait-free and side-effect-free.
//
// Why the envelope suffices: a fault point can exist or fire only while its
// domain's faultMap is installed — AddFaultPoint refuses a disabled
// framework, TriggerFaultInDomain early-returns on a nil faultMap, and
// Disable's stopFaultMap CAS-clears the enabled flag then STOPs the run
// loop. A framework DISABLED at both checks therefore excludes EVERY arming
// state — immediate, delayed-frequency, probabilistic, and blocking actions
// alike — through one non-mutating load per check.
//
// Scope precision (review-P3): the no-spill conclusion covers the lane's
// OWN stage statement under the WHOLE-LANE prerequisite that the framework
// stay disabled from CHECK A through the commit invocation. Only tombstone
// names mint exclusively in the commit-time dump a CHECK B precedes; a
// statement-time INSERT workspace spill (txnTable.Write driving dumpBatch
// during staging) CAN mint real data-object names ahead of commit, so the
// CHECK B floors are load-bearing: they keep the certified lane's staged
// shape strictly below every spill threshold. The staging-spill schedule
// that witnesses such real statement-time objects with engine-open cleanup
// is pinned by the persist-cells UT (TestPkMoveStagingInsertSpillRealNames).
//
// The certificate (both check results, the driver-exact staged shape, and
// the floors it was asserted against) is written into the lane's fencing
// record BEFORE the commit is invoked; the pool's safe-phase bookkeeping
// consumes it — detachment reads the certificate, never the checks alone.
// A failed check or a floor breach routes the lane IN-BODY to the rollback
// shape with NO commit RPC sent: the rollback runs on the driver thread with
// the engine necessarily open (the lane is active and pool-accounted, so the
// fail-stop drain cannot have passed the pool step), the lane classifies
// fail-loud, sheds PRE-DETACH, and the round fails.

// DataBranchFencingWorkspaceFloors is the compiled dump-floor set the
// certificate asserts the staged shape against. The production values are
// the disttae package's compiled constants (CommitWorkspaceThreshold 1MB,
// WriteWorkspaceThreshold 5MB, ExtraWorkspaceThreshold 500MB,
// InsertEntryThreshold 5000, types.go:154-157), pinned by the same-package
// tripwire UT reading those constants plus the live-harness fillDefaults
// equality assertion guarding the envelope against a deployment that lowers
// insertEntryMaxCount via the exported WithInsertEntryMaxCount knob.
type DataBranchFencingWorkspaceFloors struct {
	Commit      uint64
	Write       uint64
	Extra       uint64
	InsertEntry uint64
}

// DataBranchFencingCertCheck enumerates the two certification checks.
type DataBranchFencingCertCheck int

const (
	DataBranchFencingCertCheckA DataBranchFencingCertCheck = iota
	DataBranchFencingCertCheckB
)

func (c DataBranchFencingCertCheck) String() string {
	switch c {
	case DataBranchFencingCertCheckA:
		return "check-a-lane-admission"
	case DataBranchFencingCertCheckB:
		return "check-b-pre-commit"
	}
	return "unknown"
}

// DataBranchFencingCertClass partitions a certification failure: an
// envelope violation (the fault framework was ENABLED at a check — any
// armed point, including one that has not yet and may never fire) or a
// floor breach (the staged shape is not strictly under the compiled
// floors; a config-drift tripwire that cannot fire on the shipped
// driver-built shapes).
type DataBranchFencingCertClass int

const (
	DataBranchFencingCertEnvelopeViolation DataBranchFencingCertClass = iota
	DataBranchFencingCertFloorBreach
)

func (c DataBranchFencingCertClass) String() string {
	switch c {
	case DataBranchFencingCertEnvelopeViolation:
		return "fault-framework-envelope-violation"
	case DataBranchFencingCertFloorBreach:
		return "staged-shape-floor-breach"
	}
	return "unknown"
}

// DataBranchFencingCertificationError is the typed certification failure a
// stage executor returns instead of invoking the commit; it converts to the
// statement-visible determinate internal error only through the fencing
// error net (StatementError / NewDataBranchFencingUnavailable).
type DataBranchFencingCertificationError struct {
	Check DataBranchFencingCertCheck
	Class DataBranchFencingCertClass
	// Observed carries the rejected observation: the envelope flag for an
	// envelope violation, or the offending floor comparison detail.
	Observed string
}

func (e *DataBranchFencingCertificationError) Error() string {
	if e.Observed != "" {
		return "data branch fencing certification " + e.Check.String() +
			" failed (" + e.Class.String() + "): " + e.Observed
	}
	return "data branch fencing certification " + e.Check.String() +
		" failed (" + e.Class.String() + ")"
}

// AsDataBranchFencingCertificationError extracts the typed certification
// failure from a wrapped fencing error net.
func AsDataBranchFencingCertificationError(err error) (*DataBranchFencingCertificationError, bool) {
	if err == nil {
		return nil, false
	}
	if ce, ok := err.(*DataBranchFencingCertificationError); ok {
		return ce, true
	}
	if fe, ok := AsDataBranchFencingError(err); ok && fe.Cause != nil {
		if ce, ok2 := fe.Cause.(*DataBranchFencingCertificationError); ok2 {
			return ce, true
		}
	}
	return nil, false
}

// DataBranchFencingCertificate is the lane-record value published BEFORE
// the stage commit is invoked (D184(B.1)(iv), carried by D185(B.1)(iv)):
// both envelope check results, the driver-exact staged shape with its exact
// row and byte counts, and the floors the shape was asserted against. The
// records stay transient (D184(D)): they never reach durable storage.
type DataBranchFencingCertificate struct {
	CheckAEnvelopeDataObserved bool
	CheckBEnvelopeDataObserved bool
	Staged                     DataBranchFencingStageShape
	StagedRows                 int64
	StagedBytes                uint64
	Floors                     DataBranchFencingWorkspaceFloors
}

// Complete reports whether the certificate vouches a fully certified lane:
// both checks observed a DISABLED framework and the staged shape sits under
// the floors. Detachment and the residue arguments read this picture, never
// the checks alone.
func (c DataBranchFencingCertificate) Complete() bool {
	return c.CheckAEnvelopeDataObserved && c.CheckBEnvelopeDataObserved
}

// DataBranchFencingEnvelopeDisabled is the production certification
// predicate refined by D185(B.1)(i): ONE atomic load of the fault
// framework's enabled flag through its exported surface (fault.Status,
// fault.go:359-365). It is non-mutating, wait-free, and side-effect-free;
// it never probes a fault point.
func DataBranchFencingEnvelopeDisabled() bool {
	return !fault.Status()
}

// dataBranchFencingCertificationDisabled is the TEST-ONLY defect flag
// (D185(B.2) LEG 2/LEG 3 and the RED twins): when set, the certification
// checks pass unconditionally so persistence-establishing and bypass-replay
// schedules can run. It is a load-bearing test control only and must NEVER
// ship enabled in any production path; the production stage executor reads
// it at check time.
var dataBranchFencingCertificationDisabled atomic.Bool

// DisableDataBranchFencingCertificationForTest flips the certification
// defect flag and returns a restore func; harness-only (UT/live), never
// referenced from production flows other than the stage executor's check.
func DisableDataBranchFencingCertificationForTest(disabled bool) (restore func()) {
	prev := dataBranchFencingCertificationDisabled.Swap(disabled)
	return func() { dataBranchFencingCertificationDisabled.Store(prev) }
}

// DataBranchFencingEnvelopeCertify runs ONE certification check: the
// envelope load plus, for CHECK B, the staged-shape floor assertion. It
// returns nil when the framework is DISABLED (and the shape clears the
// floors); a typed *DataBranchFencingCertificationError otherwise. The
// certification-disabled defect flag passes every check unconditionally.
func DataBranchFencingEnvelopeCertify(
	ctx context.Context,
	check DataBranchFencingCertCheck,
	stagedRows int64,
	stagedBytes uint64,
	staged DataBranchFencingStageShape,
	floors DataBranchFencingWorkspaceFloors,
) error {
	if dataBranchFencingCertificationDisabled.Load() {
		return nil
	}
	if !DataBranchFencingEnvelopeDisabled() {
		return &DataBranchFencingCertificationError{
			Check: check, Class: DataBranchFencingCertEnvelopeViolation,
			Observed: "fault framework ENABLED (any armed point, including a never-fired one)",
		}
	}
	if check == DataBranchFencingCertCheckA {
		return nil
	}
	if floors.InsertEntry != 0 && stagedRows >= int64(floors.InsertEntry) {
		return &DataBranchFencingCertificationError{
			Check: check, Class: DataBranchFencingCertFloorBreach,
			Observed: moErrFloorDetail(stagedRows, stagedBytes, floors),
		}
	}
	if floors.Commit != 0 && stagedBytes >= floors.Commit {
		return &DataBranchFencingCertificationError{
			Check: check, Class: DataBranchFencingCertFloorBreach,
			Observed: moErrFloorDetail(stagedRows, stagedBytes, floors),
		}
	}
	if floors.Write != 0 && stagedBytes >= floors.Write {
		return &DataBranchFencingCertificationError{
			Check: check, Class: DataBranchFencingCertFloorBreach,
			Observed: moErrFloorDetail(stagedRows, stagedBytes, floors),
		}
	}
	if floors.Extra != 0 && stagedBytes >= floors.Extra {
		return &DataBranchFencingCertificationError{
			Check: check, Class: DataBranchFencingCertFloorBreach,
			Observed: moErrFloorDetail(stagedRows, stagedBytes, floors),
		}
	}
	_ = staged
	return nil
}

func moErrFloorDetail(rows int64, bytes uint64, floors DataBranchFencingWorkspaceFloors) string {
	return "staged rows/bytes " +
		strconv.FormatInt(rows, 10) + "/" + strconv.FormatUint(bytes, 10) +
		" not strictly under floors commit " + strconv.FormatUint(floors.Commit, 10) +
		" write " + strconv.FormatUint(floors.Write, 10) +
		" extra " + strconv.FormatUint(floors.Extra, 10) +
		" insert-entry " + strconv.FormatUint(floors.InsertEntry, 10)
}

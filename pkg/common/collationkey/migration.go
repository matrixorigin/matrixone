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

package collationkey

import (
	"bytes"
	"fmt"
)

// MigrationPhase is the persisted stop-write state for one physical relation.
// The transitions are deliberately narrower than a generic workflow: a
// published migration cannot be rolled back to the old key identity.
type MigrationPhase uint8

const (
	MigrationOpen MigrationPhase = iota
	MigrationDraining
	MigrationExclusive
	MigrationPublished
	MigrationAborted
)

func (p MigrationPhase) String() string {
	switch p {
	case MigrationOpen:
		return "open"
	case MigrationDraining:
		return "draining"
	case MigrationExclusive:
		return "exclusive"
	case MigrationPublished:
		return "published"
	case MigrationAborted:
		return "aborted"
	default:
		return fmt.Sprintf("phase(%d)", p)
	}
}

// MigrationOwner is a fencing identity, not a process name. A reconnecting
// owner must present the same incarnation and claim token; recovery takes a
// new incarnation/token before it can perform any side effect.
type MigrationOwner struct {
	OwnerID     string
	Incarnation uint64
	ClaimToken  []byte
}

// MigrationGate is the dependency-light durable state contract for explicit
// unique-key migration. The maps are exported so a catalog/HAKeeper adapter
// can serialize them into its own typed record; methods always copy byte
// slices and do not rely on process-global state.
type MigrationGate struct {
	RelationID          uint64
	MigrationEpoch      uint64
	Owner               MigrationOwner
	Phase               MigrationPhase
	PhaseDeadlineNanos  int64
	SourceSchemaEpoch   uint64
	SourceSnapshotID    uint64
	TempRelationID      uint64
	PublicationTxnID    uint64
	ReplayGeneration    uint64
	WritePermits        map[string]uint64
	ReplayTargets       map[string]uint64
	ReplayAcknowledged  map[string]uint64
	RetiredReplayTarget map[string]uint64
}

var ErrMigrationGate = migrationError("collationkey: invalid migration gate transition")

type migrationError string

func (e migrationError) Error() string { return string(e) }

type migrationWrappedError struct {
	cause   error
	message string
}

func (e migrationWrappedError) Error() string { return e.message }
func (e migrationWrappedError) Unwrap() error { return e.cause }

func wrapMigrationError(format string, args ...any) error {
	return migrationWrappedError{cause: ErrMigrationGate, message: ErrMigrationGate.Error() + ": " + fmt.Sprintf(format, args...)}
}

// NewMigrationGate creates a clean OPEN gate. relationID and schemaEpoch are
// physical identities supplied by the catalog; the gate does not invent them.
func NewMigrationGate(relationID, schemaEpoch uint64) (MigrationGate, error) {
	if relationID == 0 {
		return MigrationGate{}, wrapMigrationError("relation id is zero")
	}
	return MigrationGate{
		RelationID:          relationID,
		Phase:               MigrationOpen,
		SourceSchemaEpoch:   schemaEpoch,
		WritePermits:        make(map[string]uint64),
		ReplayTargets:       make(map[string]uint64),
		ReplayAcknowledged:  make(map[string]uint64),
		RetiredReplayTarget: make(map[string]uint64),
	}, nil
}

// Clone returns an ownership-safe copy suitable for persistence retries or
// compare-and-swap preparation.
func (g MigrationGate) Clone() MigrationGate {
	g.Owner.ClaimToken = append([]byte(nil), g.Owner.ClaimToken...)
	g.WritePermits = cloneUint64Map(g.WritePermits)
	g.ReplayTargets = cloneUint64Map(g.ReplayTargets)
	g.ReplayAcknowledged = cloneUint64Map(g.ReplayAcknowledged)
	g.RetiredReplayTarget = cloneUint64Map(g.RetiredReplayTarget)
	return g
}

func cloneUint64Map(src map[string]uint64) map[string]uint64 {
	if src == nil {
		return nil
	}
	dst := make(map[string]uint64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

// Validate checks the persisted shape and rejects partial phase records. It
// intentionally does not consult a clock or a remote membership service.
func (g MigrationGate) Validate() error {
	if g.RelationID == 0 {
		return wrapMigrationError("relation id is zero")
	}
	switch g.Phase {
	case MigrationOpen:
		if g.PhaseDeadlineNanos != 0 || g.SourceSnapshotID != 0 || g.TempRelationID != 0 ||
			g.PublicationTxnID != 0 || g.ReplayGeneration != 0 || !emptyOwner(g.Owner) ||
			len(g.ReplayTargets) != 0 || len(g.ReplayAcknowledged) != 0 || len(g.RetiredReplayTarget) != 0 {
			return wrapMigrationError("open gate carries in-flight state")
		}
	case MigrationDraining, MigrationExclusive, MigrationPublished, MigrationAborted:
		if g.MigrationEpoch == 0 {
			return wrapMigrationError("migration epoch is zero")
		}
		if !validOwner(g.Owner) {
			return wrapMigrationError("owner identity is incomplete")
		}
		if g.PhaseDeadlineNanos <= 0 {
			return wrapMigrationError("phase deadline is missing")
		}
	default:
		return wrapMigrationError("unknown phase %d", g.Phase)
	}
	if g.Phase != MigrationPublished && (g.PublicationTxnID != 0 || g.ReplayGeneration != 0 ||
		len(g.ReplayTargets) != 0 || len(g.ReplayAcknowledged) != 0 || len(g.RetiredReplayTarget) != 0) {
		return wrapMigrationError("replay state is present before publication")
	}
	if g.Phase == MigrationPublished {
		if g.PublicationTxnID == 0 || g.ReplayGeneration == 0 {
			return wrapMigrationError("published gate is missing durable identities")
		}
		for nodeID, incarnation := range g.ReplayTargets {
			if nodeID == "" || incarnation == 0 {
				return wrapMigrationError("invalid replay target %q", nodeID)
			}
		}
		for nodeID, incarnation := range g.ReplayAcknowledged {
			if g.ReplayTargets[nodeID] != incarnation {
				return wrapMigrationError("replay acknowledgement does not match target %q", nodeID)
			}
		}
		for nodeID, incarnation := range g.RetiredReplayTarget {
			if g.ReplayTargets[nodeID] != incarnation {
				return wrapMigrationError("retired replay target does not match target %q", nodeID)
			}
		}
	}
	for permitID, epoch := range g.WritePermits {
		if permitID == "" || epoch > g.MigrationEpoch {
			return wrapMigrationError("invalid write permit %q", permitID)
		}
		if g.Phase != MigrationOpen && epoch == g.MigrationEpoch {
			return wrapMigrationError("write permit %q was admitted after draining", permitID)
		}
	}
	if g.Phase == MigrationExclusive || g.Phase == MigrationPublished || g.Phase == MigrationAborted {
		if len(g.WritePermits) != 0 {
			return wrapMigrationError("phase %s still has write permits", g.Phase)
		}
	}
	return nil
}

func emptyOwner(owner MigrationOwner) bool {
	return owner.OwnerID == "" && owner.Incarnation == 0 && len(owner.ClaimToken) == 0
}

func validOwner(owner MigrationOwner) bool {
	return owner.OwnerID != "" && owner.Incarnation != 0 && len(owner.ClaimToken) != 0
}

func sameOwner(left, right MigrationOwner) bool {
	return left.OwnerID == right.OwnerID && left.Incarnation == right.Incarnation && bytes.Equal(left.ClaimToken, right.ClaimToken)
}

func checkOwner(g MigrationGate, owner MigrationOwner) error {
	if !validOwner(owner) || !sameOwner(g.Owner, owner) {
		return wrapMigrationError("stale or incomplete owner")
	}
	return nil
}

// AcquireWrite registers a write permit only while the gate is OPEN. The
// returned epoch must travel with the transaction through lock and TN commit.
func (g *MigrationGate) AcquireWrite(permitID string) (uint64, error) {
	if g == nil {
		return 0, wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return 0, err
	}
	if g.Phase != MigrationOpen {
		return 0, wrapMigrationError("writes are not admitted in phase %s", g.Phase)
	}
	if permitID == "" {
		return 0, wrapMigrationError("permit id is empty")
	}
	if _, exists := g.WritePermits[permitID]; exists {
		return 0, wrapMigrationError("permit %q already exists", permitID)
	}
	g.WritePermits[permitID] = g.MigrationEpoch
	return g.MigrationEpoch, nil
}

// CompleteWrite releases an already-admitted permit. A permit cannot be
// completed under another epoch, which is the commit-time stale-writer fence.
func (g *MigrationGate) CompleteWrite(permitID string, epoch uint64) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	want, ok := g.WritePermits[permitID]
	if !ok || want != epoch {
		return wrapMigrationError("permit %q does not match epoch %d", permitID, epoch)
	}
	delete(g.WritePermits, permitID)
	return nil
}

// BeginDraining durably fences new writes and advances the epoch. Existing
// permits remain recorded with their admission epoch and may finish before
// EXCLUSIVE is entered.
func (g *MigrationGate) BeginDraining(owner MigrationOwner, deadlineNanos int64) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Phase != MigrationOpen || !validOwner(owner) || deadlineNanos <= 0 {
		return wrapMigrationError("cannot begin draining from %s", g.Phase)
	}
	g.MigrationEpoch++
	if g.MigrationEpoch == 0 {
		return wrapMigrationError("migration epoch wrapped")
	}
	g.Owner = MigrationOwner{OwnerID: owner.OwnerID, Incarnation: owner.Incarnation, ClaimToken: append([]byte(nil), owner.ClaimToken...)}
	g.Phase = MigrationDraining
	g.PhaseDeadlineNanos = deadlineNanos
	return nil
}

// EnterExclusive is legal only after all pre-drain permits have completed.
func (g *MigrationGate) EnterExclusive(owner MigrationOwner, deadlineNanos int64) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Phase != MigrationDraining || len(g.WritePermits) != 0 || deadlineNanos <= 0 {
		return wrapMigrationError("cannot enter exclusive from %s with %d permits", g.Phase, len(g.WritePermits))
	}
	if err := checkOwner(*g, owner); err != nil {
		return err
	}
	g.Phase = MigrationExclusive
	g.PhaseDeadlineNanos = deadlineNanos
	return nil
}

// SetBuildIdentity records the post-fence snapshot and invisible temporary
// relation. It must be called before publication and cannot be changed by a
// stale owner.
func (g *MigrationGate) SetBuildIdentity(owner MigrationOwner, schemaEpoch, snapshotID, tempRelationID uint64) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Phase != MigrationExclusive || schemaEpoch != g.SourceSchemaEpoch || snapshotID == 0 || tempRelationID == 0 {
		return wrapMigrationError("invalid exclusive build identity")
	}
	if err := checkOwner(*g, owner); err != nil {
		return err
	}
	g.SourceSnapshotID = snapshotID
	g.TempRelationID = tempRelationID
	return nil
}

// Publish is the irreversible catalog linearization point. Replay targets are
// copied into the gate and must acknowledge the same generation before the
// write fence can be released.
func (g *MigrationGate) Publish(owner MigrationOwner, publicationTxnID, replayGeneration uint64, replayTargets map[string]uint64, deadlineNanos int64) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Phase != MigrationExclusive || g.SourceSnapshotID == 0 || g.TempRelationID == 0 ||
		publicationTxnID == 0 || replayGeneration == 0 || deadlineNanos <= 0 {
		return wrapMigrationError("publication identity is incomplete")
	}
	if err := checkOwner(*g, owner); err != nil {
		return err
	}
	if err := validateReplayTargets(replayTargets); err != nil {
		return err
	}
	g.Phase = MigrationPublished
	g.PublicationTxnID = publicationTxnID
	g.ReplayGeneration = replayGeneration
	g.ReplayTargets = cloneUint64Map(replayTargets)
	g.ReplayAcknowledged = make(map[string]uint64)
	g.RetiredReplayTarget = make(map[string]uint64)
	g.PhaseDeadlineNanos = deadlineNanos
	return nil
}

func validateReplayTargets(targets map[string]uint64) error {
	if len(targets) == 0 {
		return wrapMigrationError("replay target set is empty")
	}
	for nodeID, incarnation := range targets {
		if nodeID == "" || incarnation == 0 {
			return wrapMigrationError("invalid replay target %q", nodeID)
		}
	}
	return nil
}

// AbortBeforePublication preserves the old relation. An EXCLUSIVE gate must
// have no outstanding permit; a PUBLISHED gate can never be aborted.
func (g *MigrationGate) AbortBeforePublication(owner MigrationOwner, deadlineNanos int64) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Phase != MigrationDraining && g.Phase != MigrationExclusive || len(g.WritePermits) != 0 || deadlineNanos <= 0 {
		return wrapMigrationError("cannot abort migration in phase %s", g.Phase)
	}
	if err := checkOwner(*g, owner); err != nil {
		return err
	}
	g.Phase = MigrationAborted
	g.PhaseDeadlineNanos = deadlineNanos
	return nil
}

// ClaimExpired replaces an expired owner with a new incarnation/token. The
// caller supplies the observed time; no wall clock is hidden in this package.
func (g *MigrationGate) ClaimExpired(nowNanos int64, owner MigrationOwner) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Phase == MigrationOpen || nowNanos < g.PhaseDeadlineNanos || !validOwner(owner) || sameOwner(g.Owner, owner) {
		return wrapMigrationError("expired claim is not eligible")
	}
	g.Owner = MigrationOwner{OwnerID: owner.OwnerID, Incarnation: owner.Incarnation, ClaimToken: append([]byte(nil), owner.ClaimToken...)}
	return nil
}

func (g *MigrationGate) acknowledgeReplay(owner MigrationOwner, nodeID string, incarnation uint64) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Phase != MigrationPublished {
		return wrapMigrationError("replay acknowledgement requires published owner")
	}
	if err := checkOwner(*g, owner); err != nil {
		return err
	}
	want, ok := g.ReplayTargets[nodeID]
	if !ok || want != incarnation {
		return wrapMigrationError("replay target %q incarnation mismatch", nodeID)
	}
	g.ReplayAcknowledged[nodeID] = incarnation
	return nil
}

// AcknowledgeReplay records a target's exact incarnation for the published
// generation. The generation argument is part of the stale-replay fence.
func (g *MigrationGate) AcknowledgeReplay(owner MigrationOwner, generation uint64, nodeID string, incarnation uint64) error {
	if generation != g.ReplayGeneration {
		return wrapMigrationError("replay generation mismatch")
	}
	return g.acknowledgeReplay(owner, nodeID, incarnation)
}

// RetireReplayTarget fences a missing target instead of treating it as an
// implicit acknowledgement. The membership/recovery owner decides when this
// is safe; this method only enforces the persisted identity checks.
func (g *MigrationGate) RetireReplayTarget(owner MigrationOwner, generation uint64, nodeID string, incarnation uint64) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if generation != g.ReplayGeneration {
		return wrapMigrationError("replay generation mismatch")
	}
	if err := g.acknowledgeReplay(owner, nodeID, incarnation); err != nil {
		// A retirement is allowed for a target that has not acknowledged yet.
		if g.Phase != MigrationPublished || checkOwner(*g, owner) != nil {
			return err
		}
		if want, ok := g.ReplayTargets[nodeID]; !ok || want != incarnation {
			return err
		}
	}
	g.RetiredReplayTarget[nodeID] = incarnation
	return nil
}

// ReleaseAfterReplay opens the gate for the already-published relation only
// after every target is acknowledged or explicitly retired. It clears
// temporary/replay bookkeeping but retains the monotonic migration epoch.
func (g *MigrationGate) ReleaseAfterReplay(owner MigrationOwner) error {
	if g == nil {
		return wrapMigrationError("nil gate")
	}
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Phase != MigrationPublished {
		return wrapMigrationError("cannot release phase %s", g.Phase)
	}
	if err := checkOwner(*g, owner); err != nil {
		return err
	}
	for nodeID := range g.ReplayTargets {
		if _, ok := g.ReplayAcknowledged[nodeID]; ok {
			continue
		}
		if _, ok := g.RetiredReplayTarget[nodeID]; !ok {
			return wrapMigrationError("replay target %q is unresolved", nodeID)
		}
	}
	g.Phase = MigrationOpen
	g.Owner = MigrationOwner{}
	g.PhaseDeadlineNanos = 0
	g.SourceSnapshotID = 0
	g.TempRelationID = 0
	g.PublicationTxnID = 0
	g.ReplayGeneration = 0
	g.ReplayTargets = make(map[string]uint64)
	g.ReplayAcknowledged = make(map[string]uint64)
	g.RetiredReplayTarget = make(map[string]uint64)
	return nil
}

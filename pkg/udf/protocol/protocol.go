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

// Package protocol contains the transport-independent contracts shared by the
// Python UDF gateway and its callers.  It deliberately does not know about
// gRPC, Arrow, or a particular scheduler implementation.
package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// Version identifies the control envelope understood by this runtime.  It is
	// a wire compatibility field, not a language or package name.
	Version = 1

	// MaxControlBytes is a trust-boundary ceiling.  A runtime may negotiate a
	// smaller limit, but a caller must never silently raise it.
	MaxControlBytes = 1 << 20
)

var (
	ErrProtocol        = errors.New("python udf protocol violation")
	ErrSequence        = errors.New("python udf sequence violation")
	ErrGroupClosed     = errors.New("python udf execution group is closed")
	ErrDuplicate       = errors.New("python udf identity is already present")
	ErrLedgerFull      = errors.New("python udf terminal ledger is full")
	ErrUnknownIdentity = errors.New("python udf identity is unknown")
)

// FencingTuple is carried by every invocation control and data-plane
// application message.  IDs are never reused while their fence/tombstone is
// retained.  The tuple is a deduplication fence, not an authentication token.
type FencingTuple struct {
	AccountID    uint64 `json:"account_id"`
	StatementID  string `json:"statement_id"`
	GroupID      string `json:"group_id"`
	GroupEpoch   uint64 `json:"group_epoch"`
	InvocationID string `json:"invocation_id"`
	LeaseEpoch   uint64 `json:"lease_epoch"`
}

func (t FencingTuple) Validate() error {
	if t.AccountID == 0 || t.StatementID == "" || t.GroupID == "" ||
		t.GroupEpoch == 0 || t.InvocationID == "" || t.LeaseEpoch == 0 {
		return fmt.Errorf("%w: incomplete fencing tuple", ErrProtocol)
	}
	return nil
}

// Control is the small, canonical application envelope carried in Flight
// app_metadata.  Payload is a versioned descriptor or error body owned by the
// message kind; it is bounded before JSON decoding.
type Control struct {
	Version      int             `json:"version"`
	Kind         string          `json:"kind"`
	Tuple        FencingTuple    `json:"tuple"`
	Sequence     uint64          `json:"sequence,omitempty"`
	LastSequence uint64          `json:"last_sequence,omitempty"`
	AckSequence  uint64          `json:"ack_sequence,omitempty"`
	FinishID     string          `json:"finish_id,omitempty"`
	Status       string          `json:"status,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

// MarshalControl produces the byte representation used in the Flight
// application metadata field.  Struct-field order is intentional: the same
// bytes are used when a descriptor fingerprint is calculated.
func MarshalControl(control Control) ([]byte, error) {
	if control.Version == 0 {
		control.Version = Version
	}
	if control.Version != Version || control.Kind == "" {
		return nil, fmt.Errorf("%w: unsupported control version or empty kind", ErrProtocol)
	}
	if err := control.Tuple.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(control)
	if err != nil {
		return nil, fmt.Errorf("%w: encode control: %v", ErrProtocol, err)
	}
	if len(encoded) > MaxControlBytes {
		return nil, fmt.Errorf("%w: control is %d bytes, limit is %d", ErrProtocol, len(encoded), MaxControlBytes)
	}
	return encoded, nil
}

func UnmarshalControl(data []byte) (Control, error) {
	if len(data) == 0 || len(data) > MaxControlBytes {
		return Control{}, fmt.Errorf("%w: control size %d is outside the allowed range", ErrProtocol, len(data))
	}
	var control Control
	if err := json.Unmarshal(data, &control); err != nil {
		return Control{}, fmt.Errorf("%w: decode control: %v", ErrProtocol, err)
	}
	if control.Version != Version || control.Kind == "" {
		return Control{}, fmt.Errorf("%w: unsupported control version %d", ErrProtocol, control.Version)
	}
	if err := control.Tuple.Validate(); err != nil {
		return Control{}, err
	}
	return control, nil
}

// Sequence validates the independent input and result directions of one
// invocation.  EndInput is a normal half-close; it does not make results
// terminal until all results have been acknowledged.
type Sequence struct {
	mu          sync.Mutex
	nextInput   uint64
	lastInput   uint64
	inputEnded  bool
	nextResult  uint64
	lastResult  uint64
	ackedResult uint64
}

func (s *Sequence) AcceptInput(sequence uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inputEnded {
		return fmt.Errorf("%w: input arrived after EndInput", ErrSequence)
	}
	if sequence != s.nextInput+1 {
		return fmt.Errorf("%w: input sequence %d, expected %d", ErrSequence, sequence, s.nextInput+1)
	}
	s.nextInput = sequence
	s.lastInput = sequence
	return nil
}

func (s *Sequence) EndInput(lastSequence uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inputEnded {
		if s.lastInput == lastSequence {
			return nil
		}
		return fmt.Errorf("%w: EndInput changed from %d to %d", ErrSequence, s.lastInput, lastSequence)
	}
	if lastSequence != s.lastInput {
		return fmt.Errorf("%w: EndInput last sequence %d, observed %d", ErrSequence, lastSequence, s.lastInput)
	}
	s.inputEnded = true
	return nil
}

func (s *Sequence) AcceptResult(sequence uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sequence != s.nextResult+1 || sequence > s.lastInput {
		return fmt.Errorf("%w: result sequence %d, expected %d and at most input %d", ErrSequence, sequence, s.nextResult+1, s.lastInput)
	}
	s.nextResult = sequence
	s.lastResult = sequence
	return nil
}

func (s *Sequence) AcknowledgeResults(sequence uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sequence < s.ackedResult || sequence > s.lastResult {
		return fmt.Errorf("%w: result ACK %d outside [%d,%d]", ErrSequence, sequence, s.ackedResult, s.lastResult)
	}
	s.ackedResult = sequence
	return nil
}

func (s *Sequence) ReadyToFinish() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inputEnded && s.lastResult == s.lastInput && s.ackedResult == s.lastResult
}

func (s *Sequence) State() (lastInput, lastResult, acked uint64, inputEnded bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastInput, s.lastResult, s.ackedResult, s.inputEnded
}

// OutputSnapshot is the only representation that may cross the trusted
// validation/publication boundary.  Freeze copies the worker-owned bytes
// before validation.  The snapshot's backing is private, so later worker
// writes cannot change the bytes validated or published by the caller.
type OutputSnapshot struct {
	backing []byte
	digest  [sha256.Size]byte
}

func FreezeOutput(source []byte, maxBytes int64) (*OutputSnapshot, error) {
	if maxBytes <= 0 || int64(len(source)) > maxBytes {
		return nil, fmt.Errorf("%w: output snapshot size %d exceeds limit %d", ErrProtocol, len(source), maxBytes)
	}
	backing := make([]byte, len(source))
	copy(backing, source)
	return &OutputSnapshot{backing: backing, digest: sha256.Sum256(backing)}, nil
}

func (s *OutputSnapshot) Len() int {
	if s == nil {
		return 0
	}
	return len(s.backing)
}

func (s *OutputSnapshot) Digest() string {
	if s == nil {
		return ""
	}
	return hex.EncodeToString(s.digest[:])
}

func (s *OutputSnapshot) Validate(expectedLength int, expectedDigest string) error {
	if s == nil || s.backing == nil {
		return fmt.Errorf("%w: missing output snapshot", ErrProtocol)
	}
	if expectedLength >= 0 && len(s.backing) != expectedLength {
		return fmt.Errorf("%w: output snapshot length %d, expected %d", ErrProtocol, len(s.backing), expectedLength)
	}
	if expectedDigest != "" && !equalFoldHex(expectedDigest, s.Digest()) {
		return fmt.Errorf("%w: output snapshot digest changed", ErrProtocol)
	}
	return nil
}

// Bytes returns a private copy for an API that cannot consume an immutable
// view.  Validation and publication in a trusted consumer should operate on
// the same snapshot object, rather than refetching the worker buffer.
func (s *OutputSnapshot) Bytes() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.backing...)
}

// TrustedBytes exposes the private frozen backing to a validator/consumer
// that is part of the same trusted boundary.  The caller must treat the
// returned slice as read-only.  It exists so validation and decoding can use
// exactly the bytes that were frozen, rather than refetching worker memory or
// making a second mutable copy between the two steps.
func (s *OutputSnapshot) TrustedBytes() []byte {
	if s == nil {
		return nil
	}
	return s.backing
}

func equalFoldHex(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		l, r := left[i], right[i]
		if l >= 'A' && l <= 'F' {
			l += 'a' - 'A'
		}
		if r >= 'A' && r <= 'F' {
			r += 'a' - 'A'
		}
		if l != r {
			return false
		}
	}
	return true
}

type GroupState string

const (
	GroupReserved GroupState = "RESERVED"
	GroupOpen     GroupState = "OPEN"
	GroupDraining GroupState = "DRAINING"
	GroupReleased GroupState = "RELEASED"
)

// CloseReason is deliberately a string so the scheduler can add a reason in
// a newer minor protocol without changing the lifetime rules.
type CloseReason string

const (
	ReasonInputEOF         CloseReason = "INPUT_EOF"
	ReasonEmptyInput       CloseReason = "EMPTY_INPUT"
	ReasonAllNull          CloseReason = "ALL_NULL"
	ReasonNoSelectedRows   CloseReason = "NO_SELECTED_ROWS"
	ReasonPartialOpenError CloseReason = "PARTIAL_OPEN_FAILURE"
	ReasonCancel           CloseReason = "CANCEL"
	ReasonIdle             CloseReason = "IDLE"
)

// ExecutionGroup is a one-shot admission cohort.  Member terminal cleanup
// only removes that member.  The release callback belongs to the group owner
// and runs after close, all members terminal, and all in-flight opens finish.
type ExecutionGroup struct {
	mu          sync.Mutex
	id          string
	epoch       uint64
	maxMembers  int
	registered  int
	inFlight    int
	members     map[string]bool
	closing     bool
	releaseBusy bool
	released    bool
	release     func() error
	reason      CloseReason
}

func NewExecutionGroup(id string, epoch uint64, maxMembers int, release func() error) (*ExecutionGroup, error) {
	if id == "" || epoch == 0 || maxMembers <= 0 || release == nil {
		return nil, fmt.Errorf("%w: invalid execution group", ErrProtocol)
	}
	return &ExecutionGroup{
		id: id, epoch: epoch, maxMembers: maxMembers,
		members: make(map[string]bool), release: release,
	}, nil
}

// BeginOpen reserves one cumulative member-registration slot.  The returned
// token must be committed or aborted exactly once by the Open owner.
func (g *ExecutionGroup) BeginOpen(memberID string) (*OpenToken, error) {
	if memberID == "" {
		return nil, fmt.Errorf("%w: empty member id", ErrProtocol)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing || g.released {
		return nil, ErrGroupClosed
	}
	if g.members[memberID] {
		return nil, ErrDuplicate
	}
	if g.registered+g.inFlight >= g.maxMembers {
		return nil, fmt.Errorf("%w: member limit %d reached", ErrLedgerFull, g.maxMembers)
	}
	g.inFlight++
	return &OpenToken{group: g, memberID: memberID}, nil
}

type OpenToken struct {
	group    *ExecutionGroup
	memberID string
	once     sync.Once
}

func (t *OpenToken) Commit() error {
	if t == nil || t.group == nil {
		return ErrUnknownIdentity
	}
	var err error
	var release bool
	t.once.Do(func() {
		g := t.group
		g.mu.Lock()
		g.inFlight--
		if g.closing || g.released {
			err = ErrGroupClosed
			release = g.canReleaseLocked()
			g.mu.Unlock()
			return
		}
		g.members[t.memberID] = true
		g.registered++
		g.mu.Unlock()
	})
	if release {
		if releaseErr := t.group.releaseIfReady(); err == nil {
			err = releaseErr
		}
	}
	return err
}

func (t *OpenToken) Abort() error {
	if t == nil || t.group == nil {
		return ErrUnknownIdentity
	}
	var release bool
	t.once.Do(func() {
		g := t.group
		g.mu.Lock()
		g.inFlight--
		release = g.canReleaseLocked()
		g.mu.Unlock()
	})
	if release {
		return t.group.releaseIfReady()
	}
	return nil
}

func (g *ExecutionGroup) Close(reason CloseReason) error {
	if reason == "" {
		return fmt.Errorf("%w: close reason is empty", ErrProtocol)
	}
	g.mu.Lock()
	if g.released {
		g.mu.Unlock()
		return nil
	}
	if !g.closing {
		g.closing = true
		g.reason = reason
	}
	release := g.canReleaseLocked()
	g.mu.Unlock()
	if release {
		return g.releaseIfReady()
	}
	return nil
}

func (g *ExecutionGroup) MemberTerminal(memberID string) error {
	g.mu.Lock()
	if !g.members[memberID] {
		g.mu.Unlock()
		return ErrUnknownIdentity
	}
	delete(g.members, memberID)
	release := g.canReleaseLocked()
	g.mu.Unlock()
	if release {
		return g.releaseIfReady()
	}
	return nil
}

func (g *ExecutionGroup) canReleaseLocked() bool {
	return g.closing && len(g.members) == 0 && g.inFlight == 0 && !g.released && !g.releaseBusy
}

func (g *ExecutionGroup) releaseIfReady() error {
	g.mu.Lock()
	if !g.canReleaseLocked() {
		g.mu.Unlock()
		return nil
	}
	g.releaseBusy = true
	g.mu.Unlock()
	err := g.release()
	g.mu.Lock()
	g.releaseBusy = false
	if err == nil {
		g.released = true
	}
	g.mu.Unlock()
	return err
}

func (g *ExecutionGroup) State() GroupState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return GroupReleased
	}
	if g.closing {
		return GroupDraining
	}
	if g.registered > 0 {
		return GroupOpen
	}
	return GroupReserved
}

func (g *ExecutionGroup) Reason() CloseReason {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reason
}

func (g *ExecutionGroup) Counts() (registered, active, inFlight int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.registered, len(g.members), g.inFlight
}

type ledgerEntry struct {
	key       string
	bytes     int64
	expiresAt time.Time
	tombstone bool
}

// TerminalLedger is an admission-reserved bounded deduplication ledger.  A
// terminal entry is retained until expiry; it is never deleted merely to make
// room for a new invocation.
type TerminalLedger struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	entries    map[string]ledgerEntry
	reservedN  int
	reservedB  int64
}

type LedgerCredit struct {
	ledger     *TerminalLedger
	groupID    string
	remainingN int
	remainingB int64
	closed     bool
}

func NewTerminalLedger(maxEntries int, maxBytes int64) (*TerminalLedger, error) {
	if maxEntries <= 0 || maxBytes <= 0 {
		return nil, fmt.Errorf("%w: invalid terminal ledger limit", ErrProtocol)
	}
	return &TerminalLedger{maxEntries: maxEntries, maxBytes: maxBytes, entries: make(map[string]ledgerEntry)}, nil
}

func (l *TerminalLedger) Reserve(groupID string, entries int, bytes int64) (*LedgerCredit, error) {
	if groupID == "" || entries <= 0 || bytes <= 0 {
		return nil, fmt.Errorf("%w: invalid terminal ledger reservation", ErrProtocol)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reservedN > l.maxEntries-entries || l.reservedB > l.maxBytes-bytes {
		return nil, ErrLedgerFull
	}
	l.reservedN += entries
	l.reservedB += bytes
	return &LedgerCredit{ledger: l, groupID: groupID, remainingN: entries, remainingB: bytes}, nil
}

func (c *LedgerCredit) Add(key string, bytes int64, expiry time.Time) error {
	if c == nil || c.ledger == nil || key == "" || bytes <= 0 || expiry.IsZero() {
		return fmt.Errorf("%w: invalid terminal ledger entry", ErrProtocol)
	}
	c.ledger.mu.Lock()
	defer c.ledger.mu.Unlock()
	if c.closed || c.remainingN == 0 || c.remainingB < bytes {
		return ErrLedgerFull
	}
	if _, exists := c.ledger.entries[key]; exists {
		return ErrDuplicate
	}
	c.ledger.entries[key] = ledgerEntry{key: key, bytes: bytes, expiresAt: expiry}
	c.remainingN--
	c.remainingB -= bytes
	return nil
}

func (c *LedgerCredit) ReleaseUnused() {
	if c == nil || c.ledger == nil {
		return
	}
	c.ledger.mu.Lock()
	defer c.ledger.mu.Unlock()
	if c.closed {
		return
	}
	c.ledger.reservedN -= c.remainingN
	c.ledger.reservedB -= c.remainingB
	c.remainingN = 0
	c.remainingB = 0
	c.closed = true
}

func (l *TerminalLedger) Complete(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.entries[key]
	if !ok {
		return ErrUnknownIdentity
	}
	if entry.tombstone {
		return nil
	}
	entry.tombstone = true
	l.entries[key] = entry
	return nil
}

func (l *TerminalLedger) Expire(now time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := 0
	for key, entry := range l.entries {
		if !now.Before(entry.expiresAt) {
			delete(l.entries, key)
			l.reservedN--
			l.reservedB -= entry.bytes
			removed++
		}
	}
	return removed
}

func (l *TerminalLedger) Counts() (entries int, bytes int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries), l.reservedB
}

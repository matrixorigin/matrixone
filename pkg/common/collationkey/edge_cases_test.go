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
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestCodecRejectsUnsupportedDomainsAndPreservesDestination(t *testing.T) {
	badDomains := []Domain{
		{Type: Text, Charset: CharsetUTF8, Unit: PrefixBytes},
		{Type: Text, Charset: 99, Unit: PrefixCharacters},
		{Type: Binary, Charset: 99, Unit: PrefixBytes},
		{Type: Binary, Charset: CharsetBinary, Unit: PrefixCharacters},
		{Type: SignedInteger, Charset: CharsetUTF8, Width: 1},
		{Type: SignedInteger, Width: 0},
		{Type: UnsignedInteger, Prefix: 1, Width: 1},
		{Type: UnsignedInteger, Width: 0},
		{Type: Decimal, Charset: CharsetUTF8, Width: 1, Scale: 1},
		{Type: Decimal, Width: 0, Scale: 1},
		{Type: Decimal, Width: 1, Unit: PrefixBytes, Scale: 1},
		{Type: TypeFamily(99)},
	}
	for _, domain := range badDomains {
		dst := []byte("prefix")
		got, err := EncodePart(dst, Part{Domain: domain, Value: []byte("1")})
		if err == nil {
			t.Fatalf("domain %+v was accepted", domain)
		}
		if !bytes.Equal(got, dst) {
			t.Fatalf("failed encode changed destination for %+v: %X", domain, got)
		}
	}
	if _, err := EncodePart(nil, Part{Domain: generalDomain(0), Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodePart(nil, Part{Domain: binaryDomain(2), Value: []byte("abcd")}); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodePart(nil, Part{Domain: generalDomain(99), Value: []byte("a")}); err != nil {
		t.Fatal(err)
	}
}

func TestCodecValidationAndPrefixBoundaries(t *testing.T) {
	first, err := EncodePart(nil, Part{Domain: generalDomain(0), Value: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodePart(nil, Part{Domain: generalDomain(0), Value: []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	composite, err := EncodeComposite(nil, []Part{
		{Domain: generalDomain(0), Value: []byte("a")},
		{Domain: generalDomain(0), Null: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if hasNull, err := HasNullPart(composite); err != nil || !hasNull {
		t.Fatalf("second NULL part = %v, %v", hasNull, err)
	}
	zeroParts := append([]byte(nil), first...)
	binary.BigEndian.PutUint16(zeroParts[5:7], 0)
	if err := ValidateEncoded(zeroParts); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("zero parts error = %v", err)
	}
	tooMany := append([]byte(nil), first...)
	binary.BigEndian.PutUint16(tooMany[5:7], MaxParts+1)
	if err := ValidateEncoded(tooMany); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("too many parts error = %v", err)
	}
	truncated := append([]byte(nil), first[:7]...)
	if err := ValidateEncoded(truncated); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("truncated part error = %v", err)
	}
	badWeight := append([]byte(nil), first...)
	badWeight[len(badWeight)-4] = 0x01
	if err := ValidateEncoded(badWeight); err == nil {
		t.Fatal("weight larger than uint16 was accepted")
	}
	trailingSpace := append([]byte(nil), first...)
	trailingSpace[len(trailingSpace)-4] = 0
	trailingSpace[len(trailingSpace)-3] = 0
	trailingSpace[len(trailingSpace)-2] = 0
	trailingSpace[len(trailingSpace)-1] = ' '
	if err := ValidateEncoded(trailingSpace); err == nil {
		t.Fatal("general-ci trailing space weight was accepted")
	}
	badInteger, err := EncodePart(nil, Part{Domain: Domain{Type: SignedInteger, Width: 2}, Value: []byte{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	badInteger = append(badInteger, 0)
	if err := ValidateEncoded(badInteger); err == nil {
		t.Fatal("integer trailing payload was accepted")
	}
	if _, err := EncodeComposite(make([]byte, MaxKeyBytes-6), []Part{{Domain: generalDomain(0), Value: []byte("x")}}); err == nil {
		t.Fatal("destination exceeding the key limit was accepted")
	}
	if _, err := EncodeComposite(nil, []Part{}); err == nil {
		t.Fatal("empty composite was accepted")
	}
	if equal, err := Equal(Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters}, []byte{0xff}, []byte("a")); err == nil || equal {
		t.Fatalf("invalid equality input = %v, %v", equal, err)
	}
	if _, err := HasNullPart([]byte("bad")); !errors.Is(err, ErrMalformedKey) {
		t.Fatalf("malformed null probe = %v", err)
	}
	if _, err := EncodePart(nil, Part{Domain: generalDomain(100), Value: []byte("short")}); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodePart(nil, Part{Domain: Domain{Type: Text, Charset: CharsetUTF8, Unit: PrefixCharacters, Prefix: 2}, Value: []byte("a😀")}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, first) || bytes.Equal(first, second) {
		t.Fatal("sanity check failed")
	}
}

func TestCodecDecimalMalformedPayloadsAndSyntax(t *testing.T) {
	decimal := Domain{Type: Decimal, Width: 8, Scale: 2}
	for _, input := range []string{"", "+", "-", "1.2.3", "x", ".x"} {
		if _, err := EncodePart(nil, Part{Domain: decimal, Value: []byte(input)}); err == nil {
			t.Fatalf("decimal %q was accepted", input)
		}
	}
	if _, err := EncodePart(nil, Part{Domain: decimal, Value: []byte(strings.Repeat("1", 32768))}); err == nil {
		t.Fatal("decimal with an excessive input scale was accepted")
	}
	valid, err := EncodePart(nil, Part{Domain: decimal, Value: []byte("1.20")})
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte){
		"short payload":            func(key []byte) { key[18] = 0; key[19] = 0; key[20] = 0; key[21] = 8 },
		"negative scale":           func(key []byte) { key[24] = 0xff; key[25] = 0xff; key[26] = 0xff; key[27] = 0xff },
		"leading coefficient zero": func(key []byte) { key[len(key)-1] = key[len(key)-2]; key[len(key)-2] = 0 },
		"nonminimal coefficient":   func(key []byte) { key[len(key)-1] = 0 },
	} {
		key := append([]byte(nil), valid...)
		mutate(key)
		if err := ValidateEncoded(key); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestMigrationGateRejectsInvalidTransitions(t *testing.T) {
	if _, err := NewMigrationGate(0, 1); err == nil {
		t.Fatal("zero relation gate was accepted")
	}
	gate, err := NewMigrationGate(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.AcquireWrite(""); err == nil {
		t.Fatal("empty write permit was accepted")
	}
	if _, err := gate.AcquireWrite("p"); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.AcquireWrite("p"); err == nil {
		t.Fatal("duplicate write permit was accepted")
	}
	if err := gate.CompleteWrite("p", 99); err == nil {
		t.Fatal("wrong permit epoch was accepted")
	}
	if err := gate.CompleteWrite("missing", 0); err == nil {
		t.Fatal("missing permit was accepted")
	}
	if err := gate.BeginDraining(MigrationOwner{}, 1); err == nil {
		t.Fatal("incomplete owner began draining")
	}
	owner := migrationTestOwner("owner", 1)
	if err := gate.BeginDraining(owner, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.AcquireWrite("after"); err == nil {
		t.Fatal("write admitted after draining")
	}
	if err := gate.EnterExclusive(migrationTestOwner("other", 2), 20); err == nil {
		t.Fatal("stale owner entered exclusive")
	}
	if err := gate.EnterExclusive(owner, 0); err == nil {
		t.Fatal("zero exclusive deadline accepted")
	}
	if err := gate.CompleteWrite("p", 0); err != nil {
		t.Fatal(err)
	}
	if err := gate.EnterExclusive(owner, 20); err != nil {
		t.Fatal(err)
	}
	if err := gate.SetBuildIdentity(owner, 99, 1, 1); err == nil {
		t.Fatal("wrong schema epoch accepted")
	}
	if err := gate.SetBuildIdentity(owner, 2, 0, 1); err == nil {
		t.Fatal("zero snapshot accepted")
	}
	if err := gate.SetBuildIdentity(migrationTestOwner("other", 2), 2, 1, 1); err == nil {
		t.Fatal("stale build owner accepted")
	}
	if err := gate.SetBuildIdentity(owner, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := gate.AcknowledgeReplay(owner, 0, "tn", 1); err == nil {
		t.Fatal("replay acknowledgement accepted before publication")
	}
	if err := gate.Publish(owner, 0, 1, map[string]uint64{"tn": 1}, 30); err == nil {
		t.Fatal("zero publication id accepted")
	}
	if err := gate.Publish(owner, 1, 1, nil, 30); err == nil {
		t.Fatal("empty replay targets accepted")
	}
	if err := gate.Publish(owner, 1, 1, map[string]uint64{"": 1}, 30); err == nil {
		t.Fatal("empty replay target accepted")
	}
	if err := gate.Publish(owner, 1, 1, map[string]uint64{"tn": 1}, 30); err != nil {
		t.Fatal(err)
	}
	if err := gate.AcknowledgeReplay(migrationTestOwner("other", 2), 1, "tn", 1); err == nil {
		t.Fatal("stale replay owner accepted")
	}
	if err := gate.AcknowledgeReplay(owner, 2, "tn", 1); err == nil {
		t.Fatal("stale replay generation accepted")
	}
	if err := gate.AcknowledgeReplay(owner, 1, "missing", 1); err == nil {
		t.Fatal("unknown replay target accepted")
	}
	if err := gate.ReleaseAfterReplay(owner); err == nil {
		t.Fatal("unacknowledged replay target released")
	}
	if err := gate.RetireReplayTarget(owner, 1, "tn", 2); err == nil {
		t.Fatal("wrong replay incarnation retired")
	}
	if err := gate.RetireReplayTarget(owner, 1, "tn", 1); err != nil {
		t.Fatal(err)
	}
	if err := gate.ReleaseAfterReplay(owner); err != nil {
		t.Fatal(err)
	}
	if err := gate.ReleaseAfterReplay(owner); err == nil {
		t.Fatal("open gate released twice")
	}
	var nilGate *MigrationGate
	if _, err := nilGate.AcquireWrite("x"); err == nil {
		t.Fatal("nil gate admitted write")
	}
	if err := nilGate.CompleteWrite("x", 0); err == nil {
		t.Fatal("nil gate completed write")
	}
}

func TestMigrationGateValidationCoversPersistedShapeErrors(t *testing.T) {
	base, err := NewMigrationGate(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	cases := []func(*MigrationGate){
		func(g *MigrationGate) { g.RelationID = 0 },
		func(g *MigrationGate) { g.Phase = MigrationPhase(99) },
		func(g *MigrationGate) { g.Phase = MigrationDraining },
		func(g *MigrationGate) {
			g.Phase = MigrationDraining
			g.MigrationEpoch = 1
			g.Owner = migrationTestOwner("x", 1)
		},
		func(g *MigrationGate) {
			g.Phase = MigrationExclusive
			g.MigrationEpoch = 1
			g.Owner = migrationTestOwner("x", 1)
			g.PhaseDeadlineNanos = 1
			g.WritePermits["late"] = 1
		},
		func(g *MigrationGate) { g.Phase = MigrationOpen; g.PhaseDeadlineNanos = 1 },
	}
	for i, mutate := range cases {
		g := base.Clone()
		mutate(&g)
		if err := g.Validate(); err == nil {
			t.Fatalf("invalid persisted gate %d was accepted", i)
		}
	}
}

func TestSidecarClosedAndRestoreBoundaries(t *testing.T) {
	if _, err := NewSidecarStore(0, NewCollationAwareMetadata()); err == nil {
		t.Fatal("zero sidecar relation was accepted")
	}
	var nilStore *SidecarStore
	if _, err := nilStore.Begin(Admission{}); err == nil {
		t.Fatal("nil sidecar began transaction")
	}
	if got := nilStore.Metadata(); !got.IsLegacy() {
		t.Fatalf("nil store metadata = %+v", got)
	}
	if got := nilStore.Snapshot(); got.RelationID != 0 {
		t.Fatalf("nil store snapshot = %+v", got)
	}
	store, err := NewSidecarStore(8, NewCollationAwareMetadataAtGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Begin(sidecarTestAdmission(2))
	if err != nil {
		t.Fatal(err)
	}
	key := sidecarTestKey(t)
	if err := tx.Put(key, RowLocator{RelationID: 9}); !errors.Is(err, ErrSidecarRelation) {
		t.Fatalf("wrong relation put = %v", err)
	}
	if err := tx.Put([]byte("bad"), RowLocator{RelationID: 8}); err == nil {
		t.Fatal("malformed key put was accepted")
	}
	if err := tx.Delete([]byte("bad"), nil); err == nil {
		t.Fatal("malformed key delete was accepted")
	}
	if _, _, err := tx.Lookup([]byte("bad")); err == nil {
		t.Fatal("malformed key lookup was accepted")
	}
	if err := tx.Put(key, RowLocator{RelationID: 8}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrSidecarClosed) {
		t.Fatalf("second commit = %v", err)
	}
	if err := tx.Put(key, RowLocator{RelationID: 8}); !errors.Is(err, ErrSidecarClosed) {
		t.Fatalf("closed put = %v", err)
	}
	if _, _, err := tx.Lookup(key); !errors.Is(err, ErrSidecarClosed) {
		t.Fatalf("closed lookup = %v", err)
	}
	if err := tx.Delete(key, nil); !errors.Is(err, ErrSidecarClosed) {
		t.Fatalf("closed delete = %v", err)
	}
	if err := tx.Delete(key, &RowLocator{RelationID: 8, PrimaryKey: []byte("wrong")}); err == nil {
		t.Fatal("closed expected delete unexpectedly succeeded")
	}
	badSnapshot := store.Snapshot()
	badSnapshot.Revision = 0
	if _, err := RestoreSidecarStore(badSnapshot); err == nil {
		t.Fatal("non-empty zero-revision snapshot was accepted")
	}
	unsorted := store.Snapshot()
	unsorted.Entries = append(unsorted.Entries, cloneSidecarEntry(unsorted.Entries[0]))
	if _, err := RestoreSidecarStore(unsorted); err == nil {
		t.Fatal("non-strict snapshot ordering was accepted")
	}
	badLocator := store.Snapshot()
	badLocator.Entries[0].Locator.RelationID++
	if _, err := RestoreSidecarStore(badLocator); !errors.Is(err, ErrSidecarRelation) {
		t.Fatalf("mixed locator snapshot = %v", err)
	}
	var nilTx *SidecarTxn
	nilTx.Rollback()
}

func TestAdmissionContextAndActivationBoundaryErrors(t *testing.T) {
	if _, ok := AdmissionFromContext(nil); ok {
		t.Fatal("nil context unexpectedly carried admission")
	}
	ctx := WithAdmission(nil, Admission{})
	if _, ok := AdmissionFromContext(ctx); ok {
		t.Fatal("malformed admission was attached")
	}
	activation := NewActivationRequest(3, map[string]uint64{"cn": 1}, map[string]uint64{"tn": 1})
	if _, err := activation.Advance(ActivationEnabled); err == nil {
		t.Fatal("preparing activation bypassed acknowledgements")
	}
	if _, err := activation.Enable(nil, nil); err == nil {
		t.Fatal("incomplete activation was enabled")
	}
	badPhase := activation
	badPhase.Phase = ActivationPhase(99)
	if err := badPhase.Validate(); err == nil {
		t.Fatal("unknown activation phase accepted")
	}
	badTarget := activation
	badTarget.CnTargets = map[string]uint64{"": 1}
	if err := badTarget.Validate(); err == nil {
		t.Fatal("empty activation target accepted")
	}
	badTarget = activation
	badTarget.TnTargets = map[string]uint64{"tn": 0}
	if err := badTarget.Validate(); err == nil {
		t.Fatal("zero activation incarnation accepted")
	}
	if activation.NodeReady(NodeCN, NodeAcknowledgement{}) {
		t.Fatal("empty node acknowledgement was ready")
	}
}

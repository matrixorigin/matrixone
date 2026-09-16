// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package hakeeper

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func validCatalogBarrier(phase pb.CatalogMetadataBarrierPhase) *pb.CatalogMetadataBarrierState {
	state := &pb.CatalogMetadataBarrierState{
		Phase:                          phase,
		MembershipEpoch:                7,
		RequiredGeneration:             11,
		CompletedGeneration:            10,
		RequiredViewDependencyProtocol: 1,
		RequiredRecoveryProtocol:       1,
	}
	if phase == pb.CATALOG_METADATA_BARRIER_ACTIVATED {
		state.CompletedGeneration = state.RequiredGeneration
	}
	return state
}

func TestCatalogMetadataBarrierValidation(t *testing.T) {
	for phase := pb.CATALOG_METADATA_BARRIER_PREPARING; phase <= pb.CATALOG_METADATA_BARRIER_ACTIVATED; phase++ {
		require.NoError(t, validateCatalogMetadataBarrier(validCatalogBarrier(phase)))
	}

	tests := []struct {
		name  string
		state *pb.CatalogMetadataBarrierState
	}{
		{name: "unknown-positive", state: &pb.CatalogMetadataBarrierState{Phase: 99}},
		{name: "unknown-negative", state: &pb.CatalogMetadataBarrierState{Phase: -1}},
		{name: "disabled-epoch", state: &pb.CatalogMetadataBarrierState{MembershipEpoch: 1}},
		{name: "disabled-required", state: &pb.CatalogMetadataBarrierState{RequiredGeneration: 1}},
		{name: "disabled-completed", state: &pb.CatalogMetadataBarrierState{CompletedGeneration: 1}},
		{name: "disabled-view-protocol", state: &pb.CatalogMetadataBarrierState{RequiredViewDependencyProtocol: 1}},
		{name: "disabled-recovery-protocol", state: &pb.CatalogMetadataBarrierState{RequiredRecoveryProtocol: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, validateCatalogMetadataBarrier(test.state))
		})
	}

	for _, mutate := range []struct {
		name string
		fn   func(*pb.CatalogMetadataBarrierState)
	}{
		{name: "missing-epoch", fn: func(s *pb.CatalogMetadataBarrierState) { s.MembershipEpoch = 0 }},
		{name: "missing-required", fn: func(s *pb.CatalogMetadataBarrierState) { s.RequiredGeneration = 0 }},
		{name: "missing-view-protocol", fn: func(s *pb.CatalogMetadataBarrierState) { s.RequiredViewDependencyProtocol = 0 }},
		{name: "missing-recovery-protocol", fn: func(s *pb.CatalogMetadataBarrierState) { s.RequiredRecoveryProtocol = 0 }},
		{name: "completed-ahead", fn: func(s *pb.CatalogMetadataBarrierState) { s.CompletedGeneration = 12 }},
		{name: "nonterminal-complete", fn: func(s *pb.CatalogMetadataBarrierState) { s.CompletedGeneration = 11 }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			state := validCatalogBarrier(pb.CATALOG_METADATA_BARRIER_RECOVERING)
			mutate.fn(state)
			require.Error(t, validateCatalogMetadataBarrier(state))
		})
	}
	activated := validCatalogBarrier(pb.CATALOG_METADATA_BARRIER_ACTIVATED)
	activated.CompletedGeneration--
	require.Error(t, validateCatalogMetadataBarrier(activated))
}

func TestHAKeeperSnapshotFormatsAndCompatibility(t *testing.T) {
	legacy := pb.NewRSMState()
	legacy.Index = 17
	raw, err := marshalHAKeeperSnapshot(&legacy, false)
	require.NoError(t, err)
	require.False(t, bytes.HasPrefix(raw, hakeeperSnapshotPrefix))

	floor := legacy
	floor.PersistedExpressionRequiredProtocolVersion = 3
	moh2, err := marshalHAKeeperSnapshot(&floor, false)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(moh2, hakeeperSnapshotMagicV2))

	barrier := floor
	barrier.CatalogMetadataBarrier = validCatalogBarrier(
		pb.CATALOG_METADATA_BARRIER_RECOVERING)
	moh3, err := marshalHAKeeperSnapshot(&barrier, false)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(moh3, hakeeperSnapshotMagicV3))

	disabledMOH3, err := marshalHAKeeperSnapshot(&legacy, true)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(disabledMOH3, hakeeperSnapshotMagicV3))

	for name, fixture := range map[string][]byte{
		"legacy-raw": raw,
		"moh2":       moh2,
		"moh3":       moh3,
		"moh3-zero":  disabledMOH3,
	} {
		t.Run(name, func(t *testing.T) {
			decoded, err := unmarshalHAKeeperSnapshot(fixture)
			require.NoError(t, err)
			require.Equal(t, uint64(17), decoded.Index)
			if name == "moh3" {
				require.Equal(t, barrier.CatalogMetadataBarrier, decoded.CatalogMetadataBarrier)
			}
		})
	}
}

func TestHAKeeperSnapshotFeaturePayloadConsistency(t *testing.T) {
	states := []struct {
		name  string
		state pb.HAKeeperRSMState
	}{
		{name: "zero", state: pb.NewRSMState()},
		{name: "floor", state: func() pb.HAKeeperRSMState {
			s := pb.NewRSMState()
			s.PersistedExpressionRequiredProtocolVersion = 1
			return s
		}()},
		{name: "barrier", state: func() pb.HAKeeperRSMState {
			s := pb.NewRSMState()
			s.CatalogMetadataBarrier = validCatalogBarrier(
				pb.CATALOG_METADATA_BARRIER_PREPARING)
			return s
		}()},
		{name: "both", state: func() pb.HAKeeperRSMState {
			s := pb.NewRSMState()
			s.PersistedExpressionRequiredProtocolVersion = 1
			s.CatalogMetadataBarrier = validCatalogBarrier(
				pb.CATALOG_METADATA_BARRIER_PREPARING)
			return s
		}()},
	}
	for _, test := range states {
		payload, err := test.state.Marshal()
		require.NoError(t, err)
		for features := uint64(0); features < 4; features++ {
			t.Run(fmt.Sprintf("%s/features-%d", test.name, features), func(t *testing.T) {
				envelope := pb.HAKeeperSnapshotEnvelope{
					FormatVersion:    1,
					RSMState:         payload,
					RequiredFeatures: features,
				}
				encoded, marshalErr := envelope.Marshal()
				require.NoError(t, marshalErr)
				fixture := append(append([]byte{}, hakeeperSnapshotMagicV3...), encoded...)
				_, decodeErr := unmarshalHAKeeperSnapshot(fixture)
				if features == snapshotRequiredFeatures(&test.state) {
					require.NoError(t, decodeErr)
				} else {
					require.Error(t, decodeErr)
				}
			})
		}
	}
}

func TestHAKeeperSnapshotRejectsBarrierInLegacyFormats(t *testing.T) {
	state := pb.NewRSMState()
	state.CatalogMetadataBarrier = validCatalogBarrier(pb.CATALOG_METADATA_BARRIER_PREPARING)
	payload, err := state.Marshal()
	require.NoError(t, err)
	_, err = unmarshalHAKeeperSnapshot(payload)
	require.Error(t, err)

	state.PersistedExpressionRequiredProtocolVersion = 1
	payload, err = state.Marshal()
	require.NoError(t, err)
	moh2 := append(append([]byte{}, hakeeperSnapshotMagicV2...), payload...)
	_, err = unmarshalHAKeeperSnapshot(moh2)
	require.Error(t, err)

	state.CatalogMetadataBarrier = &pb.CatalogMetadataBarrierState{MembershipEpoch: 1}
	_, err = marshalHAKeeperSnapshot(&state, false)
	require.Error(t, err)
}

func TestHAKeeperSnapshotRejectsInvalidEnvelopeAtomically(t *testing.T) {
	valid := pb.NewRSMState()
	valid.Index = 9
	payload, err := valid.Marshal()
	require.NoError(t, err)
	makeEnvelope := func(version uint32, features uint64, state []byte) []byte {
		t.Helper()
		envelope, marshalErr := (&pb.HAKeeperSnapshotEnvelope{
			FormatVersion: version, RSMState: state, RequiredFeatures: features,
		}).Marshal()
		require.NoError(t, marshalErr)
		return append(append([]byte{}, hakeeperSnapshotMagicV3...), envelope...)
	}
	invalidPhase := valid
	invalidPhase.CatalogMetadataBarrier = &pb.CatalogMetadataBarrierState{Phase: 99}
	invalidPhasePayload, err := invalidPhase.Marshal()
	require.NoError(t, err)

	fixtures := map[string][]byte{
		"empty":           makeEnvelope(1, 0, nil),
		"unknown-version": makeEnvelope(2, 0, payload),
		"unknown-feature": makeEnvelope(1, 1<<63, payload),
		"unknown-phase":   makeEnvelope(1, 0, invalidPhasePayload),
		"unknown-magic":   append(append([]byte{}, hakeeperSnapshotPrefix...), '9'),
		"truncated-moh3":  append(append([]byte{}, hakeeperSnapshotMagicV3...), 0x12, 0x7f),
		"truncated-moh2":  append(append([]byte{}, hakeeperSnapshotMagicV2...), 0x0a, 0x7f),
		"corrupt-raw":     {0x0a, 0x7f},
	}
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			rsm := NewStateMachine(0, 1).(*stateMachine)
			rsm.state.Index = 1234
			rsm.state.NextID = 5678
			before := rsm.state
			err := rsm.RecoverFromSnapshot(bytes.NewReader(fixture), nil, nil)
			require.Error(t, err)
			require.Equal(t, before, rsm.state)
		})
	}
}

func TestHAKeeperSnapshotRecoveryReplacesReusedBarrierState(t *testing.T) {
	legacy := pb.NewRSMState()
	legacy.Index = 42
	data, err := marshalHAKeeperSnapshot(&legacy, false)
	require.NoError(t, err)

	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CatalogMetadataBarrier = validCatalogBarrier(
		pb.CATALOG_METADATA_BARRIER_ACTIVATED)
	require.NoError(t, rsm.RecoverFromSnapshot(bytes.NewReader(data), nil, nil))
	require.Equal(t, uint64(42), rsm.state.Index)
	require.Nil(t, rsm.state.CatalogMetadataBarrier)
}

func benchmarkSnapshotRecovery(b *testing.B, format string, reused bool) {
	state := pb.NewRSMState()
	state.PersistedExpressionRequiredProtocolVersion = 1
	for i := range 256 {
		state.NextIDByKey[fmt.Sprintf("key-%04d", i)] = uint64(i)
	}
	forceV3 := format == "moh3"
	if format == "raw" {
		state.PersistedExpressionRequiredProtocolVersion = 0
	}
	data, err := marshalHAKeeperSnapshot(&state, forceV3)
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(data)), "snapshot-bytes")
	var rsm *stateMachine
	for range b.N {
		if !reused || rsm == nil {
			rsm = NewStateMachine(0, 1).(*stateMachine)
		}
		if err := rsm.RecoverFromSnapshot(bytes.NewReader(data), nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHAKeeperSnapshotRecovery(b *testing.B) {
	for _, format := range []string{"raw", "moh2", "moh3"} {
		for _, reused := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/reused-%t", format, reused), func(b *testing.B) {
				benchmarkSnapshotRecovery(b, format, reused)
			})
		}
	}
}

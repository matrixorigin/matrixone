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

package hakeeper

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func runtimeEvidenceFixture(phase pb.CatalogMetadataBarrierPhase) pb.HAKeeperRSMState {
	s := pb.NewRSMState()
	s.Tick = 10
	s.CatalogMetadataBarrierRequiredProtocolVersion = 1
	s.CatalogMetadataBarrier = validCatalogBarrier(phase)
	if phase == pb.CATALOG_METADATA_BARRIER_DISABLED {
		s.CatalogMetadataBarrier = &pb.CatalogMetadataBarrierState{}
	}
	s.CatalogMetadataBarrier.RuntimeEvidenceVersion = 1
	return s
}

func initializeRuntimeEvidence(s *pb.HAKeeperRSMState) {
	b := s.CatalogMetadataBarrier
	b.EvidenceInitialized = true
	b.Arbitration = &pb.CatalogMetadataArbitration{
		MaintenanceEnabled: true, LastOperationID: 2, LastConsumedFence: 1,
		Members: map[uint64]pb.CatalogMetadataReplicaIdentity{1: {UUID: "log", StoreIncarnation: "inc"}},
	}
	for _, stage := range []struct {
		phase  pb.CatalogMetadataBarrierPhase
		action pb.CatalogMetadataAction
		slot   **pb.CatalogMetadataReceipt
	}{
		{pb.CATALOG_METADATA_BARRIER_CATALOG_REQUIRED, pb.CATALOG_ACTION_CATALOG_REQUIRED, &b.Arbitration.RequiredReceipt},
		{pb.CATALOG_METADATA_BARRIER_RECOVERING, pb.CATALOG_ACTION_RECOVERY_STARTED, &b.Arbitration.StartedReceipt},
		{pb.CATALOG_METADATA_BARRIER_ACTIVATED, pb.CATALOG_ACTION_COMPLETE, &b.Arbitration.CompletedReceipt},
	} {
		if b.Phase >= stage.phase {
			claim := uint64(0)
			if stage.action != pb.CATALOG_ACTION_CATALOG_REQUIRED {
				claim, b.Arbitration.ClaimID = 2, 2
			}
			*stage.slot = &pb.CatalogMetadataReceipt{MembershipEpoch: b.MembershipEpoch, RequiredGeneration: b.RequiredGeneration, ClaimID: claim, Action: stage.action, Digest: make([]byte, 32)}
		}
	}
}

func TestCatalogMetadataRuntimeSnapshotMatrix(t *testing.T) {
	// Exercise every phase independently of the old expression floor, including
	// an initialized empty capture and the explicitly uninitialized legacy state.
	for phase := pb.CATALOG_METADATA_BARRIER_DISABLED; phase <= pb.CATALOG_METADATA_BARRIER_ACTIVATED; phase++ {
		for _, expression := range []uint64{0, 3} {
			for _, initialized := range []bool{false, true} {
				t.Run(fmt.Sprintf("phase-%d/expression-%d/initialized-%t", phase, expression, initialized), func(t *testing.T) {
					s := runtimeEvidenceFixture(phase)
					s.PersistedExpressionRequiredProtocolVersion = expression
					if initialized {
						initializeRuntimeEvidence(&s)
					}
					encoded, err := marshalHAKeeperSnapshot(&s, false)
					if phase == pb.CATALOG_METADATA_BARRIER_DISABLED && initialized {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
						return
					}
					require.NoError(t, err)
					require.True(t, bytes.HasPrefix(encoded, hakeeperSnapshotMagicV3))
					var envelope pb.HAKeeperSnapshotEnvelope
					require.NoError(t, envelope.Unmarshal(encoded[len(hakeeperSnapshotMagicV3):]))
					expected := uint64(1 << 2)
					if expression != 0 {
						expected |= 1 << 0
					}
					if phase != pb.CATALOG_METADATA_BARRIER_DISABLED {
						expected |= 1 << 1
					}
					require.Equal(t, expected, envelope.RequiredFeatures)
					rsm := NewStateMachine(0, 1).(*stateMachine)
					require.NoError(t, rsm.RecoverFromSnapshot(bytes.NewReader(encoded), nil, nil))
					require.Equal(t, uint64(1), rsm.state.CatalogMetadataBarrierRequiredProtocolVersion)
					require.Equal(t, expression, rsm.state.PersistedExpressionRequiredProtocolVersion)
					require.Equal(t, s.CatalogMetadataBarrier, rsm.state.CatalogMetadataBarrier)

					// Independently forge each missing or surplus known feature.
					for features := uint64(0); features < 8; features++ {
						if features == expected {
							continue
						}
						fixture := marshalSnapshotEnvelopeFixture(t, 1, features, envelope.RSMState)
						_, err := unmarshalHAKeeperSnapshot(fixture)
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "features=%d", features)
					}
				})
			}
		}
	}
}

func TestCatalogMetadataEvidenceRejectsInvalidSnapshotsAtomically(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*pb.HAKeeperRSMState)
	}{
		{"unknown-floor", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrierRequiredProtocolVersion = 2 }},
		{"missing-floor", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrierRequiredProtocolVersion = 0 }},
		{"missing-barrier", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier = nil }},
		{"legacy-version-with-floor", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.RuntimeEvidenceVersion = 0 }},
		{"unknown-version", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.RuntimeEvidenceVersion = 2 }},
		{"targets-without-initialization", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.EvidenceInitialized = false }},
		{"bad-service", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.Targets[0].ServiceType = pb.LogService }},
		{"unknown-service", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.Targets[0].ServiceType = 99 }},
		{"empty-uuid", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.Targets[0].UUID = "" }},
		{"zero-generation", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.Targets[0].Generation = 0 }},
		{"future-capture", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.Targets[0].CapturedTick = 11 }},
		{"unobserved-seal", func(s *pb.HAKeeperRSMState) { s.CatalogMetadataBarrier.Targets[0].SealComplete = true }},
		{"malformed-retirement", func(s *pb.HAKeeperRSMState) {
			s.CatalogMetadataBarrier.Targets[0].AuthorityRetirementDigest = []byte{1}
		}},
		{"retirement-without-seal", func(s *pb.HAKeeperRSMState) {
			s.CatalogMetadataBarrier.Targets[0].AuthorityRetirementDigest = bytes.Repeat([]byte{1}, 32)
			s.CatalogMetadataBarrier.Targets[0].ObservedPreparing = true
		}},
		{"retirement-without-observation", func(s *pb.HAKeeperRSMState) {
			s.CatalogMetadataBarrier.Targets[0].AuthorityRetirementDigest = bytes.Repeat([]byte{1}, 32)
			s.CatalogMetadataBarrier.Targets[0].SealComplete = true
		}},
		{"premature-seal", func(s *pb.HAKeeperRSMState) {
			s.CatalogMetadataBarrier.Phase = pb.CATALOG_METADATA_BARRIER_PREPARING
			s.CatalogMetadataBarrier.Targets[0].ObservedPreparing = true
			s.CatalogMetadataBarrier.Targets[0].SealComplete = true
		}},
		{"duplicate-target", func(s *pb.HAKeeperRSMState) {
			s.CatalogMetadataBarrier.Targets = append(s.CatalogMetadataBarrier.Targets, s.CatalogMetadataBarrier.Targets[0])
		}},
		{"unordered-generation", func(s *pb.HAKeeperRSMState) {
			v := s.CatalogMetadataBarrier.Targets[0]
			v.Generation--
			s.CatalogMetadataBarrier.Targets = append(s.CatalogMetadataBarrier.Targets, v)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := runtimeEvidenceFixture(pb.CATALOG_METADATA_BARRIER_SEALED)
			initializeRuntimeEvidence(&s)
			s.CatalogMetadataBarrier.Targets = []pb.CatalogMetadataBarrierTarget{{ServiceType: pb.CNService, UUID: "cn", Generation: 7, CapturedTick: 10}}
			tc.mutate(&s)
			_, err := marshalHAKeeperSnapshot(&s, false)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			payload, err := s.Marshal()
			require.NoError(t, err)
			fixture := marshalSnapshotEnvelopeFixture(t, 1, snapshotRequiredFeatures(&s), payload)
			rsm := NewStateMachine(0, 1).(*stateMachine)
			rsm.state = runtimeEvidenceFixture(pb.CATALOG_METADATA_BARRIER_ACTIVATED)
			rsm.state.NextIDByKey["independent"] = 17
			before, err := rsm.state.Marshal()
			require.NoError(t, err)
			err = rsm.RecoverFromSnapshot(bytes.NewReader(fixture), nil, nil)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			after, err := rsm.state.Marshal()
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, uint64(17), rsm.state.NextIDByKey["independent"])
		})
	}
}

func TestCatalogMetadataRuntimeRejectsLegacyContainers(t *testing.T) {
	s := runtimeEvidenceFixture(pb.CATALOG_METADATA_BARRIER_DISABLED)
	s.PersistedExpressionRequiredProtocolVersion = 3
	payload, err := s.Marshal()
	require.NoError(t, err)
	for _, fixture := range [][]byte{payload, append(append([]byte{}, hakeeperSnapshotMagicV2...), payload...)} {
		_, err := unmarshalHAKeeperSnapshot(fixture)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	}
}

func TestCatalogMetadataRuntimeTargetsRoundTripAndLegacyReplacement(t *testing.T) {
	s := runtimeEvidenceFixture(pb.CATALOG_METADATA_BARRIER_SEALED)
	initializeRuntimeEvidence(&s)
	s.CatalogMetadataBarrier.Targets = []pb.CatalogMetadataBarrierTarget{
		{ServiceType: pb.CNService, UUID: "a", Generation: 1, ObservedPreparing: true, SealComplete: true},
		{ServiceType: pb.CNService, UUID: "a", Generation: 2},
		{ServiceType: pb.CNService, UUID: "b", Generation: 1},
		{ServiceType: pb.ProxyService, UUID: "a", Generation: 1},
	}
	encoded, err := marshalHAKeeperSnapshot(&s, false)
	require.NoError(t, err)
	rsm := NewStateMachine(0, 1).(*stateMachine)
	require.NoError(t, rsm.RecoverFromSnapshot(bytes.NewReader(encoded), nil, nil))
	require.Equal(t, s.CatalogMetadataBarrier, rsm.state.CatalogMetadataBarrier)
	legacy := pb.NewRSMState()
	legacy.CatalogMetadataBarrier = validCatalogBarrier(pb.CATALOG_METADATA_BARRIER_PREPARING)
	encoded, err = marshalHAKeeperSnapshot(&legacy, false)
	require.NoError(t, err)
	require.NoError(t, rsm.RecoverFromSnapshot(bytes.NewReader(encoded), nil, nil))
	require.Zero(t, rsm.state.CatalogMetadataBarrierRequiredProtocolVersion)
	require.Zero(t, rsm.state.CatalogMetadataBarrier.RuntimeEvidenceVersion)
	require.False(t, rsm.state.CatalogMetadataBarrier.EvidenceInitialized)
	require.Empty(t, rsm.state.CatalogMetadataBarrier.Targets)
}

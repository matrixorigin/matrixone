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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	hakeeperSnapshotMagicV2 = []byte{0x0f, 'M', 'O', 'H', '2'}
	hakeeperSnapshotMagicV3 = []byte{0x0f, 'M', 'O', 'H', '3'}
	hakeeperSnapshotPrefix  = []byte{0x0f, 'M', 'O', 'H'}
	// hakeeperSnapshotMagic keeps the established MOH2 package contract.
	hakeeperSnapshotMagic = hakeeperSnapshotMagicV2
)

const (
	hakeeperSnapshotEnvelopeVersion        uint32 = 1
	hakeeperSnapshotFeatureExpressionFloor uint64 = 1 << 0
	hakeeperSnapshotFeatureCatalogBarrier  uint64 = 1 << 1
	hakeeperSnapshotKnownFeatures                 = hakeeperSnapshotFeatureExpressionFloor |
		hakeeperSnapshotFeatureCatalogBarrier
)

func validateCatalogMetadataBarrier(state *pb.CatalogMetadataBarrierState) error {
	if state == nil {
		return nil
	}
	phase := state.Phase
	if phase < pb.CATALOG_METADATA_BARRIER_DISABLED ||
		phase > pb.CATALOG_METADATA_BARRIER_ACTIVATED {
		return moerr.NewInvalidInputNoCtx(fmt.Sprintf("unknown catalog metadata barrier phase %d", phase))
	}
	if phase == pb.CATALOG_METADATA_BARRIER_DISABLED {
		if state.MembershipEpoch != 0 || state.RequiredGeneration != 0 ||
			state.CompletedGeneration != 0 || state.RequiredViewDependencyProtocol != 0 ||
			state.RequiredRecoveryProtocol != 0 {
			return moerr.NewInvalidInputNoCtx("disabled catalog metadata barrier has durable state")
		}
		return nil
	}
	if state.MembershipEpoch == 0 || state.RequiredGeneration == 0 ||
		state.RequiredViewDependencyProtocol == 0 || state.RequiredRecoveryProtocol == 0 {
		return moerr.NewInvalidInputNoCtx("active catalog metadata barrier is missing required state")
	}
	if state.CompletedGeneration > state.RequiredGeneration {
		return moerr.NewInvalidInputNoCtx("catalog metadata completed generation exceeds required generation")
	}
	if phase == pb.CATALOG_METADATA_BARRIER_ACTIVATED {
		if state.CompletedGeneration != state.RequiredGeneration {
			return moerr.NewInvalidInputNoCtx("activated catalog metadata barrier has incomplete generation")
		}
	} else if state.CompletedGeneration >= state.RequiredGeneration {
		return moerr.NewInvalidInputNoCtx("incomplete catalog metadata barrier has terminal generation")
	}
	return nil
}

func catalogMetadataBarrierEnabled(state *pb.HAKeeperRSMState) bool {
	return state.CatalogMetadataBarrier != nil &&
		state.CatalogMetadataBarrier.Phase != pb.CATALOG_METADATA_BARRIER_DISABLED
}

func snapshotRequiredFeatures(state *pb.HAKeeperRSMState) uint64 {
	var features uint64
	if state.PersistedExpressionRequiredProtocolVersion != 0 {
		features |= hakeeperSnapshotFeatureExpressionFloor
	}
	if catalogMetadataBarrierEnabled(state) {
		features |= hakeeperSnapshotFeatureCatalogBarrier
	}
	return features
}

func validateSnapshotFeatures(state *pb.HAKeeperRSMState, features uint64) error {
	if features&^hakeeperSnapshotKnownFeatures != 0 {
		return moerr.NewInvalidInputNoCtx(fmt.Sprintf("HAKeeper snapshot requires unknown features %#x", features))
	}
	if features != snapshotRequiredFeatures(state) {
		return moerr.NewInvalidInputNoCtx("HAKeeper snapshot features do not match its durable state")
	}
	return nil
}

// marshalHAKeeperSnapshot is the single snapshot writer policy. forceV3 exists
// only for compatibility fixtures while the production barrier has no producer.
func marshalHAKeeperSnapshot(state *pb.HAKeeperRSMState, forceV3 bool) ([]byte, error) {
	if err := validateCatalogMetadataBarrier(state.CatalogMetadataBarrier); err != nil {
		return nil, err
	}
	payload, err := state.Marshal()
	if err != nil {
		return nil, err
	}
	if catalogMetadataBarrierEnabled(state) || forceV3 {
		envelope := pb.HAKeeperSnapshotEnvelope{
			FormatVersion:    hakeeperSnapshotEnvelopeVersion,
			RSMState:         payload,
			RequiredFeatures: snapshotRequiredFeatures(state),
		}
		encoded, err := envelope.Marshal()
		if err != nil {
			return nil, err
		}
		return append(append([]byte{}, hakeeperSnapshotMagicV3...), encoded...), nil
	}
	if state.PersistedExpressionRequiredProtocolVersion != 0 {
		return append(append([]byte{}, hakeeperSnapshotMagicV2...), payload...), nil
	}
	return payload, nil
}

func unmarshalHAKeeperSnapshot(data []byte) (pb.HAKeeperRSMState, error) {
	decoded := pb.NewRSMState()
	switch {
	case bytes.HasPrefix(data, hakeeperSnapshotMagicV3):
		var envelope pb.HAKeeperSnapshotEnvelope
		if err := envelope.Unmarshal(data[len(hakeeperSnapshotMagicV3):]); err != nil {
			return pb.HAKeeperRSMState{}, err
		}
		if envelope.FormatVersion != hakeeperSnapshotEnvelopeVersion {
			return pb.HAKeeperRSMState{}, moerr.NewInvalidInputNoCtx(fmt.Sprintf(
				"unsupported HAKeeper snapshot envelope version %d", envelope.FormatVersion))
		}
		if len(envelope.RSMState) == 0 {
			return pb.HAKeeperRSMState{}, moerr.NewInvalidInputNoCtx("HAKeeper snapshot envelope has no RSM state")
		}
		if err := decoded.Unmarshal(envelope.RSMState); err != nil {
			return pb.HAKeeperRSMState{}, err
		}
		if err := validateCatalogMetadataBarrier(decoded.CatalogMetadataBarrier); err != nil {
			return pb.HAKeeperRSMState{}, err
		}
		if err := validateSnapshotFeatures(&decoded, envelope.RequiredFeatures); err != nil {
			return pb.HAKeeperRSMState{}, err
		}
	case bytes.HasPrefix(data, hakeeperSnapshotMagicV2):
		if err := decoded.Unmarshal(data[len(hakeeperSnapshotMagicV2):]); err != nil {
			return pb.HAKeeperRSMState{}, err
		}
		if decoded.PersistedExpressionRequiredProtocolVersion == 0 {
			return pb.HAKeeperRSMState{}, moerr.NewInvalidInputNoCtx(
				"persisted HAKeeper snapshot envelope is missing its protocol floor")
		}
		if catalogMetadataBarrierEnabled(&decoded) {
			return pb.HAKeeperRSMState{}, moerr.NewInvalidInputNoCtx("MOH2 snapshot contains catalog metadata barrier state")
		}
		if err := validateCatalogMetadataBarrier(decoded.CatalogMetadataBarrier); err != nil {
			return pb.HAKeeperRSMState{}, err
		}
	case bytes.HasPrefix(data, hakeeperSnapshotPrefix):
		return pb.HAKeeperRSMState{}, moerr.NewInvalidInputNoCtx("unknown HAKeeper snapshot format")
	default:
		if err := decoded.Unmarshal(data); err != nil {
			return pb.HAKeeperRSMState{}, err
		}
		if catalogMetadataBarrierEnabled(&decoded) {
			return pb.HAKeeperRSMState{}, moerr.NewInvalidInputNoCtx("legacy snapshot contains catalog metadata barrier state")
		}
		if err := validateCatalogMetadataBarrier(decoded.CatalogMetadataBarrier); err != nil {
			return pb.HAKeeperRSMState{}, err
		}
	}
	return decoded, nil
}

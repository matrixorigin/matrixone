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
	"maps"
)

// ActivationPhase is the durable cluster-level state for a codec generation.
// A generation is never silently downgraded after it reaches Enabled.
type ActivationPhase uint8

const (
	ActivationDisabled ActivationPhase = iota
	ActivationPreparing
	ActivationEnabled
	ActivationAborted
)

func (p ActivationPhase) String() string {
	switch p {
	case ActivationDisabled:
		return "disabled"
	case ActivationPreparing:
		return "preparing"
	case ActivationEnabled:
		return "enabled"
	case ActivationAborted:
		return "aborted"
	default:
		return fmt.Sprintf("phase(%d)", p)
	}
}

// NodeKind identifies the two node classes that must acknowledge a codec
// generation before it can be enabled. It intentionally does not encode a
// transport or HAKeeper implementation detail.
type NodeKind uint8

const (
	NodeCN NodeKind = iota + 1
	NodeTN
)

// NodeAcknowledgement is the normalized state used by the admission check.
// The durable protobuf messages may contain additional transport fields; the
// activation contract only consumes the node identity, incarnation, and
// complete codec capability.
type NodeAcknowledgement struct {
	NodeID      string
	Incarnation uint64
	Capability  Capability
}

// Activation is the durable, generation-scoped enablement contract. Target
// map values are node incarnations, not ephemeral heartbeat timestamps. A
// restarted node must publish a new incarnation and be acknowledged again.
type Activation struct {
	RequestedVersion uint32
	RegistryVersion  uint32
	RegistryDigest   []byte
	Generation       uint64
	Phase            ActivationPhase
	CnTargets        map[string]uint64
	TnTargets        map[string]uint64
}

// NewActivationRequest creates a preparing request for the current codec
// registry. The target maps are copied so a caller cannot mutate a pending
// activation after it has been persisted by its owner.
func NewActivationRequest(generation uint64, cnTargets, tnTargets map[string]uint64) Activation {
	return Activation{
		RequestedVersion: CollationAwareVersion,
		RegistryVersion:  uint32(RegistryVersion),
		RegistryDigest:   RegistryDigest(),
		Generation:       generation,
		Phase:            ActivationPreparing,
		CnTargets:        maps.Clone(cnTargets),
		TnTargets:        maps.Clone(tnTargets),
	}
}

// Validate checks the durable shape before a state is copied to a heartbeat,
// plan, or transaction admission object. Unknown phases and stale/mismatched
// registry identities fail closed.
func (a Activation) Validate() error {
	switch a.Phase {
	case ActivationDisabled:
		if a.RequestedVersion != 0 || a.RegistryVersion != 0 || len(a.RegistryDigest) != 0 ||
			a.Generation != 0 || len(a.CnTargets) != 0 || len(a.TnTargets) != 0 {
			return wrapCodecError(ErrMalformedKey, "disabled activation carries state")
		}
		return nil
	case ActivationPreparing, ActivationEnabled, ActivationAborted:
		if a.RequestedVersion != CollationAwareVersion {
			return wrapCodecError(ErrMalformedKey, "activation version %d", a.RequestedVersion)
		}
		if a.RegistryVersion != uint32(RegistryVersion) || !bytes.Equal(a.RegistryDigest, RegistryDigest()) {
			return wrapCodecError(ErrMalformedKey, "activation registry mismatch")
		}
		if a.Generation == 0 {
			return wrapCodecError(ErrMalformedKey, "activation generation is zero")
		}
		if a.Phase != ActivationAborted && (len(a.CnTargets) == 0 || len(a.TnTargets) == 0) {
			return wrapCodecError(ErrMalformedKey, "activation target set is incomplete")
		}
		if err := validateTargets(a.CnTargets, "CN"); err != nil {
			return err
		}
		return validateTargets(a.TnTargets, "TN")
	default:
		return wrapCodecError(ErrMalformedKey, "unknown activation phase %d", a.Phase)
	}
}

func validateTargets(targets map[string]uint64, kind string) error {
	for nodeID, incarnation := range targets {
		if nodeID == "" {
			return wrapCodecError(ErrMalformedKey, "%s target has empty node id", kind)
		}
		if incarnation == 0 {
			return wrapCodecError(ErrMalformedKey, "%s target %q has zero incarnation", kind, nodeID)
		}
	}
	return nil
}

// Advance moves an activation through the only legal durable transitions.
// Enabled and aborted generations cannot be reset to disabled or reused with
// a different registry. A caller that wants a new attempt must allocate a
// strictly newer generation.
func (a Activation) Advance(next ActivationPhase) (Activation, error) {
	if err := a.Validate(); err != nil {
		return Activation{}, err
	}
	allowed := false
	switch a.Phase {
	case ActivationPreparing:
		allowed = next == ActivationAborted
	case ActivationEnabled, ActivationAborted:
		allowed = next == a.Phase
	case ActivationDisabled:
		allowed = next == ActivationDisabled
	}
	if !allowed {
		return Activation{}, wrapCodecError(ErrMalformedKey, "illegal activation transition %s -> %s", a.Phase, next)
	}
	a.Phase = next
	a.RegistryDigest = RegistryDigest()
	return a, nil
}

// Enable is the only transition into the enabled phase. It requires every
// target incarnation to acknowledge both read and write support before the
// durable state changes, so a caller cannot bypass the capability fence with a
// bare phase update.
func (a Activation) Enable(cn, tn []NodeAcknowledgement) (Activation, error) {
	if !a.Ready(cn, tn) {
		return Activation{}, wrapCodecError(ErrMalformedKey, "activation acknowledgements are incomplete")
	}
	a.Phase = ActivationEnabled
	a.RegistryDigest = RegistryDigest()
	return a, nil
}

// NodeReady reports whether one acknowledged node is exactly the target
// incarnation and supports the requested codec for both read and write. It
// treats an unknown node kind or target as not ready rather than guessing.
func (a Activation) NodeReady(kind NodeKind, ack NodeAcknowledgement) bool {
	if a.Validate() != nil || ack.NodeID == "" || ack.Incarnation == 0 {
		return false
	}
	targets := a.CnTargets
	if kind == NodeTN {
		targets = a.TnTargets
	} else if kind != NodeCN {
		return false
	}
	wantIncarnation, ok := targets[ack.NodeID]
	if !ok || wantIncarnation != ack.Incarnation {
		return false
	}
	metadata := NewCollationAwareMetadata()
	return ack.Capability.Supports(metadata, false) && ack.Capability.Supports(metadata, true)
}

// Ready requires every target incarnation to acknowledge the same registry
// and both read/write support. Duplicate or extra acknowledgements are
// harmless; missing targets keep the generation in PREPARING.
func (a Activation) Ready(cn, tn []NodeAcknowledgement) bool {
	if a.Phase != ActivationPreparing || a.Validate() != nil {
		return false
	}
	return allTargetsReady(NodeCN, a.CnTargets, cn, a) &&
		allTargetsReady(NodeTN, a.TnTargets, tn, a)
}

func allTargetsReady(kind NodeKind, targets map[string]uint64, acks []NodeAcknowledgement, activation Activation) bool {
	ready := make(map[string]bool, len(targets))
	for _, ack := range acks {
		if activation.NodeReady(kind, ack) {
			ready[ack.NodeID] = true
		}
	}
	for nodeID := range targets {
		if !ready[nodeID] {
			return false
		}
	}
	return true
}

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

import "context"

// Admission is the immutable, request-scoped view used by planner and
// storage entry points. It deliberately carries both the durable activation
// record and the local node acknowledgement: an enabled generation alone is
// not enough to let a node read or write its keys after a restart.
type Admission struct {
	Activation Activation
	Kind       NodeKind
	Node       NodeAcknowledgement
}

func (a Admission) Validate(metadata RelationMetadata, write bool) error {
	if err := metadata.Validate(); err != nil {
		return err
	}
	if !metadata.IsV2() {
		return nil
	}
	if err := a.Activation.Validate(); err != nil {
		return err
	}
	if a.Activation.Phase != ActivationEnabled ||
		metadata.ActivationGeneration != a.Activation.Generation {
		return wrapCodecError(ErrMalformedKey, "activation generation is not enabled")
	}
	if !a.Activation.NodeReady(a.Kind, a.Node) {
		return wrapCodecError(ErrMalformedKey, "node capability is not ready")
	}
	if !a.Node.Capability.Supports(metadata, write) {
		return wrapCodecError(ErrMalformedKey, "node does not support codec version %d", metadata.Version)
	}
	return nil
}

type admissionContextKey struct{}

// WithAdmission attaches a validated request-scoped admission record. It
// returns the original context when the record is malformed so callers cannot
// accidentally turn an invalid activation into an enabled request.
func WithAdmission(ctx context.Context, admission Admission) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if admission.Activation.Validate() != nil || admission.Node.NodeID == "" || admission.Node.Incarnation == 0 {
		return ctx
	}
	return context.WithValue(ctx, admissionContextKey{}, admission)
}

func AdmissionFromContext(ctx context.Context) (Admission, bool) {
	if ctx == nil {
		return Admission{}, false
	}
	a, ok := ctx.Value(admissionContextKey{}).(Admission)
	return a, ok
}

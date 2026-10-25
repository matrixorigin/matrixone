// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// remote_object_admission.go is the r23 producer-provenance registry
// for a remote fragment's in-band object-ownership metadata (issue
// #25590 round-23 review, findings 1 and 3).
//
// Finding 1: the r22 intake interpreted rows as ownership metadata
// from their five-column shape alone, a shape an ordinary query result
// can carry, so a SELECT naming an existing committed file in the
// right column could book that file for deletion. Authority now comes
// from the plan the coordinator itself built: a stream's rows are a
// candidate for ownership adoption ONLY when the received scope's own
// operator tree contains one of the registered remote-persistence
// producers running a METADATA-BEARING OUTPUT MODE (design r11 D2,
// round-24 finding F2: the operator family alone is not provenance -
// UpdateWriteTable multi-update, workspace/external insert and local
// delete are ordinary row streams carrying no ownership obligation).
// An ordinary query fragment carries none of them — its rows never
// reach a recognizer, whatever their shape.
//
// Finding 3: the registry is also the settlement boundary. A fragment
// transfers its local ownership only when it contains a registered
// persistence producer (so the coordinator's intake could adopt every
// name the fragment persisted before the ack); a fragment with no
// registered producer keeps its local owner on every ending, so a new
// persistence producer class cannot release files the recording side
// never adopted until it registers here and contributes an intake
// classifier.
package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"go.uber.org/zap"
)

// remoteObjectAdmission records which registered remote-persistence
// producers a scope's own operator tree contains, AUTHENTICATED BY
// EACH PRODUCER'S OWN OUTPUT MODE (issue #25590 round-24 review finding
// F2, design r11 D2): an operator family alone is not provenance of a
// metadata output edge, only the mode that actually emits persisted
// object metadata is (MultiUpdate.UpdateWriteS3, Insert.ToWriteS3
// without ToExternal, Deletion.RemoteDelete).
type remoteObjectAdmission struct {
	multiUpdateS3 bool // vm.MultiUpdate / vm.PartitionMultiUpdate with Action==UpdateWriteS3
	insertS3      bool // vm.Insert / vm.PartitionInsert with ToWriteS3 && !ToExternal
	deleteRemote  bool // vm.Deletion / vm.PartitionDelete with RemoteDelete
}

// remotePersistedMetadataProducer is the structural interface every
// registered persistence producer family exports on BOTH its raw and
// its partition-wrapper types (the wrapper's raw field is only visible
// inside the producer package, so the mode reaches the admission
// through this predicate alone - no compile-side field access into
// wrappers).
type remotePersistedMetadataProducer interface {
	// ProducesRemotePersistedObjectMetadata reports whether the operator
	// itself runs a metadata-bearing output mode: only such a stream's
	// rows may ever be interpreted as ownership metadata.
	ProducesRemotePersistedObjectMetadata() bool
}

func (a remoteObjectAdmission) any() bool {
	return a.multiUpdateS3 || a.insertS3 || a.deleteRemote
}

// scopeRemoteObjectAdmission walks the scope the coordinator itself
// built — the exact operator tree it sent — and reports every
// registered persistence producer in a metadata-bearing output mode,
// including nested scopes.
func scopeRemoteObjectAdmission(s *Scope) remoteObjectAdmission {
	var adm remoteObjectAdmission
	admitScope(s, &adm)
	return adm
}

func admitScope(s *Scope, adm *remoteObjectAdmission) {
	if s == nil {
		return
	}
	admitOpTree(s.RootOp, adm)
	for _, sub := range s.PreScopes {
		admitScope(sub, adm)
	}
}

func admitOpTree(op vm.Operator, adm *remoteObjectAdmission) {
	if op == nil {
		return
	}
	if producer, produces := op.(remotePersistedMetadataProducer); produces &&
		producer.ProducesRemotePersistedObjectMetadata() {
		switch op.OpType() {
		case vm.MultiUpdate, vm.PartitionMultiUpdate:
			adm.multiUpdateS3 = true
		case vm.Insert, vm.PartitionInsert:
			adm.insertS3 = true
		case vm.Deletion, vm.PartitionDelete:
			adm.deleteRemote = true
		}
	}
	base := op.GetOperatorBase()
	if base == nil {
		return
	}
	n := base.NumChildren()
	for i := 0; i < n; i++ {
		admitOpTree(base.GetChildren(i), adm)
	}
}

// remoteRunScopesAdmitAdoption reports whether the decoded fragment
// scopes may carry adoptable ownership metadata at all: at least one
// registered persistence producer in a metadata-bearing output mode
// must sit in the fragment's own operator tree. The server-side
// settlement uses the same predicate to permit a transferred
// classification; every other fragment keeps a reachable local owner on
// every ending.
func remoteRunScopesAdmitAdoption(scopes []*Scope) bool {
	for _, s := range scopes {
		if scopeRemoteObjectAdmission(s).any() {
			return true
		}
	}
	return false
}

// coordinatorIntakeAdoptionVersion is the first coordinator build whose
// receive loops adopt the fragment workspace intake (issue #25590
// design r11 D3”): V_adopt. A request advertising a lower value —
// including the absent/zero default an older coordinator build
// produces — is an incapable coordinator for remotely dispatched
// persistence-producing DML.
const coordinatorIntakeAdoptionVersion = defines.MORPCVersion132

// requestAdoptsIntakeAdmission reports whether an execution request's
// advertised coordinator capability reaches the first adoption build.
// The run case's D3' refusal and the settlement's transferred
// classification both consume the value the REQUEST carried — never a
// receiver-side global or a test-forced fixture — so capability only
// ever reaches the fragment through the wire.
func requestAdoptsIntakeAdmission(advertised int64) bool {
	return advertised >= coordinatorIntakeAdoptionVersion
}

// refuseRemoteWriteFromIncapableCoordinator implements the D3' rule: a
// run whose decoded scope registers a persistence producer executes
// only when the request's advertised coordinator capability meets
// V_adopt. Below that — absent/zero included — the server refuses the
// run BEFORE pipeline execution: no writer persists anything, the
// definite unsupported-capability error takes the sendError terminal,
// the statement fails on both sides, and the refused run has nothing
// to delete (the empty rollback). Fragments with no registered
// producer stay executable for every coordinator: a plain query
// fragment from a below-adoption build keeps base semantics, so
// read traffic never depends on the adoption line.
func refuseRemoteWriteFromIncapableCoordinator(
	ctx context.Context,
	advertised int64,
	scopes []*Scope,
) error {
	if remoteRunScopesAdmitAdoption(scopes) &&
		!requestAdoptsIntakeAdmission(advertised) {
		logutil.Warn(
			"REMOTE-WRITE-REFUSED-INCAPABLE-COORDINATOR",
			zap.Int64("advertised", advertised),
			zap.Int64("required", coordinatorIntakeAdoptionVersion))
		return moerr.NewNotSupportedf(ctx,
			"remote write fragments require coordinator protocol capability %d or newer (coordinator advertised %d)",
			coordinatorIntakeAdoptionVersion, advertised)
	}
	return nil
}

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

package disttae

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// Pre-PR review round 9 (D-10 clause 3/G4): the compile finishes the
// whole-statement admission decision BEFORE prePipelineInitializer chooses
// the pre-pipeline lock options, so ProbeExecutionReadSupport must predict
// EXACTLY the soft-refusal class BindExecutionReadOp returns for the same
// relation — one shared predicate behind both consumers — while performing
// no binding side effect: the prelock probe can never disagree with the
// source-initialization binding it stands in front of.
func TestProbeExecutionReadSupportPreMeasuresEveryBindRefusal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	op, closeFn := client.NewTestTxnOperator(ctx)
	defer closeFn()
	// The supporting-bind subcase reaches txnIsValid and the tombstone
	// mask, whose binding contract requires a live workspace exactly like
	// the pin test.
	op.AddWorkspace(&Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	})
	carrier := client.ApplyExecutionReadTS(op, op.SnapshotTS())

	// The reviewer's counterexample class: an ORDINARY table the
	// transaction itself created (a transactional CREATE TABLE joined into
	// a promoted Shared locking read) softly refuses its
	// source-initialization binding; the prelock probe must return exactly
	// that refusal before any lock option is chosen.
	_, origin := newExecutionReadTestTable(t, op)
	created := origin
	created.createdInTxn = true
	probeVsBind(t, &txnTableDelegate{origin: created, reusableHandle: true},
		carrier, "transaction-created table")

	// Every other soft-refusal class predicts the same way.
	probeVsBind(t, &txnTableDelegate{origin: origin}, carrier,
		"non-exclusive relation handle")
	probeVsBind(t, &txnTableDelegate{origin: origin, reusableHandle: true, isMock: true},
		carrier, "relation handling")
	combined := &txnTableDelegate{origin: origin, reusableHandle: true}
	combined.combined.is = true
	probeVsBind(t, combined, carrier, "relation handling")
	sharded := &txnTableDelegate{origin: origin, reusableHandle: true}
	sharded.shard.is = true
	probeVsBind(t, sharded, carrier, "relation handling")
	// A PARTITIONED base's index relation refuses through its
	// partition-service parent link (D-10.2).
	partitionedParent := &txnTable{extraInfo: &api.SchemaExtra{FeatureFlag: features.Partitioned}}
	probeVsBind(t, &txnTableDelegate{origin: origin, reusableHandle: true, parent: partitionedParent},
		carrier, "relation handling")

	// None of the probes mutated the origin binding.
	require.Same(t, op, origin.db.op)

	// The probe is side-effect-free and the ENGINE INTERFACE carries it: a
	// supporting handle answers nil twice through engine.ExecutionReadBinding,
	// stays on its cached origin, and then binds through the same interface
	// with the full frontier pin — the probe never re-based anything.
	_, plainOrigin := newExecutionReadTestTable(t, op)
	plain := &txnTableDelegate{origin: plainOrigin, reusableHandle: true}
	binder, ok := engine.Relation(plain).(engine.ExecutionReadBinding)
	require.True(t, ok, "the exclusive handle exposes the execution-read binding")
	require.NoError(t, binder.ProbeExecutionReadSupport())
	require.NoError(t, binder.ProbeExecutionReadSupport())
	require.Same(t, plainOrigin, plain.origin,
		"the probe re-based the handle while only measuring support")
	frontierTS := timestamp.Timestamp{
		PhysicalTime: op.SnapshotTS().PhysicalTime + 83,
		LogicalTime:  5,
	}
	frontierCarrier := client.ApplyExecutionReadTS(op, frontierTS)
	require.NoError(t, binder.BindExecutionReadOp(frontierCarrier))
	require.Equal(t, frontierTS, plain.origin.db.op.SnapshotTS(),
		"the bind after a nil probe pins the full frontier")
	require.NotSame(t, plainOrigin, plain.origin)
}

// probeVsBind asserts the round-9 contract for one relation fixture: the
// probe returns the SAME soft refusal the bind returns — same class, same
// message — while the bind path keeps its own refusal behavior untouched.
func probeVsBind(t *testing.T, handle *txnTableDelegate, carrier client.TxnOperator, detail string) {
	t.Helper()
	binder, ok := engine.Relation(handle).(engine.ExecutionReadBinding)
	require.True(t, ok, detail)
	probeErr := binder.ProbeExecutionReadSupport()
	require.Error(t, probeErr, detail)
	require.True(t, engine.IsExecutionReadUnsupported(probeErr), probeErr)
	require.Contains(t, probeErr.Error(), detail)
	bindErr := binder.BindExecutionReadOp(carrier)
	require.Error(t, bindErr, detail)
	require.True(t, engine.IsExecutionReadUnsupported(bindErr), bindErr)
	require.Equal(t, probeErr.Error(), bindErr.Error(),
		"the probe must predict exactly the bind refusal for %s", detail)
}

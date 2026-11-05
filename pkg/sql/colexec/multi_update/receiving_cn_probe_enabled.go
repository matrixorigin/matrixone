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

//go:build receiving_cn_acceptance

package multi_update

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type receivingCNProbeTag struct{ partitionWriter bool }

// ReceivingCNProbeEnabled permits the compiler to eliminate probe paths from
// ordinary builds, including argument evaluation and writer-stat retention.
const ReceivingCNProbeEnabled = true

// ReceivingCNEvent contains identities and hashes, never SQL or batch payloads.
type ReceivingCNEvent struct {
	RunID, QueryID, ServiceID, Stage    string
	TxnID                               [16]byte
	TableID, Rows, ObjectRows, Blocks   uint64
	ReceiverID                          uint64
	Action                              uint8
	Objects, Active, Drained, Remaining int
	PartitionWriter                     bool
	Estimate                            float64
	SelectedS3                          bool
	Digest                              [32]byte
}

type receivingCNProbe struct {
	runID  string
	txnID  [16]byte
	handle func(context.Context, ReceivingCNEvent) error
}

var currentReceivingCNProbe atomic.Pointer[receivingCNProbe]
var nextReceivingCNReceiverID atomic.Uint64

type receivingCNReceiverKey struct{}

// ObserveReceivingCNReceiver binds a remote pipeline generation to the
// admitted transaction. Its close event is emitted after the handler's compile
// and operator cleanup, using captured identity rather than pooled state.
func ObserveReceivingCNReceiver(proc *process.Process) func() {
	p := receivingCNIdentity(proc)
	if p == nil {
		return nil
	}
	id := nextReceivingCNReceiverID.Add(1)
	proc.ReplaceTopCtx(context.WithValue(proc.GetTopContext(), receivingCNReceiverKey{}, id))
	e := receivingCNEvent(proc, "receiver-open")
	e.ReceiverID = id
	ctx := proc.Ctx
	_ = receivingCNEmitForProbe(ctx, p, e)
	return func() {
		e.Stage = "receiver-closed"
		_ = receivingCNEmitForProbe(ctx, p, e)
	}
}

// InstallReceivingCNProbe admits exactly one named run and one nonzero SQL
// transaction. Uninstall cannot remove a subsequent generation. The owner must
// join the statement and cleanup before reusing a transaction ID; uninstall does
// not wait for an already-running callback. The callback must bound any wait
// and honor its context; only progress may inject an error.
func InstallReceivingCNProbe(runID string, txnID [16]byte, handle func(context.Context, ReceivingCNEvent) error) (func(), error) {
	if runID == "" || txnID == [16]byte{} || handle == nil {
		return nil, errors.New("receiving CN probe requires run, transaction, and handler")
	}
	p := &receivingCNProbe{runID: runID, txnID: txnID, handle: handle}
	if !currentReceivingCNProbe.CompareAndSwap(nil, p) {
		return nil, errors.New("receiving CN probe already installed")
	}
	return func() { currentReceivingCNProbe.CompareAndSwap(p, nil) }, nil
}

func receivingCNEmit(ctx context.Context, txnID [16]byte, e ReceivingCNEvent) error {
	p := currentReceivingCNProbe.Load()
	if p == nil || p.txnID != txnID {
		return nil
	}
	return receivingCNEmitForProbe(ctx, p, e)
}

func receivingCNEmitForProbe(ctx context.Context, p *receivingCNProbe, e ReceivingCNEvent) error {
	if p == nil || currentReceivingCNProbe.Load() != p {
		return nil
	}
	e.RunID, e.TxnID = p.runID, p.txnID
	return p.handle(ctx, e)
}

func receivingCNIdentity(proc *process.Process) *receivingCNProbe {
	var id [16]byte
	if proc == nil || proc.Base == nil || proc.GetTxnOperator() == nil {
		return nil
	}
	raw := proc.GetTxnOperator().Txn().ID
	if len(raw) != len(id) {
		return nil
	}
	copy(id[:], raw)
	p := currentReceivingCNProbe.Load()
	if p == nil || id != p.txnID {
		return nil
	}
	return p
}

// ReceivingCNForceMultiCN is diagnostic-only and scoped to the admitted SQL
// transaction. It must not change bootstrap or unrelated query placement.
func ReceivingCNForceMultiCN(proc *process.Process) bool {
	return os.Getenv("MO_RECEIVING_CN_TARGETED_FORCE") == "1" && receivingCNIdentity(proc) != nil
}

func receivingCNEvent(proc *process.Process, stage string) ReceivingCNEvent {
	id, _ := proc.Ctx.Value(receivingCNReceiverKey{}).(uint64)
	return ReceivingCNEvent{ServiceID: proc.GetService(), QueryID: proc.QueryId(), Stage: stage, ReceiverID: id}
}

func markReceivingCNPartitionWriter(w *s3WriterDelegate) { w.partitionWriter = true }

func ObserveReceivingCNCompile(proc *process.Process, estimate float64, selected bool) {
	p := receivingCNIdentity(proc)
	if p == nil {
		return
	}
	e := receivingCNEvent(proc, "compile")
	e.Estimate, e.SelectedS3 = estimate, selected
	_ = receivingCNEmitForProbe(proc.Ctx, p, e)
}

func receivingCNPartitionCall(proc *process.Process, action UpdateAction) {
	p := receivingCNIdentity(proc)
	if p == nil {
		return
	}
	e := receivingCNEvent(proc, "partition-call")
	e.Action = uint8(action)
	_ = receivingCNEmitForProbe(proc.Ctx, p, e)
}

func receivingCNProgress(proc *process.Process, w *s3WriterDelegate, i int, stats []objectio.ObjectStats) error {
	p := receivingCNIdentity(proc)
	if p == nil {
		return nil
	}
	e := receivingCNEvent(proc, "sync")
	e.TableID = w.updateCtxs[i].TableDef.TblId
	e.Action = uint8(w.action)
	e.PartitionWriter, e.Objects = w.partitionWriter, len(stats)
	for _, st := range stats {
		e.ObjectRows += uint64(st.Rows())
		e.Blocks += uint64(st.BlkCnt())
	}
	return receivingCNEmitForProbe(proc.Ctx, p, e)
}

func receivingCNRecord(proc *process.Process, stage string, action uint8, table, rows uint64, name, data []byte) {
	p := receivingCNIdentity(proc)
	if p == nil || actionType(action) == actionAffectedRows {
		return
	}
	e := receivingCNEvent(proc, stage)
	e.Action, e.TableID, e.Rows = action, table, rows
	h := sha256.New()
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(name)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(name)
	_, _ = h.Write(data)
	copy(e.Digest[:], h.Sum(nil))
	_ = receivingCNEmitForProbe(proc.Ctx, p, e)
}

func receivingCNProduced(proc *process.Process, b *batch.Batch) {
	if b == nil || receivingCNIdentity(proc) == nil {
		return
	}
	actions := vector.MustFixedColNoTypeCheck[uint8](b.Vecs[0])
	tables := vector.MustFixedColNoTypeCheck[uint64](b.Vecs[1])
	rows := vector.MustFixedColNoTypeCheck[uint64](b.Vecs[2])
	names, nameArea := vector.MustVarlenaRawData(b.Vecs[3])
	data, dataArea := vector.MustVarlenaRawData(b.Vecs[4])
	for i, action := range actions {
		receivingCNRecord(proc, "produced", action, tables[i], rows[i], names[i].GetByteSlice(nameArea), data[i].GetByteSlice(dataArea))
	}
}

func receivingCNApplied(proc *process.Process, b *batch.Batch, i int) {
	if receivingCNIdentity(proc) == nil {
		return
	}
	actions := vector.MustFixedColNoTypeCheck[uint8](b.Vecs[0])
	tables := vector.MustFixedColNoTypeCheck[uint64](b.Vecs[1])
	rows := vector.MustFixedColNoTypeCheck[uint64](b.Vecs[2])
	names, nameArea := vector.MustVarlenaRawData(b.Vecs[3])
	data, dataArea := vector.MustVarlenaRawData(b.Vecs[4])
	receivingCNRecord(proc, "applied", actions[i], tables[i], rows[i], names[i].GetByteSlice(nameArea), data[i].GetByteSlice(dataArea))
}

func receivingCNCleanup(proc *process.Process, active, drained, remaining int) {
	p := receivingCNIdentity(proc)
	if p == nil || active+drained == 0 {
		return
	}
	e := receivingCNEvent(proc, "writers-cleaned")
	e.Active, e.Drained, e.Remaining = active, drained, remaining
	_ = receivingCNEmitForProbe(proc.Ctx, p, e)
}

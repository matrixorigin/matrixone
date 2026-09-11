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

package ivfflat

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// planSearchGeneration is fully initialized before readers are published. Its
// immutable state is shared; only reference ownership changes during execution.
type planSearchGeneration struct {
	proc        *process.Process
	snapshot    client.TxnOperator
	membership  docfilter.MembershipFilter
	route       []int64
	search      func(*planReader) error
	refs        atomic.Int32
	parallelism int32
	completed   atomic.Int32
	outputRows  atomic.Uint64
}

func (s *planSearchGeneration) release() {
	if s.refs.Add(-1) != 0 {
		return
	}
	if s.membership != nil {
		s.membership.Free()
		s.membership = nil
	}
	if s.proc.Cancel != nil {
		s.proc.Cancel(nil)
	}
	s.proc, s.snapshot, s.route, s.search = nil, nil, nil, nil
}

// NewPlanReaders creates disjoint local PRE readers, retaining logical CN
// identity for centroid cache keys. No entry reader opens until initialization
// has sealed the shared domain, snapshot, version and centroid route.
func NewPlanReaders(proc *process.Process, spec *plan.VectorIndexScan, req searchplugin.Request, parallelism int) ([]engine.Reader, error) {
	if parallelism <= 0 || int64(parallelism) > int64(^uint32(0)>>1) {
		return nil, moerr.NewInvalidInputNoCtx("invalid local vector parallelism")
	}
	prototypeReader, err := NewPlanReader(proc, spec, req)
	if err != nil {
		return nil, err
	}
	prototype := prototypeReader.(*planReader)
	if proc.Ctx == nil || !req.MembershipFilterRequired || req.Identity.PartitionCount > 1 || req.Identity.PartitionIndex != 0 {
		return nil, moerr.NewInvalidInputNoCtx("local PRE readers require a coordinator context and exact domain")
	}
	if err := proc.Ctx.Err(); err != nil {
		return nil, err
	}
	async, err := catalog.IndexParamAsync(spec.Index.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	if parallelism > 1 && (async || req.HasFirstRound || spec.FirstRoundLimit != nil || spec.BucketExpandStep > 0 || spec.ScanWork == nil) {
		return nil, moerr.NewInvalidInputNoCtx("vector execution mode does not support local DOP")
	}
	if req.CandidateBudget == 0 || len(req.MembershipFilter) == 0 {
		readers := make([]engine.Reader, parallelism)
		for i := range readers {
			readers[i] = &planReader{initialized: true}
		}
		return readers, nil
	}
	var keys vector.Vector
	if err := keys.UnmarshalBinary(req.MembershipFilter); err != nil {
		return nil, err
	}
	defer keys.Free(nil)
	pk := spec.SourceTableDef.GetPkey()
	if pk == nil {
		return nil, moerr.NewInvalidInputNoCtx("PRE source has no primary key")
	}
	pos, ok := spec.SourceTableDef.Name2ColIndex[pk.PkeyColName]
	if !ok || pos < 0 || int(pos) >= len(spec.SourceTableDef.Cols) || keys.HasNull() ||
		keys.GetType().Oid != types.T(spec.SourceTableDef.Cols[pos].Typ.Id) {
		return nil, moerr.NewInvalidInputNoCtx("PRE membership key type does not match the source primary key")
	}
	if parallelism > 1 && !docfilter.SupportsBitset(*keys.GetType()) {
		return nil, moerr.NewInvalidInputNoCtx("local PRE DOP requires an exact integer domain")
	}
	session := &planSearchGeneration{proc: proc.NewContextChildProc(0), snapshot: proc.GetTxnOperator(), parallelism: int32(parallelism)}
	session.refs.Store(1) // construction owns one reference until all readers exist
	defer session.release()
	if docfilter.SupportsBitset(*keys.GetType()) && keys.Length() > 0 {
		admission := docfilter.AdmissionForService(proc.GetService())
		payload, err := docfilter.BuildWithMemoryAdmission(&keys, admission)
		if err != nil {
			return nil, err
		}
		session.membership, err = docfilter.NewWithMemoryAdmission(payload, admission)
		if err != nil {
			return nil, err
		}
		if !session.membership.Exact() {
			return nil, moerr.NewInvalidStateNoCtx("integer PRE domain is not exact")
		}
	}
	if snapshot := req.Identity.Snapshot; snapshot != nil && snapshot.TS != nil &&
		(snapshot.TS.LogicalTime != 0 || snapshot.TS.PhysicalTime != 0) && snapshot.TS.Less(session.snapshot.Txn().SnapshotTS) {
		session.snapshot = session.snapshot.CloneSnapshotOp(*snapshot.TS)
	}
	prototype.proc, prototype.generation = session.proc, session
	prototype.scanner.proc, prototype.scanner.generation = session.proc, session
	if err := prototype.prepareSearch(true); err != nil {
		return nil, err
	}
	readers := make([]engine.Reader, 0, parallelism)
	for i := 0; i < parallelism; i++ {
		child := session.proc.NewContextChildProc(0)
		reader, err := NewPlanReader(child, spec, req)
		if err != nil {
			child.Cancel(err)
			for _, opened := range readers {
				_ = opened.Close()
			}
			return nil, err
		}
		r := reader.(*planReader)
		r.generation, r.ownsContext = session, true
		session.refs.Add(1)
		r.scanner.generation = session
		r.scanner.partitionCount, r.scanner.partitionIndex = int32(parallelism), int32(i)
		r.scanner.ownsInMemory = i == 0
		readers = append(readers, r)
	}
	return readers, nil
}

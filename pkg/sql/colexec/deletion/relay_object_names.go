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

// relay_object_names.go derives the storage-object names a standalone
// remote DELETE fragment's result stream carries (issue #25590
// round-23 review, finding 3).
//
// A remote deletion operator (Deletion.RemoteDelete) flushes what it
// persisted through the fragment result stream itself:
// makeDelRemoteBatch's five-vector layout [BlockMeta_Delete_ID,
// BlockMeta_DeltaLoc, BlockMeta_Type, BlockMeta_Partition,
// BlockMeta_Deletes_Length]. Two row classes share that stream, and
// only one of them names an object: for each CN-flushed S3 tombstone
// the row's DeltaLoc column carries the MARSHALED tombstone stats
// batch (a [ObjectMeta_ObjectStats] batch, one serialized ObjectStats
// row per tombstone object) and its Type column is FlushDeltaLoc; a
// per-block row carries the retained row-id batch and a block class
// in Type, owning nothing. The pre-r23 intake recognized none of it:
// a clean, drained standalone-delete fragment settled transferred,
// released its local owner, and a cancellation before the coordinator's
// record left the tombstone files enumerable nowhere.
//
// The recognizer is attribute-anchored and excludes the row-only class
// by the producer's own Type discriminator; a DeltaLoc that cannot
// decode rejects the ack instead of adopting a subset of the tombstones
// the delete persisted.
package deletion

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// ProducesRemotePersistedObjectMetadata is the mode-authenticated
// provenance edge the coordinator's remote-object admission asserts
// (issue #25590 round-24 review finding F2, design r11 D2): the remote
// deletion layout names tombstone objects only when a real remote
// S3-flushing deletion produced it. A local delete (RemoteDelete off)
// is a stream whose rows are ordinary data - the very same
// makeDelRemoteBatch-shaped rows from such an operator must never reach
// the recognizer, where they would either reject a legitimate stream
// fail-closed or adopt embedded stats as cleanup obligations.
func (deletion *Deletion) ProducesRemotePersistedObjectMetadata() bool {
	return deletion.RemoteDelete
}

// ProducesRemotePersistedObjectMetadata mirrors the raw operator's mode
// on the partition wrapper: the wrapper's raw field is only visible
// inside this package, so the compile-side admission consumes the mode
// through this predicate alone, never by reaching into the wrapper.
func (op *PartitionDelete) ProducesRemotePersistedObjectMetadata() bool {
	return op.raw != nil && op.raw.RemoteDelete
}

// TryExtractRemoteDeleteObjectNames returns ok=true only for a batch
// carrying the standalone deletion operator's remote layout, with one
// canonical name per flushed tombstone object, or an error when a
// FlushDeltaLoc row cannot prove the object it claims.
func TryExtractRemoteDeleteObjectNames(
	bat *batch.Batch, mp *mpool.MPool,
) (names []string, ok bool, err error) {
	if bat == nil || len(bat.Vecs) < 4 || bat.Vecs[0].Length() == 0 {
		return nil, false, nil
	}
	if len(bat.Attrs) < 4 ||
		bat.Attrs[0] != catalog.BlockMeta_Delete_ID ||
		bat.Attrs[1] != catalog.BlockMeta_DeltaLoc ||
		bat.Attrs[2] != catalog.BlockMeta_Type ||
		bat.Attrs[3] != catalog.BlockMeta_Partition {
		return nil, false, nil
	}
	if bat.Vecs[0].GetType().Oid != types.T_text ||
		bat.Vecs[1].GetType().Oid != types.T_text ||
		bat.Vecs[2].GetType().Oid != types.T_int8 ||
		bat.Vecs[3].GetType().Oid != types.T_int32 {
		return nil, false, nil
	}
	classes := vector.MustFixedColNoTypeCheck[int8](bat.Vecs[2])
	data, area := vector.MustVarlenaRawData(bat.Vecs[1])
	seen := make(map[string]struct{})
	inner := batch.NewOffHeapEmpty()
	defer inner.Clean(mp)
	for i, class := range classes {
		if class != int8(FlushDeltaLoc) {
			// A retained block's rows stayed in the workspace; the
			// producer's own class marker says no tombstone object.
			continue
		}
		blob := data[i].GetByteSlice(area)
		if len(blob) == 0 {
			return nil, false, moerr.NewInternalErrorNoCtx(
				"issue #25590: a flushed tombstone row carries no serialized stats batch")
		}
		inner.CleanOnlyData()
		if err := inner.UnmarshalBinaryWithAnyMp(blob, mp); err != nil {
			return nil, false, err
		}
		statsVec := -1
		for j, attr := range inner.Attrs {
			if attr == catalog.ObjectMeta_ObjectStats {
				statsVec = j
				break
			}
		}
		if statsVec < 0 || inner.Vecs[statsVec].Length() == 0 {
			return nil, false, moerr.NewInternalErrorNoCtx(
				"issue #25590: a flushed tombstone row's stats batch carries no object-stats column")
		}
		col, tailArea := vector.MustVarlenaRawData(inner.Vecs[statsVec])
		for j := range col {
			bs := col[j].GetByteSlice(tailArea)
			if len(bs) < objectio.ObjectStatsLen {
				return nil, false, moerr.NewInternalErrorNoCtx(
					"issue #25590: a flushed tombstone row carries a short object-stats value")
			}
			stats := objectio.ObjectStats(bs)
			name := stats.ObjectName().String()
			if _, dup := seen[name]; !dup {
				seen[name] = struct{}{}
				names = append(names, name)
			}
		}
	}
	return names, true, nil
}

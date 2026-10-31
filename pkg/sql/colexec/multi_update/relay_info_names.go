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

// relay_info_names.go derives the storage-object names a remote
// multi-update writer's relayed S3-info batch carries (issue #25590
// round-22 review finding H1, hardened by the round-23 review).
//
// The fragment's UpdateWriteS3 writer flushes its persisted objects'
// metadata to the coordinator as data: baseBatchToWire's five-vector
// layout [action, tableID, rowCount, name, serializedBatch] is the ONLY
// transport the coordinator's UpdateFlushS3Info consumer uses to record
// those objects. The names therefore ALREADY travel in band with every
// forwarded batch — the relay batch IS the ownership transfer note. The
// coordinator's transport client adopts them (admission reservation on
// the coordinator's workspace) BEFORE forwarding and BEFORE the ack
// returns to the fragment.
//
// Round-23 hardening, findings of the r22 pre-PR review: the four
// scalar columns never name an object. The r22 derivation read the
// delete rows' routing-name column — which s3writer_delegate.go packs
// as raw object-ID bytes plus '|type', a flush key, never a storage
// name — and decoded only the insert tail's first BlockInfo although
// the writer accumulates every object it persisted into that tail. The
// names now derive from the serialized tails exactly like the recording
// side does: EVERY row of the tail's ObjectStats column becomes one
// canonical object name (the same ObjectName() derivation the workspace
// record choke uses), the row-only delete marker (PhysicalAddr_Attr)
// owns nothing, and a tail that cannot decode, or decodes without its
// ObjectStats column, REJECTS the ack — the run fails while the
// fragment still holds its local owner, instead of silently adopting
// fewer objects than the writer persisted.
package multi_update

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
// (issue #25590 round-24 review finding F2, design r11 D2): an
// operator's FAMILY is not provenance of a metadata output edge, only
// its own producing mode is. The relay metadata batches the
// coordinator adopts flow exclusively from UpdateWriteS3 writers -
// UpdateWriteTable returns its input batch unchanged (see update()), so
// its ordinary rows are ordinary query data: they must never reach the
// relay recognizer,
// where they would either reject a legitimate stream fail-closed or
// adopt embedded object stats as cleanup obligations.
func (update *MultiUpdate) ProducesRemotePersistedObjectMetadata() bool {
	return update.Action == UpdateWriteS3
}

// ProducesRemotePersistedObjectMetadata mirrors the raw writer's mode on
// the partition wrapper: the wrapper's own raw field is only visible
// inside this package, so the compile-side admission consumes the mode
// through this predicate alone, never by reaching into the wrapper.
func (op *PartitionMultiUpdate) ProducesRemotePersistedObjectMetadata() bool {
	return op.raw != nil && op.raw.Action == UpdateWriteS3
}

// TryExtractRelayS3InfoObjectNames returns ok=true only for a batch
// carrying the relay layout, or an error when that layout claims
// persisted objects through a tail it cannot prove. Ordinary query rows
// never reach this function on a pure-data stream: the client's scope
// gate only admits streams whose own fragment runs a covered
// persistence producer in a METADATA-BEARING OUTPUT MODE (the plan the
// coordinator itself built), and even inside a gated stream a relay row
// must still decode its ownership metadata before the ack. actionUpdate
// (a non-transport class) and any unknown action byte reject the whole
// batch rather than adopt on a guess.
func TryExtractRelayS3InfoObjectNames(
	bat *batch.Batch, mp *mpool.MPool,
) (names []string, ok bool, err error) {
	if bat == nil || len(bat.Vecs) != 5 || bat.Vecs[0].Length() == 0 {
		return nil, false, nil
	}
	if bat.Vecs[0].GetType().Oid != types.T_uint8 ||
		bat.Vecs[1].GetType().Oid != types.T_uint64 ||
		bat.Vecs[2].GetType().Oid != types.T_uint64 ||
		bat.Vecs[3].GetType().Oid != types.T_varchar ||
		bat.Vecs[4].GetType().Oid != types.T_text {
		return nil, false, nil
	}
	actions := vector.MustFixedColNoTypeCheck[uint8](bat.Vecs[0])
	for _, action := range actions {
		if action != uint8(actionInsert) && action != uint8(actionDelete) &&
			action != uint8(actionAffectedRows) {
			return nil, false, nil
		}
	}
	nameData, nameArea := vector.MustVarlenaRawData(bat.Vecs[3])
	batData, batArea := vector.MustVarlenaRawData(bat.Vecs[4])
	seen := make(map[string]struct{})
	inner := batch.NewOffHeapEmpty()
	defer inner.Clean(mp)
	for i, action := range actions {
		switch actionType(action) {
		case actionDelete:
			if string(nameData[i].UnsafeGetString(nameArea)) == objectio.PhysicalAddr_Attr {
				// Row-only deletes persisted no object; the writer uses
				// this exact marker for their metadata record.
				continue
			}
			rowNames, tailErr := relayTailObjectNames(inner, mp, batData[i].GetByteSlice(batArea))
			if tailErr != nil {
				return nil, false, tailErr
			}
			names, seen = mergeNames(names, seen, rowNames)
		case actionInsert:
			rowNames, tailErr := relayTailObjectNames(inner, mp, batData[i].GetByteSlice(batArea))
			if tailErr != nil {
				return nil, false, tailErr
			}
			names, seen = mergeNames(names, seen, rowNames)
		case actionAffectedRows:
			// The internal count control records carry no object.
		}
	}
	return names, true, nil
}

// relayTailObjectNames decodes one row's serialized metadata batch and
// enumerates every distinct canonical object name its ObjectStats
// column carries — the same derivation the workspace record choke uses
// (objectio ObjectStats bytes -> ObjectName). A tail that cannot
// decode, or decodes without a non-empty ObjectStats column, cannot
// prove the objects the row claims, so ownership adoption fails
// closed on it instead of adopting a subset.
func relayTailObjectNames(
	inner *batch.Batch, mp *mpool.MPool, blob []byte,
) ([]string, error) {
	if len(blob) == 0 {
		return nil, moerr.NewInternalErrorNoCtx(
			"issue #25590: a relayed ownership row carries no serialized metadata batch")
	}
	inner.CleanOnlyData()
	if err := inner.UnmarshalBinaryWithAnyMp(blob, mp); err != nil {
		return nil, err
	}
	statsVec := -1
	for j, attr := range inner.Attrs {
		if attr == catalog.ObjectMeta_ObjectStats {
			statsVec = j
			break
		}
	}
	if statsVec < 0 || inner.Vecs[statsVec].Length() == 0 {
		return nil, moerr.NewInternalErrorNoCtx(
			"issue #25590: a relayed ownership row's metadata batch carries no object-stats column")
	}
	col, area := vector.MustVarlenaRawData(inner.Vecs[statsVec])
	names := make([]string, 0, inner.Vecs[statsVec].Length())
	for i := range col {
		bs := col[i].GetByteSlice(area)
		if len(bs) < objectio.ObjectStatsLen {
			return nil, moerr.NewInternalErrorNoCtx(
				"issue #25590: a relayed metadata row carries a short object-stats value")
		}
		stats := objectio.ObjectStats(bs)
		name := stats.ObjectName().String()
		if name == "" {
			return nil, moerr.NewInternalErrorNoCtx(
				"issue #25590: a relayed metadata row's object name is empty")
		}
		names = append(names, name)
	}
	return names, nil
}

// mergeNames accumulates deduplicated row names into the batch result.
func mergeNames(names []string, seen map[string]struct{}, rowNames []string) ([]string, map[string]struct{}) {
	for _, name := range rowNames {
		if _, dup := seen[name]; !dup {
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	return names, seen
}

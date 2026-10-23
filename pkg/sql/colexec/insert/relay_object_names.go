// relay_object_names.go derives the storage-object names a standalone
// remote INSERT fragment's result stream carries (issue #25590
// round-23 review, finding 3).
//
// A standalone insert operator with an S3 writer (Insert.ToWriteS3)
// flushes its persisted objects to the coordinator through the fragment
// result stream itself: every returned batch is the insert writer's own
// block-info batch — AllocCNS3ResultBat's two-vector layout
// [BlockMeta_BlockInfo, ObjectMeta_ObjectStats] — carrying one row per
// persisted insert object. The only authoritative source of the
// object's name is the ObjectStats column; a client that parsed the
// four- or five-scalar shapes typed by hand for other layouts could
// never prove this stream, and the pre-r23 intake recognized none of
// it: a clean, drained standalone-insert fragment therefore settled
// transferred, released its local owner, and a cancellation before the
// coordinator's record left the persisted files enumerated nowhere.
//
// The recognizer is deliberately attribute-anchored: both column names
// are the writer's internal metadata attributes. Rows never adopt
// outside a scope whose fragment runs the covered producer (the
// compile-side scope gate owns that check), and a stats value that
// cannot carry an ObjectStats rejects the ack instead of adopting a
// subset of the objects the insert persisted.
package insert

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// ProducesRemotePersistedObjectMetadata is the mode-authenticated
// provenance edge the coordinator's remote-object admission asserts
// (issue #25590 round-24 review finding F2, design r11 D2): the
// standalone-insert result layout names persisted objects only when a
// real S3 writer produced it. An insert writing into the workspace or
// into a writable external table's stage files (ToExternal) is a
// stream whose rows are ordinary data - the very same two-vector
// batch from such an operator must never reach the recognizer, where
// it would either reject a legitimate stream fail-closed or adopt
// embedded object stats as cleanup obligations.
func (insert *Insert) ProducesRemotePersistedObjectMetadata() bool {
	return insert.ToWriteS3 && !insert.ToExternal
}

// ProducesRemotePersistedObjectMetadata mirrors the raw writer's mode on
// the partition wrapper: the wrapper's raw field is only visible inside
// this package, so the compile-side admission consumes the mode through
// this predicate alone, never by reaching into the wrapper.
func (op *PartitionInsert) ProducesRemotePersistedObjectMetadata() bool {
	return op.raw != nil && op.raw.ToWriteS3 && !op.raw.ToExternal
}

// TryExtractS3InsertObjectNames returns ok=true only for a batch
// carrying the standalone insert writer's result layout, with one
// canonical name per persisted object, or an error when a row cannot
// prove the object it claims.
func TryExtractS3InsertObjectNames(
	bat *batch.Batch,
) (names []string, ok bool, err error) {
	if bat == nil || len(bat.Vecs) != 2 || bat.Vecs[0].Length() == 0 {
		return nil, false, nil
	}
	if len(bat.Attrs) < 2 ||
		bat.Attrs[0] != catalog.BlockMeta_BlockInfo ||
		bat.Attrs[1] != catalog.ObjectMeta_ObjectStats {
		return nil, false, nil
	}
	if bat.Vecs[0].GetType().Oid != types.T_text ||
		bat.Vecs[1].GetType().Oid != types.T_binary {
		return nil, false, nil
	}
	col, area := vector.MustVarlenaRawData(bat.Vecs[1])
	names = make([]string, 0, len(col))
	for i := range col {
		bs := col[i].GetByteSlice(area)
		if len(bs) < objectio.ObjectStatsLen {
			return nil, false, moerr.NewInternalErrorNoCtx(
				"issue #25590: a standalone insert result row carries a short object-stats value")
		}
		stats := objectio.ObjectStats(bs)
		names = append(names, stats.ObjectName().String())
	}
	return names, true, nil
}

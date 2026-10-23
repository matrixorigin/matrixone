package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// TestIssue25590RemoteObjectAdmissionRegistry pins the r23
// producer-provenance registry (issue #25590 round-23 review, findings 1
// and 3): only a scope whose own operator tree contains a REGISTERED
// remote-persistence producer may interpret its rows as ownership
// metadata, and the settlement transfers only such a fragment.
func TestIssue25590RemoteObjectAdmissionRegistry(t *testing.T) {
	// An ordinary query fragment: a connector over a value scan, no
	// persistence producer — whatever column shapes its rows carry,
	// the intake admits nothing.
	vs := value_scan.NewArgument()
	ordinary := &Scope{}
	ordinary.RootOp = connector.NewArgument()
	ordinary.RootOp.GetOperatorBase().AppendChild(vs)
	require.False(t, scopeRemoteObjectAdmission(ordinary).any(),
		"an ordinary query scope never admits ownership rows")

	// The same ordinary shape nested as a PreScope stays ordinary.
	parent := &Scope{}
	query := &Scope{}
	query.RootOp = connector.NewArgument()
	parent.PreScopes = append(parent.PreScopes, query)
	require.False(t, scopeRemoteObjectAdmission(parent).any(),
		"nested ordinary scopes stay ordinary")

	// A multi-update write fragment: the UpdateWriteS3 writer registers.
	write := &Scope{}
	mu := multi_update.NewArgument()
	mu.Action = multi_update.UpdateWriteS3
	write.RootOp = mu
	adm := scopeRemoteObjectAdmission(write)
	require.True(t, adm.multiUpdateS3, "the UpdateWriteS3 writer registers")
	require.False(t, adm.insertS3 || adm.deleteRemote,
		"only the writer that sat in the tree registered")

	// The registry predicates the client gate and the settlement share.
	require.False(t, remoteRunScopesAdmitAdoption([]*Scope{ordinary}))
	require.True(t, remoteRunScopesAdmitAdoption([]*Scope{write}))

	// A producer nested inside PreScopes keeps its fragment registry-
	// covered, so its drained clean runs may still transfer.
	mixed := &Scope{}
	mixed.PreScopes = append(mixed.PreScopes, write)
	require.True(t, remoteRunScopesAdmitAdoption([]*Scope{mixed}),
		"a producer nested in PreScopes keeps its fragment transferable")
}

// TestIssue25590AdmissionAuthenticatesProducerModesR29 pins the design
// r11 D2 rule (round-24 finding F2, the round-29 pre-PR finding 1): an
// operator FAMILY alone never admits - only the producer's own output
// mode does, for both raw and partition-wrapper types, at root and
// nested placement.
func TestIssue25590AdmissionAuthenticatesProducerModesR29(t *testing.T) {
	admitOf := func(root vm.Operator) remoteObjectAdmission {
		s := &Scope{}
		s.RootOp = root
		return scopeRemoteObjectAdmission(s)
	}

	// multi_update: only UpdateWriteS3 emits relay metadata.
	tableUpd := multi_update.NewArgument()
	require.Equal(t, multi_update.UpdateWriteTable, tableUpd.Action,
		"the zero action is the ordinary passthrough mode")
	require.False(t, tableUpd.ProducesRemotePersistedObjectMetadata(),
		"UpdateWriteTable returns its input rows unchanged: not a metadata edge")
	require.False(t, admitOf(tableUpd).any(),
		"an ordinary multi-update fragment admits nothing")

	s3Upd := multi_update.NewArgument()
	s3Upd.Action = multi_update.UpdateWriteS3
	require.True(t, s3Upd.ProducesRemotePersistedObjectMetadata())
	require.True(t, admitOf(s3Upd).multiUpdateS3)

	flushInfo := multi_update.NewArgument()
	flushInfo.Action = multi_update.UpdateFlushS3Info
	require.False(t, flushInfo.ProducesRemotePersistedObjectMetadata(),
		"the flush-info consumer records relay metadata; it never produces it")

	// Partition wrappers carry the same mode through the raw field,
	// which only the predicate can see from here.
	wrapS3 := multi_update.NewPartitionMultiUpdate(s3Upd)
	require.True(t, admitOf(wrapS3).multiUpdateS3,
		"a partition wrapper around an UpdateWriteS3 writer registers")
	wrapTable := multi_update.NewPartitionMultiUpdate(tableUpd)
	require.False(t, admitOf(wrapTable).any(),
		"a partition wrapper around an ordinary writer admits nothing")

	// insert: only an S3 writer that is not writing to an external
	// table's stage files emits the standalone insert result layout.
	insOrdinary := insert.NewArgument()
	require.False(t, insOrdinary.ProducesRemotePersistedObjectMetadata(),
		"a workspace insert's rows are ordinary data")
	require.False(t, admitOf(insOrdinary).any())

	insS3 := insert.NewArgument()
	insS3.ToWriteS3 = true
	require.True(t, insS3.ProducesRemotePersistedObjectMetadata())
	require.True(t, admitOf(insS3).insertS3)

	insExt := insert.NewArgument()
	insExt.ToWriteS3 = true
	insExt.ToExternal = true
	require.False(t, insExt.ProducesRemotePersistedObjectMetadata(),
		"an external-table writer streams to stage files: ordinary data")
	require.False(t, admitOf(insExt).any())

	insWrap := insert.NewPartitionInsert(insS3, 7)
	require.True(t, admitOf(insWrap).insertS3,
		"a partition wrapper around an S3 insert registers")
	insWrapOrdinary := insert.NewPartitionInsert(insOrdinary, 7)
	require.False(t, admitOf(insWrapOrdinary).any())

	// deletion: only a remote-deleting operator (tombstones flushed to
	// S3) emits the remote deletion layout.
	delLocal := deletion.NewArgument()
	require.False(t, delLocal.ProducesRemotePersistedObjectMetadata(),
		"a local delete's rows are ordinary data")
	require.False(t, admitOf(delLocal).any())

	delRemote := deletion.NewArgument()
	delRemote.RemoteDelete = true
	require.True(t, delRemote.ProducesRemotePersistedObjectMetadata())
	require.True(t, admitOf(delRemote).deleteRemote)

	delWrap := deletion.NewPartitionDelete(delRemote, 7)
	require.True(t, admitOf(delWrap).deleteRemote,
		"a partition wrapper around a remote delete registers")
	delWrapLocal := deletion.NewPartitionDelete(delLocal, 7)
	require.False(t, admitOf(delWrapLocal).any())

	// Nested placement: a metadata-mode producer in PreScopes keeps the
	// fragment transferable; the same families in ordinary modes do
	// not, even all together.
	nestedMeta := &Scope{}
	nestedMeta.PreScopes = append(nestedMeta.PreScopes, &Scope{RootOp: wrapS3})
	require.True(t, remoteRunScopesAdmitAdoption([]*Scope{nestedMeta}),
		"a wrapped metadata-mode producer nested in PreScopes keeps the fragment transferable")

	nestedOrdinary := &Scope{}
	nestedOrdinary.PreScopes = append(nestedOrdinary.PreScopes,
		&Scope{RootOp: tableUpd}, &Scope{RootOp: insOrdinary}, &Scope{RootOp: delLocal})
	require.False(t, remoteRunScopesAdmitAdoption([]*Scope{nestedOrdinary}),
		"ALL the same families in ordinary modes still settle transferred=false: no classifier may ever see their rows")
	require.NoError(t, refuseRemoteWriteFromIncapableCoordinator(context.Background(), 0, []*Scope{nestedOrdinary}),
		"an ordinary-mode fragment never depends on coordinator adoption capability")

	mixedBearer := &Scope{}
	ordinaryRoot := connector.NewArgument()
	ordinaryRoot.GetOperatorBase().AppendChild(value_scan.NewArgument())
	mixedBearer.RootOp = ordinaryRoot
	mixedBearer.PreScopes = append(mixedBearer.PreScopes, &Scope{RootOp: s3Upd})
	require.True(t, refuseRemoteWriteFromIncapableCoordinator(context.Background(), 0, []*Scope{mixedBearer}) != nil,
		"a fragment with ANY metadata-mode producer (here in PreScopes) still refuses a below-adoption coordinator")
}

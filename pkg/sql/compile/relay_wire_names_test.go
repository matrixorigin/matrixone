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

package compile

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// TestRelayWireNames pins the mixed-version contract of the remote-run
// sender: a batch carries no column names, but an older coordinator
// recognizes the standalone insert's and the remote delete's relay
// batches by name. The sender writes the producer's fixed names only when
// its scope runs that producer and the batch has the producer's layout,
// and writes no names for anything else.
func TestRelayWireNames(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	insertResult := colexec.AllocCNS3ResultBat(false, mp)
	defer insertResult.Clean(mp)
	require.NoError(t, vector.AppendBytes(insertResult.Vecs[0], []byte("blk"), false, mp))
	require.NoError(t, vector.AppendBytes(insertResult.Vecs[1], []byte("stats"), false, mp))
	insertResult.SetRowCount(1)

	deleteRows := batch.NewWithSize(5)
	deleteRows.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
	deleteRows.Vecs[1] = vector.NewVec(types.T_text.ToType(), mp)
	deleteRows.Vecs[2] = vector.NewVec(types.T_int8.ToType(), mp)
	deleteRows.Vecs[3] = vector.NewVec(types.T_int32.ToType(), mp)
	deleteRows.Vecs[4] = vector.NewVec(types.T_uint32.ToType(), mp)
	defer deleteRows.Clean(mp)

	data := batch.NewWithSize(2)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	data.Vecs[1] = vector.NewVec(types.T_int64.ToType(), mp)
	defer data.Clean(mp)

	insertAdm := remoteObjectAdmission{insertS3: true}
	deleteAdm := remoteObjectAdmission{deleteRemote: true}
	updateAdm := remoteObjectAdmission{multiUpdateS3: true}
	none := remoteObjectAdmission{}

	require.Equal(t,
		[]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats},
		insertAdm.relayWireNames(insertResult))
	require.Nil(t, insertAdm.relayWireNames(data))
	require.Nil(t, insertAdm.relayWireNames(deleteRows))

	require.Equal(t, deletion.DelRemoteBatchAttrs, deleteAdm.relayWireNames(deleteRows))
	require.Equal(t, catalog.BlockMeta_Delete_ID, deleteAdm.relayWireNames(deleteRows)[0])
	require.Nil(t, deleteAdm.relayWireNames(insertResult))
	require.Nil(t, deleteAdm.relayWireNames(data))

	// The multi_update relay was never named; scopes without a
	// persistence producer write no names at all.
	require.Nil(t, updateAdm.relayWireNames(insertResult))
	require.Nil(t, none.relayWireNames(insertResult))
	require.Nil(t, none.relayWireNames(deleteRows))

	// What the sender writes is what an older coordinator reads back as
	// the batch's names, and the default encoding writes none.
	var buf bytes.Buffer
	encoded, err := insertResult.MarshalBinaryWithBufferAndNames(
		&buf, true, insertAdm.relayWireNames(insertResult))
	require.NoError(t, err)
	names, err := batch.WireNames(encoded)
	require.NoError(t, err)
	require.Equal(t, colexec.CNS3ResultAttrs, names)

	decoded := batch.NewWithSize(0)
	defer decoded.Clean(mp)
	require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(encoded, mp))
	require.Equal(t, 1, decoded.RowCount())
	require.Len(t, decoded.Vecs, 2)

	plain, err := data.MarshalBinary()
	require.NoError(t, err)
	names, err = batch.WireNames(plain)
	require.NoError(t, err)
	require.Empty(t, names)
}

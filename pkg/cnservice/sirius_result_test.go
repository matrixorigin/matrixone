// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cnservice

import (
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedSiriusResultBorrowsCheckedFixedAndVarlena(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	data := make([]byte, 16+2*types.VarlenaSize+25)
	binary.LittleEndian.PutUint64(data, 42)
	strings := data[16 : 16+2*types.VarlenaSize]
	strings[0], strings[1] = 1, 'a'
	binary.LittleEndian.PutUint32(strings[types.VarlenaSize:], types.VarlenaBigHdr)
	binary.LittleEndian.PutUint32(strings[types.VarlenaSize+8:], 25)
	area := data[16+2*types.VarlenaSize:]
	copy(area, "1234567890123456789012345")
	result := siriusbridge.Result{Rows: 2, Backing: data, Vectors: []siriusbridge.Vector{
		{Data: data[:16], Nulls: []byte{2, 0, 0, 0, 0, 0, 0, 0}}, {Data: strings, Area: area},
	}}
	request := compile.SiriusPrepareRequest{Headings: []string{"n", "s"}, OutputTypes: []planpb.Type{{Id: int32(types.T_int64)}, {Id: int32(types.T_varchar), Width: 25}}}
	bat, err := decodeEmbeddedSiriusResult(result, request, proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	require.Equal(t, vector.BorrowedLease, bat.Vecs[0].DataBackingKind())
	require.Equal(t, vector.BorrowedLease, bat.Vecs[1].AreaBackingKind())
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0))
	require.True(t, bat.Vecs[0].IsNull(1))
	require.Equal(t, "a", string(bat.Vecs[1].GetBytesAt(0)))
	require.Equal(t, string(area), string(bat.Vecs[1].GetBytesAt(1)))
	require.Same(t, &data[0], &bat.Vecs[0].GetData()[0], "do not copy the complete Go payload again")
}

func TestEmbeddedSiriusResultRejectsMalformedLayoutBeforeBorrowing(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	request := compile.SiriusPrepareRequest{Headings: []string{"s"}, OutputTypes: []planpb.Type{{Id: int32(types.T_varchar), NotNullable: true}}}
	for _, mutate := range []func(*siriusbridge.Vector){
		func(v *siriusbridge.Vector) { v.Class = 1 },
		func(v *siriusbridge.Vector) { v.Data = v.Data[:23] },
		func(v *siriusbridge.Vector) { v.Nulls = []byte{1, 0, 0, 0, 0, 0, 0, 0} },
		func(v *siriusbridge.Vector) { v.Nulls = []byte{0} },
		func(v *siriusbridge.Vector) { v.Data[0] = 24 },
		func(v *siriusbridge.Vector) {
			binary.LittleEndian.PutUint32(v.Data, types.VarlenaBigHdr)
			binary.LittleEndian.PutUint32(v.Data[8:], 1)
		},
	} {
		v := siriusbridge.Vector{Data: make([]byte, types.VarlenaSize)}
		mutate(&v)
		bat, err := decodeEmbeddedSiriusResult(siriusbridge.Result{Rows: 1, Vectors: []siriusbridge.Vector{v}}, request, proc.Mp())
		require.Error(t, err)
		require.Nil(t, bat)
		require.Zero(t, proc.Mp().CurrNB())
	}
}

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

func TestEmbeddedDecimal256ResultPreservesLimbsNullsAndBorrowing(t *testing.T) {
	proc := testutil.NewProcess(t)
	values := []types.Decimal256{{B0_63: 9007199254740993, B192_255: 1}, {B0_63: ^uint64(124), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}, {}}
	backing := make([]byte, len(values)*32+8)
	for row, value := range values {
		for limb, word := range []uint64{value.B0_63, value.B64_127, value.B128_191, value.B192_255} {
			binary.LittleEndian.PutUint64(backing[row*32+limb*8:], word)
		}
	}
	backing[len(values)*32] = 4
	result := siriusbridge.Result{Rows: 3, Backing: backing, Vectors: []siriusbridge.Vector{{Data: backing[:96], Nulls: backing[96:]}}}
	request := compile.SiriusPrepareRequest{Headings: []string{"v"}, OutputTypes: []planpb.Type{{Id: int32(types.T_decimal256), Width: 65, Scale: 4}}}
	bat, err := decodeEmbeddedSiriusResult(result, request, proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	require.Equal(t, vector.BorrowedLease, bat.Vecs[0].DataBackingKind())
	require.Same(t, &backing[0], &bat.Vecs[0].GetData()[0])
	require.Equal(t, types.New(types.T_decimal256, 65, 4), *bat.Vecs[0].GetType())
	for row := 0; row < 2; row++ {
		require.Equal(t, values[row], vector.GetFixedAtNoTypeCheck[types.Decimal256](bat.Vecs[0], row))
	}
	require.True(t, bat.Vecs[0].IsNull(2))
	for _, bad := range []siriusbridge.Vector{
		{Data: backing[:95]},
		{Data: backing[:96], Class: 1},
		{Data: backing[:96], Nulls: []byte{8, 0, 0, 0, 0, 0, 0, 0}},
	} {
		invalid, err := decodeEmbeddedSiriusResult(siriusbridge.Result{Rows: 3, Vectors: []siriusbridge.Vector{bad}}, request, proc.Mp())
		require.Error(t, err)
		require.Nil(t, invalid)
	}
}

func TestEmbeddedSchemaEvidenceRetainsDescriptorIdentity(t *testing.T) {
	request := compile.SiriusPrepareRequest{Headings: []string{"private expression heading"}, OutputTypes: []planpb.Type{{Id: int32(types.T_decimal256), Width: 15, Scale: 2}}}
	columns, digest := embeddedSchemaEvidence(request)
	require.Equal(t, []embeddedColumnEvidence{{OID: int32(types.T_decimal256), Precision: 15, Scale: 2, Nullable: true}}, columns)
	require.Len(t, digest, 64)
	_, same := embeddedSchemaEvidence(request)
	require.Equal(t, digest, same)
	for _, mutate := range []func(){
		func() { request.OutputTypes[0].Id = int32(types.T_decimal64) },
		func() { request.OutputTypes[0].Width++ },
		func() { request.OutputTypes[0].Scale++ },
		func() { request.OutputTypes[0].NotNullable = true },
		func() { request.Headings[0] = "another heading" },
	} {
		mutate()
		_, changed := embeddedSchemaEvidence(request)
		require.NotEqual(t, digest, changed)
		digest = changed
	}
}

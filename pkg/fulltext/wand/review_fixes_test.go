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

package wand

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// TestDocFilterMembershipUuid is the B1 regression: a uuid membership probe must hit
// the filter entry, which is built from the source uuid's RAW 16 bytes. Before the
// fix, Contains re-encoded via encodePk (36-char canonical string) and never matched,
// so a uuid-PK retrieval query with a WHERE prefilter returned zero rows.
func TestDocFilterMembershipUuid(t *testing.T) {
	mp := mpool.MustNewZero()
	uu := types.Uuid([16]byte{0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8})
	other := types.Uuid([16]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9})

	vec := vector.NewVec(types.New(types.T_uuid, 16, 0))
	require.NoError(t, vector.AppendFixed(vec, uu, false, mp))
	fbytes, err := docfilter.Build(vec)
	require.NoError(t, err)
	filter, err := docfilter.New(fbytes)
	require.NoError(t, err)

	m := &WandModel{PkType: int32(types.T_uuid), pks: []any{uu, other}}
	dfm := &docFilterMembership{m: m, f: filter}

	require.True(t, dfm.Contains(0), "uuid IN the membership filter must match (B1)")
	require.False(t, dfm.Contains(1), "uuid NOT in the filter must not match")
}

// TestDecodeWandCdcTruncatedPk is the #6 regression: a blob whose internal pkLen
// exceeds the remaining bytes (valid outer CRC, but a truncated body) must return a
// clean error rather than silently zero-filling a corrupt pk.
func TestDecodeWandCdcTruncatedPk(t *testing.T) {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, wandCdcMagic)
	_ = binary.Write(&b, binary.LittleEndian, int32(types.T_varchar))
	_ = binary.Write(&b, binary.LittleEndian, int64(1)) // one event
	b.WriteByte(byte(cdcInsert))
	_ = binary.Write(&b, binary.LittleEndian, uint32(1000)) // pkLen=1000, but no pk bytes follow
	// recompute the outer CRC so the truncation is caught by the length guard, not the CRC
	sum := crc32.ChecksumIEEE(b.Bytes())
	_ = binary.Write(&b, binary.LittleEndian, sum)

	_, err := DecodeWandCdc(b.Bytes())
	require.Error(t, err, "a pkLen past the buffer end must error, not zero-fill (#6)")
}

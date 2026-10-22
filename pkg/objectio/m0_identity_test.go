//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestM0ObjectSchemaIdentity(t *testing.T) {
	require.IsType(t, (*error)(nil), &ErrM0SchemaIdentityUnavailable)
	require.IsType(t, (*error)(nil), &ErrM0AppendLayout)
	h := BuildHeader()
	h.SetSchemaVersion(7)
	meta := buildObjectDataMetaV1(1)
	meta.BlockHeader().SetTableID(42)
	tableID, version, err := M0ObjectSchemaIdentity(h, meta)
	require.NoError(t, err)
	require.Equal(t, uint64(42), tableID)
	require.Equal(t, uint32(7), version)

	for _, test := range []struct {
		name string
		h    Header
		meta ObjectDataMeta
	}{
		{"short header", h[:8], meta},
		{"bad magic", make(Header, HeaderSize), meta},
		{"short metadata", h, meta[:8]},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := M0ObjectSchemaIdentity(test.h, test.meta)
			require.Error(t, err)
		})
	}
	meta.BlockHeader().SetTableID(0)
	_, _, err = M0ObjectSchemaIdentity(h, meta)
	require.ErrorIs(t, err, ErrM0SchemaIdentityUnavailable)
}

// This exercises the actual B writer, disk bytes and a newly opened service.
// A nonzero schema version and dense columns do not supply an origin relation.
// Reopening cannot manufacture the missing identity. This is not a SQL clone
// or WAL/checkpoint recovery test.
func TestM0PersistedBObjectHasNoWriterSchemaIdentity(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	cfg := fileservice.Config{
		Name: defines.SharedFileServiceName, Backend: "DISK-V2", DataDir: t.TempDir(), Cache: fileservice.DisabledCacheConfig,
	}
	fs, err := fileservice.NewFileService(ctx, cfg, nil)
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			fs.Close(ctx)
		}
	}()
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	w, err := NewObjectWriter(name, fs, 7, []uint16{0}, nil)
	require.NoError(t, err)
	bat := batch.NewWithSize(1)
	bat.SetVector(0, vector.NewVec(types.T_int32.ToType()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int32{1, 2, 3}, nil, mp))
	bat.SetRowCount(3)
	defer bat.Clean(mp)
	_, err = w.Write(bat)
	require.NoError(t, err)
	_, err = w.WriteEnd(ctx)
	require.NoError(t, err)
	fs.Close(ctx)
	closed = true

	reopened, err := fileservice.NewFileService(ctx, cfg, nil)
	require.NoError(t, err)
	defer reopened.Close(ctx)
	r, err := NewObjectReaderWithStr(name.String(), reopened)
	require.NoError(t, err)
	h, err := r.ReadHeader(ctx, mp)
	require.NoError(t, err)
	meta, err := r.ReadAllMeta(ctx, mp)
	require.NoError(t, err)
	data := meta.MustDataMeta()
	require.Equal(t, uint64(0), data.BlockHeader().TableID())
	require.Equal(t, uint16(0), data.BlockHeader().MaxSeqnum())
	require.Equal(t, uint32(7), types.DecodeUint32(h[8+2+ExtentSize:8+2+ExtentSize+4]))
	before := bytes.Clone(data)
	_, _, err = M0ObjectSchemaIdentity(h, data)
	require.ErrorIs(t, err, ErrM0SchemaIdentityUnavailable)
	require.Equal(t, before, []byte(data))
	// A proposed appended column is physically absent, but remains unproven.
	require.Zero(t, data.MustGetColumn(1).DataType())
	require.Zero(t, data.MustGetColumn(1).NullCnt())
}

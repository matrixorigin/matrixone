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

package objectio

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

// policyRecordingFS records the policy of each read.
type policyRecordingFS struct {
	fileservice.FileService
	mu       sync.Mutex
	policies []fileservice.Policy
}

func (fs *policyRecordingFS) Read(ctx context.Context, vec *fileservice.IOVector) error {
	fs.mu.Lock()
	fs.policies = append(fs.policies, vec.Policy)
	fs.mu.Unlock()
	return fs.FileService.Read(ctx, vec)
}

// Object metadata and bloom filter loads read exactly their extents and never preload the
// whole object, so they read no bytes of the object's columns.
func TestMetadataLoadsReadExactly(t *testing.T) {
	ctx := context.Background()
	base, err := fileservice.NewMemoryFS("metadata-policy", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { base.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { bat.Clean(mp) })
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, mp))
	bat.SetRowCount(1)
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, base, 0, []uint16{0}, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBF(0, 0, []byte("bloom"), 0))
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	location := BuildLocation(name, blocks[0].GetExtent(), 1, 0)

	oldCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(1 << 20))
	t.Cleanup(func() { metaCache = oldCache })
	fs := &policyRecordingFS{FileService: base}
	_, err = FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)
	_, err = FastLoadBF(ctx, location, false, fs)
	require.NoError(t, err)
	require.Len(t, fs.policies, 2, "a meta read and a bloom filter read")
	for _, policy := range fs.policies {
		require.True(t, policy.Any(fileservice.ExactRead), "policy %b", policy)
		require.True(t, policy.Any(fileservice.SkipFullFilePreloads), "policy %b", policy)
	}
}

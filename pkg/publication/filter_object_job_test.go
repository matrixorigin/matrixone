// Copyright 2021 Matrix Origin
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

package publication

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewGetMetaJob(t *testing.T) {
	ctx := context.Background()
	job := NewGetMetaJob(ctx, nil, "obj1", "acc1", "pub1")
	require.NotNil(t, job)
	assert.Equal(t, int8(1), job.GetType())
}

func TestNewGetChunkJob(t *testing.T) {
	ctx := context.Background()
	job := NewGetChunkJob(ctx, nil, "obj1", 5, "acc1", "pub1")
	require.NotNil(t, job)
	assert.Equal(t, "obj1", job.GetObjectName())
	assert.Equal(t, int64(5), job.GetChunkIndex())
	assert.Equal(t, int8(2), job.GetType())
}

func TestNewWriteObjectJob(t *testing.T) {
	ctx := context.Background()
	content := []byte("test content")
	job := NewWriteObjectJob(ctx, nil, "obj1", content, nil, nil)
	require.NotNil(t, job)
	assert.Equal(t, "obj1", job.GetObjectName())
	assert.Equal(t, int64(len(content)), job.GetObjectSize())
	assert.Equal(t, JobTypeWriteObject, job.GetType())
}

func TestNewFilterObjectJob(t *testing.T) {
	ctx := context.Background()
	statsBytes := []byte("stats")
	ts := types.BuildTS(100, 0)
	job := NewFilterObjectJob(
		ctx, statsBytes, ts, nil, false, nil, nil, nil, nil,
		"acc1", "pub1", nil, nil, nil, nil,
	)
	require.NotNil(t, job)
	assert.Equal(t, JobTypeFilterObject, job.GetType())
}

func TestGetOrCreateChunkSemaphore(t *testing.T) {
	sem := getOrCreateChunkSemaphore()
	assert.NotNil(t, sem)
	// Should return the same instance
	assert.Equal(t, sem, getOrCreateChunkSemaphore())
}

func TestAObjectMap(t *testing.T) {
	m := NewAObjectMap()
	assert.NotNil(t, m)

	mapping := &AObjectMapping{DBName: "db1", TableName: "t1"}
	m.Set("key1", mapping)

	got, ok := m.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "db1", got.DBName)

	_, ok = m.Get("nonexistent")
	assert.False(t, ok)

	m.Delete("key1")
	_, ok = m.Get("key1")
	assert.False(t, ok)
}

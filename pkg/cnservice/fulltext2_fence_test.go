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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

var fenceTestIdentity atomic.Uint64

type blockingFenceSearch struct {
	entered   chan struct{}
	release   chan struct{}
	destroyed chan struct{}
	once      sync.Once
}

func (*blockingFenceSearch) Load(*sqlexec.SqlProcess) error { return nil }
func (s *blockingFenceSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return []int64{1}, []float64{1}, nil
}
func (s *blockingFenceSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	_, _, err := s.Search(nil, nil, vectorindex.RuntimeConfig{})
	return err
}
func (*blockingFenceSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (s *blockingFenceSearch) Destroy() { close(s.destroyed) }

type immediateFenceSearch struct{}

func (*immediateFenceSearch) Load(*sqlexec.SqlProcess) error { return nil }
func (*immediateFenceSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{2}, []float64{2}, nil
}
func (*immediateFenceSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}
func (*immediateFenceSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (*immediateFenceSearch) Destroy() {}

func TestHandleFulltext2CacheFenceMonotonicAck(t *testing.T) {
	s := &service{}
	request := func(base, tail int64) *querypb.Request {
		return &querypb.Request{Fulltext2CacheFenceRequest: querypb.Fulltext2CacheFenceRequest{
			AccountID: 17, Database: t.Name(), StorageTable: "store", MetadataTable: "meta",
			BaseTimestamp: base, TailChunk: tail,
		}}
	}
	for _, generation := range [][2]int64{{3, 5}, {3, 5}, {2, 99}, {3, 7}} {
		resp := &querypb.Response{}
		require.NoError(t, s.handleFulltext2CacheFence(context.Background(), request(generation[0], generation[1]), resp, nil))
		require.True(t, resp.Fulltext2CacheFenceResponse.EvictionClaimed)
	}
	resp := &querypb.Response{}
	require.NoError(t, s.handleFulltext2CacheFence(context.Background(), request(3, 6), resp, nil))
	require.Equal(t, int64(3), resp.Fulltext2CacheFenceResponse.RequiredBaseTimestamp)
	require.Equal(t, int64(7), resp.Fulltext2CacheFenceResponse.RequiredTailChunk)
	require.True(t, resp.Fulltext2CacheFenceResponse.EvictionClaimed)
}

func TestHandleFulltext2CacheFenceRejectsIncompleteIdentity(t *testing.T) {
	s := &service{}
	err := s.handleFulltext2CacheFence(context.Background(), &querypb.Request{}, &querypb.Response{}, nil)
	require.ErrorContains(t, err, "incomplete FULLTEXT2 cache identity")
}

func TestHandleFulltext2CacheFenceClaimsWithoutWaitingForOldSearch(t *testing.T) {
	oldCache := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache = oldCache })

	id := fulltext2.CacheIdentity{
		AccountID: 23, Database: fmt.Sprintf("%s-%d", t.Name(), fenceTestIdentity.Add(1)),
		StorageTable: "store", MetadataTable: "meta",
	}
	old := &blockingFenceSearch{entered: make(chan struct{}), release: make(chan struct{}), destroyed: make(chan struct{})}
	oldDone := make(chan error, 1)
	go func() {
		_, _, err := veccache.Cache.Search(nil, id.Key(), old, nil, vectorindex.RuntimeConfig{})
		oldDone <- err
	}()
	select {
	case <-old.entered:
	case <-time.After(time.Second):
		t.Fatal("old search did not acquire its reader lease")
	}

	req := &querypb.Request{Fulltext2CacheFenceRequest: querypb.Fulltext2CacheFenceRequest{
		AccountID: id.AccountID, Database: id.Database, StorageTable: id.StorageTable, MetadataTable: id.MetadataTable,
		BaseTimestamp: 9, TailChunk: 4,
	}}
	resp := &querypb.Response{}
	ackDone := make(chan error, 1)
	go func() { ackDone <- (&service{}).handleFulltext2CacheFence(context.Background(), req, resp, nil) }()
	select {
	case err := <-ackDone:
		require.NoError(t, err)
		require.True(t, resp.Fulltext2CacheFenceResponse.EvictionClaimed)
	case <-time.After(time.Second):
		t.Fatal("fence ACK waited for the old search")
	}
	_, present := veccache.Cache.IndexMap.Load(id.Key())
	require.False(t, present, "claim must remove old-generation admission before ACK")

	keys, _, err := veccache.Cache.Search(nil, id.Key(), &immediateFenceSearch{}, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, []int64{2}, keys)
	close(old.release)
	require.NoError(t, <-oldDone)
	select {
	case <-old.destroyed:
	case <-time.After(time.Second):
		t.Fatal("old object was not destroyed after its reader finished")
	}
}

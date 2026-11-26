// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type readTestIndex struct {
	MockSearch
	loads atomic.Int64
}

func (i *readTestIndex) Load(*sqlexec.SqlProcess) error { i.loads.Add(1); return nil }

func TestReadSharesGenerationAndProtectsLifetime(t *testing.T) {
	c := NewVectorIndexCache()
	defer c.Destroy()
	proc := sqlexec.NewSqlProcess(testutil.NewProc(t))
	first := &readTestIndex{}
	second := &readTestIndex{}
	require.NoError(t, c.Read(proc, "stats", first, func(i VectorIndexSearchIf) error { require.Same(t, first, i); return nil }))
	require.NoError(t, c.Read(proc, "stats", second, func(i VectorIndexSearchIf) error { require.Same(t, first, i); return nil }))
	require.Equal(t, int64(1), first.loads.Load())
	require.Zero(t, second.loads.Load())
	want := moerr.NewInternalErrorNoCtx("read callback failed")
	found, err := c.ReadIfLoaded("stats", func(VectorIndexSearchIf) error { return want })
	require.True(t, found)
	require.ErrorIs(t, err, want)
	entered, release, finished, removed := make(chan struct{}), make(chan struct{}), make(chan error, 1), make(chan struct{})
	go func() {
		finished <- c.Read(proc, "stats", second, func(VectorIndexSearchIf) error { close(entered); <-release; return nil })
	}()
	<-entered
	go func() { c.Remove("stats"); close(removed) }()
	select {
	case <-removed:
		close(release)
		t.Fatal("eviction freed a borrowed generation")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-finished)
	select {
	case <-removed:
	case <-time.After(5 * time.Second):
		t.Fatal("eviction did not finish")
	}
	found, err = c.ReadIfLoaded("stats", func(VectorIndexSearchIf) error { t.Fatal("callback on missing generation"); return nil })
	require.False(t, found)
	require.NoError(t, err)
}

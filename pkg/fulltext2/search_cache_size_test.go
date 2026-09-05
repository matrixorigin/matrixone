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

package fulltext2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// GetIndexSize answers in both states: after Preload from the counted docs, and
// after Load from the loaded segments. Nothing is device resident.
func TestFulltext2SearchGetIndexSize(t *testing.T) {
	t.Run("before preload", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		host, device := s.GetIndexSize()
		require.Equal(t, int64(0), host)
		require.Equal(t, int64(0), device)
	})

	// Between Preload and Load the entry reports the count Preload measured.
	t.Run("after preload", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		s.preloadNdoc, s.preloaded = 100, true

		host, device := s.GetIndexSize()
		require.Equal(t, int64(100*estBytesPerDocHeap), host)
		require.Equal(t, int64(0), device)
	})

	// Once loaded the segments supersede the preload count; a nil segment adds nothing.
	t.Run("after load", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		s.preloadNdoc, s.preloaded = 100, true
		// Built directly: NewIndex resolves every segment, so it cannot hold the
		// nil entry GetIndexSize defends against.
		s.idx = &Index{segments: []*Segment{{N: 3}, nil, {N: 4}}}
		s.loaded = true

		host, device := s.GetIndexSize()
		require.Equal(t, int64(7*estBytesPerDocHeap), host)
		require.Equal(t, int64(0), device)
	})

	// loaded with a nil index falls back to the preload figure.
	t.Run("loaded but no index", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		s.preloadNdoc, s.preloaded = 5, true
		s.loaded = true

		host, _ := s.GetIndexSize()
		require.Equal(t, int64(5*estBytesPerDocHeap), host)
	})
}

// baseDocCount sums nrow across the tag=0 bases. Empty batches are skipped.
func TestBaseDocCount(t *testing.T) {
	mp := mpool.MustNewZero()
	cfg := TableConfig{DbName: "db", MetadataTable: "meta"}

	t.Run("sum", func(t *testing.T) {
		swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 42)}}, nil
		})
		got, err := baseDocCount(nil, cfg)
		require.NoError(t, err)
		require.Equal(t, int64(42), got)
	})

	t.Run("empty batches are skipped", func(t *testing.T) {
		empty := int64Batch(mp, 0)
		empty.SetRowCount(0)
		swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{empty, int64Batch(mp, 7)}}, nil
		})
		got, err := baseDocCount(nil, cfg)
		require.NoError(t, err)
		require.Equal(t, int64(7), got)
	})

	t.Run("no rows at all", func(t *testing.T) {
		swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
			return executor.Result{Mp: mp}, nil
		})
		got, err := baseDocCount(nil, cfg)
		require.NoError(t, err)
		require.Equal(t, int64(0), got)
	})

	t.Run("sql error", func(t *testing.T) {
		swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("boom")
		})
		_, err := baseDocCount(nil, cfg)
		require.Error(t, err)
	})
}

// Preload records the count in preloadNdoc and sets preloaded.
func TestFulltext2SearchPreload(t *testing.T) {
	mp := mpool.MustNewZero()
	swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 9)}}, nil
	})

	s := NewFulltext2Search(TableConfig{DbName: "db", MetadataTable: "meta"})
	require.NoError(t, s.Preload(nil))
	require.True(t, s.preloaded)
	require.Equal(t, int64(9), s.preloadNdoc)

	host, _ := s.GetIndexSize()
	require.Equal(t, int64(9*estBytesPerDocHeap), host, "the governor charges what Preload measured")
}

// A failed count leaves preloaded false.
func TestFulltext2SearchPreload_Error(t *testing.T) {
	swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("boom")
	})

	s := NewFulltext2Search(TableConfig{DbName: "db", MetadataTable: "meta"})
	require.Error(t, s.Preload(nil))
	require.False(t, s.preloaded)
}

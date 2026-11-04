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

package search

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type keyTestReader struct {
	engine.Reader
	closed      atomic.Int32
	err         error
	diagnostics []*plan.Query
}

func (r *keyTestReader) Close() error { r.closed.Add(1); return r.err }
func (r *keyTestReader) TakeExplainDiagnostics() []*plan.Query {
	out := r.diagnostics
	r.diagnostics = nil
	return out
}

func readerTestKeys(t *testing.T, released *atomic.Int32) *keycodec.KeyPayload {
	t.Helper()
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.T_int64.ToType())
	defer v.Free(mp)
	require.NoError(t, vector.AppendFixed(v, int64(42), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	keys, err := keycodec.NewKeyPayload(context.Background(), [][]byte{data}, 0, func() { released.Add(1) })
	require.NoError(t, err)
	return keys
}

func TestRetainReaderKeysLifetimeAndDiagnostics(t *testing.T) {
	var released atomic.Int32
	keys := readerTestKeys(t, &released)
	rootOwned := true
	defer func() {
		if rootOwned {
			keys.Release()
		}
	}()
	closeErr := moerr.NewInternalErrorNoCtx("reader close failed")
	first, second := &keyTestReader{diagnostics: []*plan.Query{{}}}, &keyTestReader{err: closeErr}
	readers, err := RetainReaderKeys([]engine.Reader{first, second}, keys)
	require.NoError(t, err)
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()
	keys.Release()
	rootOwned = false
	require.Zero(t, released.Load())
	diagnostic := readers[0].(engine.ExplainDiagnosticReader)
	require.Len(t, diagnostic.TakeExplainDiagnostics(), 1)
	require.Empty(t, diagnostic.TakeExplainDiagnostics())
	require.NoError(t, readers[0].Close())
	require.NoError(t, readers[0].Close())
	require.EqualValues(t, 1, first.closed.Load())
	require.Zero(t, released.Load())
	require.NoError(t, keys.Visit(context.Background(), func(v *vector.Vector) error {
		require.EqualValues(t, 42, vector.MustFixedColNoTypeCheck[int64](v)[0])
		return nil
	}))
	require.ErrorIs(t, readers[1].Close(), closeErr)
	require.ErrorIs(t, readers[1].Close(), closeErr)
	require.EqualValues(t, 1, second.closed.Load())
	require.EqualValues(t, 1, released.Load())
	require.False(t, keys.Retain())
}

func TestRetainReaderKeysConcurrentClose(t *testing.T) {
	var released atomic.Int32
	keys := readerTestKeys(t, &released)
	source := &keyTestReader{}
	readers, err := RetainReaderKeys([]engine.Reader{source}, keys)
	require.NoError(t, err)
	keys.Release()
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() { _ = readers[0].Close() })
	}
	workers.Wait()
	require.EqualValues(t, 1, source.closed.Load())
	require.EqualValues(t, 1, released.Load())
}

func TestRetainReaderKeysRejectsReleasedOwner(t *testing.T) {
	var released atomic.Int32
	keys := readerTestKeys(t, &released)
	keys.Release()
	first, second := &keyTestReader{}, &keyTestReader{}
	readers, err := RetainReaderKeys([]engine.Reader{first, second}, keys)
	require.Error(t, err)
	require.Nil(t, readers)
	require.EqualValues(t, 1, first.closed.Load())
	require.EqualValues(t, 1, second.closed.Load())
	require.EqualValues(t, 1, released.Load())
}

func TestRetainReaderKeysPartialConstruction(t *testing.T) {
	var released atomic.Int32
	keys := readerTestKeys(t, &released)
	rootOwned := true
	t.Cleanup(func() {
		if rootOwned {
			keys.Release()
		}
	})
	first := &keyTestReader{err: moerr.NewInternalErrorNoCtx("reader close failed")}
	last := &keyTestReader{}
	readers, err := RetainReaderKeys([]engine.Reader{first, nil, last}, keys)
	require.Error(t, err)
	require.Nil(t, readers)
	require.EqualValues(t, 1, first.closed.Load())
	require.EqualValues(t, 1, last.closed.Load(), "a close error must not skip remaining readers")
	require.Zero(t, released.Load(), "construction borrows the caller's reference")
	keys.Release()
	rootOwned = false
	require.EqualValues(t, 1, released.Load(), "the partially constructed reader must release its reference")
}

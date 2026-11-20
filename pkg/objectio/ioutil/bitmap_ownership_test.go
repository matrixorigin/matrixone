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

package ioutil

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func bitmapInput(t *testing.T, mp *mpool.MPool) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(1)
	b.Attrs = []string{"v"}
	b.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	t.Cleanup(func() { b.Clean(mp) })
	require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(1), true, mp))
	b.Vecs[0].GetGrouping().Add(0)
	b.SetRowCount(1)
	return b
}

func TestSinkerBitmapBackingStaysChargedInReusableBuffers(t *testing.T) {
	mp := mpool.MustNew("bitmap-sink")
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	sourceMP := mpool.MustNew("bitmap-input")
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	s := NewSinker(-1, []string{"v"}, []types.Type{types.T_int64.ToType()}, mockFactory(nil, nil), mp, nil, WithMemorySizeThreshold(1<<30), WithTailSizeCap(0))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	b := bitmapInput(t, sourceMP)
	t.Cleanup(func() { b.Clean(sourceMP) })
	ask := s.AppendAllocation(b)
	require.NoError(t, s.Write(context.Background(), b))
	before := s.RetainedBitmapBytes()
	require.Positive(t, before)
	require.LessOrEqual(t, mp.AllocatedBytes()+before, ask)
	require.NoError(t, s.trySpill(context.Background()))
	require.Equal(t, before, s.RetainedBitmapBytes(), "cleared reusable vectors still own their bitmap backing")
	require.NoError(t, s.Close())
	require.Zero(t, s.RetainedBitmapBytes())
	require.Zero(t, mp.AllocatedBytes())
}

type blockedBitmapSink struct {
	mockFileSinker
	entered chan struct{}
	release chan struct{}
}

func (s *blockedBitmapSink) Sink(ctx context.Context, b *batch.Batch) error {
	close(s.entered)
	select {
	case <-s.release:
		return s.mockFileSinker.Sink(ctx, b)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func TestSinkerPipelineBitmapOwnership(t *testing.T) {
	for _, mode := range []string{"success", "cancel-before-submit", "prior-error"} {
		t.Run(mode, func(t *testing.T) {
			mp := mpool.MustNew("pipeline-bitmap")
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			factory := func(*mpool.MPool, fileservice.FileService) FileSinker {
				return &blockedBitmapSink{entered: entered, release: release}
			}
			var released atomic.Int32
			s := NewSinker(-1, []string{"v"}, []types.Type{types.T_int64.ToType()}, factory, mp, nil, WithPipelineFlush(), WithRetainedRelease(func() { released.Add(1) }))
			t.Cleanup(func() { s.Close() })
			t.Cleanup(unblock)
			s.pipe.result = &pipelineResult{ctx: ctx, cancel: cancel}
			injected := errors.New("prior pipeline failure")
			if mode == "cancel-before-submit" {
				cancel()
			}
			if mode == "prior-error" {
				s.pipe.result.setError(injected)
			}
			b := bitmapInput(t, mp)
			t.Cleanup(func() { unblock(); s.drainPipeline() })
			// pipelineSubmit transfers ownership on both success and rejection.
			err := s.pipelineSubmit(ctx, []*batch.Batch{b})
			if mode == "success" {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("sink worker did not consume its admitted source")
				}
				require.Positive(t, s.RetainedBitmapBytes(), "source remains live while the sink worker is blocked")
				require.Zero(t, released.Load())
				unblock()
				require.NoError(t, err)
				require.NoError(t, s.drainPipeline())
			} else {
				require.Error(t, err)
				if mode == "prior-error" {
					require.ErrorIs(t, err, injected)
				}
			}
			require.Zero(t, s.RetainedBitmapBytes())
			require.Zero(t, mp.AllocatedBytes())
			require.Equal(t, int32(1), released.Load(), "every accepted/rejected source owner is destroyed once")
		})
	}
}

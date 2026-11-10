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

package fileservice

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type ownedServiceTestReader struct {
	io.Reader
	close func() error
}

func (r *ownedServiceTestReader) Close() error { return r.close() }

type ownedReaderTestService struct {
	FileService
	close func(context.Context)
}

func (f *ownedReaderTestService) Close(ctx context.Context) { f.close(ctx) }

func TestReadCloserWithOwnedFileService(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "reader close failure", err: errors.New("reader close failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			var group sync.WaitGroup
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				group.Wait()
			})
			var readerCloses, serviceCloses atomic.Int32
			var readerFinished, serviceAfterReader atomic.Bool
			contexts := make(chan context.Context, 5)
			reader := &ownedServiceTestReader{Reader: strings.NewReader("owned content"), close: func() error {
				if readerCloses.Add(1) == 1 {
					close(started)
				}
				<-release
				readerFinished.Store(true)
				return tc.err
			}}
			service := &ownedReaderTestService{close: func(ctx context.Context) {
				serviceCloses.Add(1)
				serviceAfterReader.Store(readerFinished.Load())
				contexts <- ctx
			}}
			wrapped := NewReadCloserWithOwnedFileService(ctx, reader, service)
			data, err := io.ReadAll(wrapped)
			require.NoError(t, err)
			require.Equal(t, "owned content", string(data))
			cancel() // The operation's original context, including cancellation, is retained.

			results := make(chan error, 4)
			for range 4 {
				group.Go(func() { results <- wrapped.Close() })
			}
			<-started
			select {
			case err := <-results:
				t.Fatalf("Close returned before its reader finished: %v", err)
			default:
			}
			releaseOnce.Do(func() { close(release) })
			group.Wait()
			for range 4 {
				require.ErrorIs(t, <-results, tc.err)
			}
			require.ErrorIs(t, wrapped.Close(), tc.err)
			require.Equal(t, int32(1), readerCloses.Load())
			require.Equal(t, int32(1), serviceCloses.Load())
			require.True(t, serviceAfterReader.Load())
			require.Len(t, contexts, 1)
			require.Same(t, ctx, <-contexts)
		})
	}
}

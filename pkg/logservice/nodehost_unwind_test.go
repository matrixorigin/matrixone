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

package logservice

import (
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type unwindLogDBFactory struct{ create func() (raftio.ILogDB, error) }

func (f unwindLogDBFactory) Name() string { return "unwind" }
func (f unwindLogDBFactory) Create(config.NodeHostConfig, config.LogDBCallback, []string, []string) (raftio.ILogDB, error) {
	return f.create()
}

type unwindLock struct {
	io.Closer
	closes int32
}

func (l *unwindLock) Close() error { atomic.AddInt32(&l.closes, 1); return l.Closer.Close() }

type unwindFS struct {
	vfs.FS
	locks []*unwindLock
}

func (f *unwindFS) Lock(name string) (io.Closer, error) {
	l, err := f.FS.Lock(name)
	if err != nil {
		return nil, err
	}
	tracked := &unwindLock{Closer: l}
	f.locks = append(f.locks, tracked)
	return tracked, nil
}

// Exercise the actual pinned constructor: a MatrixOne-side recovery wrapper
// cannot repair a swallowed panic or release locks hidden inside the dependency.
func TestNodeHostConstructorUnwind(t *testing.T) {
	failure := errors.New("logdb acquisition failed")
	cases := []struct {
		name       string
		run        func() error
		panicValue any
		returned   bool
	}{
		{"error", func() error { return failure }, nil, true},
		{"string_panic", func() error { panic("constructor unwind") }, "constructor unwind", false},
		{"error_panic", func() error { panic(failure) }, failure, false},
		{"goexit", func() error { runtime.Goexit(); return nil }, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := &unwindFS{FS: vfs.NewStrictMem()}
			t.Cleanup(func() {
				// Preserve the missing-destructor oracle before independently cleaning a
				// failing baseline probe; do not disguise it with the fallback Close.
				for _, l := range fs.locks {
					if atomic.LoadInt32(&l.closes) == 0 {
						_ = l.Closer.Close()
					}
				}
				vfs.ReportLeakedFD(fs.FS, t)
			})
			cfg := config.NodeHostConfig{NodeHostDir: "/constructor", RTTMillisecond: 10, RaftAddress: "127.0.0.1:1"}
			cfg.Expert.FS = fs
			called, returned := false, false
			var owner *dragonboat.NodeHost
			var gotErr error
			var gotPanic any
			cfg.Expert.LogDBFactory = unwindLogDBFactory{create: func() (raftio.ILogDB, error) { called = true; return nil, tc.run() }}
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { gotPanic = recover() }()
				owner, gotErr = dragonboat.NewNodeHost(cfg)
				returned = true
			}()
			<-done
			if owner != nil {
				owner.Close()
				t.Fatal("failed construction published a host")
			}
			require.True(t, called)
			require.Equal(t, tc.returned, returned)
			// Interface equality preserves the error pointer, not just its message.
			require.True(t, gotPanic == tc.panicValue, "panic identity changed: got %#v, want %#v", gotPanic, tc.panicValue)
			if tc.returned {
				require.True(t, gotErr == failure, "acquisition error identity changed")
			}
			require.Len(t, fs.locks, 1)
			require.Equal(t, int32(1), atomic.LoadInt32(&fs.locks[0].closes), "owned lock must close before unwind completes")
		})
	}
}

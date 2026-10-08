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

package lockservice

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestLockServiceTestSocketDirectoriesAreIsolated(t *testing.T) {
	const count = 64

	dirs := make(chan string, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dir, err := createTestSocketDir()
			if err != nil {
				errs <- err
				return
			}
			dirs <- dir
		}()
	}
	wg.Wait()
	close(dirs)
	close(errs)

	seen := make(map[string]struct{}, count)
	for dir := range dirs {
		t.Cleanup(func() {
			_ = removeTestSocketDir(dir)
		})
		_, exists := seen[dir]
		require.False(t, exists, "duplicate test socket directory: %s", dir)
		seen[dir] = struct{}{}
	}
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, seen, count)

	for dir := range seen {
		require.NoError(t, removeTestSocketDir(dir))
		_, err := os.Stat(dir)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}

func TestTopologyCleanupAttemptsEveryResourceInOrder(t *testing.T) {
	closeErr := errors.New("injected service close failure")
	var closed []string
	dir, err := createTestSocketDir()
	require.NoError(t, err)
	cleanup := testTopologyCleanup{
		serviceClosers: []func() error{
			func() error {
				closed = append(closed, "service-0")
				return closeErr
			},
			func() error {
				closed = append(closed, "service-1")
				return nil
			},
		},
		allocatorCloser: func() error {
			closed = append(closed, "allocator")
			return nil
		},
		clusterCloser: func() {
			closed = append(closed, "cluster")
		},
		socketDir: dir,
	}

	err = cleanup.close()
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, []string{"service-0", "service-1", "allocator", "cluster"}, closed)
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestTopologyCleanupSupportsPartialConstruction(t *testing.T) {
	var closed []string
	dir, err := createTestSocketDir()
	require.NoError(t, err)
	cleanup := testTopologyCleanup{
		serviceClosers: []func() error{func() error {
			closed = append(closed, "service-0")
			return nil
		}},
		clusterCloser: func() {
			closed = append(closed, "cluster")
		},
		socketDir: dir,
	}

	require.NoError(t, cleanup.close())
	require.Equal(t, []string{"service-0", "cluster"}, closed)
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRunLockServicesForTestCleansUpAfterCallbackPanic(t *testing.T) {
	panicValue := errors.New("injected callback panic")
	var lockService *service
	var socketDir string
	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		runLockServiceTests(
			t,
			[]string{"s1"},
			func(_ *lockTableAllocator, services []*service) {
				lockService = services[0]
				socketPath := strings.TrimPrefix(lockService.GetConfig().ListenAddress, "unix://")
				socketDir = filepath.Dir(socketPath)
				panic(panicValue)
			},
		)
	}()

	require.Same(t, panicValue, recovered)
	lockService.lifecycle.RLock()
	closing := lockService.lifecycle.closing
	lockService.lifecycle.RUnlock()
	require.True(t, closing)
	_, err := os.Stat(socketDir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRunLockServicesForTestStartsAllocatorBeforeServices(t *testing.T) {
	const disconnectDuration = 123 * time.Millisecond
	constructed := 0
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2"},
		time.Second,
		func(allocator *lockTableAllocator, services []*service) {
			require.Len(t, services, 2)
			require.Equal(t, 2, constructed)
			require.Equal(t, disconnectDuration, allocator.options.removeDisconnectDuration)
			for _, service := range services {
				require.Equal(t, disconnectDuration, service.cfg.removeDisconnectDuration)
			}
		},
		func(cfg *Config) {
			cfg.removeDisconnectDuration = disconnectDuration
		},
		func(service *service) {
			constructed++
			// Options run inside NewLockService before its keeper starts. A real
			// connection at this point proves allocator readiness without sleeps
			// or depending on which goroutine wins the startup race.
			serviceSocket := strings.TrimPrefix(service.cfg.ListenAddress, "unix://")
			allocatorSocket := filepath.Join(filepath.Dir(serviceSocket), "allocator.sock")
			conn, err := net.DialTimeout("unix", allocatorSocket, 10*time.Second)
			if err != nil {
				t.Errorf("allocator must listen before service construction: %v", err)
				return
			}
			require.NoError(t, conn.Close())
		},
	)
}

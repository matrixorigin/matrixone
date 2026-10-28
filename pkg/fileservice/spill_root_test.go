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

package fileservice

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"
)

func TestSpillRootErrorSentinelsRemainDistinct(t *testing.T) {
	sentinels := []error{ErrSpillRootFenced, ErrSpillRootOwned, ErrSpillRootClosed}
	for i, sentinel := range sentinels {
		wrapped := errors.Join(sentinel, os.ErrPermission)
		for j, target := range sentinels {
			require.Equal(t, i == j, errors.Is(wrapped, target))
		}
		require.ErrorIs(t, wrapped, os.ErrPermission)
	}
}

func TestAnonymousSpillUnlinkFailure(t *testing.T) {
	name := filepath.Join(t.TempDir(), "partial")
	failure := errors.New("injected unlink failure")
	var opened *os.File
	file, err := createAndRemoveFile(name, func(name string) (*os.File, error) {
		var err error
		opened, err = os.Create(name)
		return opened, err
	}, func(string) error { return failure })
	require.ErrorIs(t, err, failure)
	require.Nil(t, file, "an unlinked descriptor must not be published on failure")
	require.NotNil(t, opened)
	_, err = opened.Write([]byte{1})
	require.ErrorIs(t, err, os.ErrClosed)
	info, err := os.Stat(name)
	require.NoError(t, err)
	require.Zero(t, info.Size())
}

func TestAnonymousSpillUnlinkPreservesCloseError(t *testing.T) {
	name := filepath.Join(t.TempDir(), "closed")
	failure := errors.New("unlink failure")
	file, err := createAndRemoveFile(name, func(name string) (*os.File, error) {
		fd, err := os.Create(name)
		if err != nil {
			return nil, err
		}
		require.NoError(t, fd.Close())
		return fd, nil
	}, func(string) error { return failure })
	require.Nil(t, file)
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, err, os.ErrClosed)
}

type spillRemoveFailure struct {
	MutableFileService
	fail        func(string) error
	afterCreate func(string)
	create      atomic.Int32
}

func (f *spillRemoveFailure) CreateFile(ctx context.Context, name string) (*os.File, error) {
	f.create.Add(1)
	fd, err := f.MutableFileService.CreateFile(ctx, name)
	if err == nil && f.afterCreate != nil {
		f.afterCreate(name)
	}
	return fd, err
}

func (f *spillRemoveFailure) RemoveFile(ctx context.Context, name string) error {
	if err := f.fail(name); err != nil {
		return err
	}
	return f.MutableFileService.RemoveFile(ctx, name)
}

func testSpillRoot(t *testing.T) (*LocalETLFS, *spillFileService) {
	t.Helper()
	local, err := NewLocalETLFS("local", t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(context.Background()) })
	service, err := GetSpillFileService(context.Background(), local, "spill")
	require.NoError(t, err)
	view := service.(*spillFileService)
	// This test-owned release simulates process exit only after all assertions
	// and holders close. Production never clears a fenced incarnation in place.
	t.Cleanup(func() {
		local.Close(context.Background())
		if lease, ok := fencedSpillLeases.LoadAndDelete(view.root.path); ok {
			require.NoError(t, lease.(*os.File).Close())
		}
	})
	return local, view
}

func TestSpillRootFencesEveryViewAfterUnlinkFailure(t *testing.T) {
	ctx := context.Background()
	local, view := testSpillRoot(t)
	failure := errors.New("injected removal failure")
	fault := &spillRemoveFailure{MutableFileService: view.MutableFileService,
		fail: func(string) error { return failure }}
	view = &spillFileService{MutableFileService: fault, root: view.root}
	file, err := view.CreateFile(ctx, "empty")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.ErrorIs(t, view.RemoveFile(ctx, "empty"), failure)
	for i := 0; i < 32; i++ {
		file, err := view.CreateFile(ctx, "must-not-exist")
		require.Nil(t, file)
		require.ErrorIs(t, err, ErrSpillRootFenced)
	}
	require.EqualValues(t, 1, fault.create.Load())
	_, err = GetSpillFileService(ctx, local, "spill")
	require.ErrorIs(t, err, ErrSpillRootFenced)
	entries, err := os.ReadDir(view.root.path)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "empty", entries[0].Name())
}

func TestSpillRootAdmittedCreationCleansAfterConcurrentFence(t *testing.T) {
	ctx := context.Background()
	_, view := testSpillRoot(t)
	first, err := view.CreateFile(ctx, "first")
	require.NoError(t, err)
	require.NoError(t, first.Close())
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	failure := errors.New("first unlink failed")
	fault := &spillRemoveFailure{MutableFileService: view.MutableFileService, fail: func(name string) error {
		if name == "first" {
			return failure
		}
		return nil
	}, afterCreate: func(string) { close(entered); <-release }}
	view = &spillFileService{MutableFileService: fault, root: view.root}
	var workers sync.WaitGroup
	workers.Add(1)
	result := make(chan error, 1)
	go func() {
		defer workers.Done()
		fd, err := view.CreateFile(ctx, "admitted")
		if err == nil {
			err = errors.Join(fd.Close(), view.RemoveFile(ctx, "admitted"))
		}
		result <- err
	}()
	defer func() { releaseBarrier(); workers.Wait() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("creation did not reach publication barrier")
	}
	require.ErrorIs(t, view.RemoveFile(ctx, "first"), failure)
	_, err = view.CreateFile(ctx, "rejected")
	require.ErrorIs(t, err, ErrSpillRootFenced)
	require.EqualValues(t, 1, fault.create.Load())
	releaseBarrier()
	workers.Wait()
	require.NoError(t, <-result)
	require.NoFileExists(t, filepath.Join(view.root.path, "admitted"))
	require.FileExists(t, filepath.Join(view.root.path, "first"))
}

func TestSpillRootFencedAdmissionDoesNotWaitForCleanup(t *testing.T) {
	ctx := context.Background()
	_, view := testSpillRoot(t)
	for _, name := range []string{"first", "blocked"} {
		file, err := view.CreateFile(ctx, name)
		require.NoError(t, err)
		require.NoError(t, file.Close())
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var workers sync.WaitGroup
	defer func() { close(release); workers.Wait() }()
	failure := errors.New("remove unavailable")
	fault := &spillRemoveFailure{MutableFileService: view.MutableFileService,
		fail: func(name string) error {
			if name == "blocked" {
				close(entered)
				<-release
			}
			return failure
		}}
	view = &spillFileService{MutableFileService: fault, root: view.root}
	require.ErrorIs(t, view.RemoveFile(ctx, "first"), failure)
	workers.Add(1)
	go func() { defer workers.Done(); _ = view.RemoveFile(ctx, "blocked") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not reach its barrier")
	}
	rejected := make(chan error, 1)
	workers.Add(1)
	go func() { defer workers.Done(); _, err := view.CreateFile(ctx, "new"); rejected <- err }()
	select {
	case err := <-rejected:
		require.ErrorIs(t, err, ErrSpillRootFenced)
	case <-time.After(5 * time.Second):
		t.Fatal("admission waited behind blocked cleanup")
	}
	require.Zero(t, fault.create.Load())
}

func TestSpillRootLeaseRejectsAliasBeforeDeletingFiles(t *testing.T) {
	ctx := context.Background()
	local, view := testSpillRoot(t)
	file, err := view.CreateFile(ctx, "owned")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	_, err = file.WriteString("still owned")
	require.NoError(t, err)
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(local.rootPath, alias))
	other, err := NewLocalETLFS("other", alias)
	require.NoError(t, err)
	t.Cleanup(func() { other.Close(ctx) })
	_, err = GetSpillFileService(ctx, other, "spill")
	require.ErrorIs(t, err, ErrSpillRootOwned)
	data, err := os.ReadFile(filepath.Join(view.root.path, "owned"))
	require.NoError(t, err)
	require.Equal(t, "still owned", string(data))
}

func TestSpillRootFencedLeaseOutlivesDroppedService(t *testing.T) {
	base := t.TempDir()
	var canonical string
	ref := func() weak.Pointer[LocalETLFS] {
		local, err := NewLocalETLFS("local", base)
		require.NoError(t, err)
		service, err := GetSpillFileService(context.Background(), local, "spill")
		require.NoError(t, err)
		root := service.(*spillFileService).root
		canonical = root.path
		root.fence(errors.New("terminal failure"))
		return weak.Make(local) // deliberately simulate a forgotten Close
	}()
	t.Cleanup(func() {
		if lease, ok := fencedSpillLeases.LoadAndDelete(canonical); ok {
			require.NoError(t, lease.(*os.File).Close())
		}
	})
	require.Eventually(t, func() bool { runtime.GC(); return ref.Value() == nil }, 5*time.Second, 10*time.Millisecond)
	other, err := NewLocalETLFS("replacement", base)
	require.NoError(t, err)
	t.Cleanup(func() { other.Close(context.Background()) })
	_, err = GetSpillFileService(context.Background(), other, "spill")
	require.ErrorIs(t, err, ErrSpillRootOwned)
}

type startupSpillFault struct {
	*LocalETLFS
	listErr, deleteErr error
	listed, release    chan struct{}
}

func (f *startupSpillFault) List(ctx context.Context, name string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		if f.listed != nil {
			close(f.listed)
			<-f.release
		}
		if f.listErr != nil {
			yield(nil, f.listErr)
			return
		}
		f.LocalETLFS.List(ctx, name)(yield)
	}
}

func (f *startupSpillFault) Delete(ctx context.Context, names ...string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.LocalETLFS.Delete(ctx, names...)
}

func TestSpillRootStartupFailureIsNotPublished(t *testing.T) {
	for _, phase := range []string{"list", "delete", "cancel"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			base := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(base, "spill"), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(base, "spill", "old"), []byte("old"), 0600))
			local, err := NewLocalETLFS("local", base)
			require.NoError(t, err)
			fault := &startupSpillFault{LocalETLFS: local}
			failure := errors.New("injected startup " + phase)
			if phase == "list" {
				fault.listErr = failure
			}
			if phase == "delete" {
				fault.deleteErr = failure
			}
			if phase == "cancel" {
				cancel()
				failure = context.Canceled
			}
			view, err := GetSpillFileService(ctx, fault, "spill")
			require.Nil(t, view)
			require.ErrorIs(t, err, failure)
			require.FileExists(t, filepath.Join(base, "spill", "old"))
			local.Close(context.Background())
			if lease, ok := fencedSpillLeases.LoadAndDelete(filepath.Join(base, "spill")); ok {
				require.NoError(t, lease.(*os.File).Close())
			}
		})
	}
}

func TestSpillRootCloseDuringInitializationRetainsLeaseUntilIOStops(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(base, "spill"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(base, "spill", "old"), []byte("old"), 0600))
	local, err := NewLocalETLFS("local", base)
	require.NoError(t, err)
	fault := &startupSpillFault{LocalETLFS: local, listed: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fault.release) }) }
	var worker sync.WaitGroup
	worker.Add(1)
	go func() { defer worker.Done(); _, err := GetSpillFileService(ctx, fault, "spill"); done <- err }()
	defer func() { release(); worker.Wait(); local.Close(ctx) }()
	select {
	case <-fault.listed:
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not reach barrier")
	}
	local.Close(ctx)
	other, err := NewLocalETLFS("other", base)
	require.NoError(t, err)
	t.Cleanup(func() { other.Close(ctx) })
	_, err = GetSpillFileService(ctx, other, "spill")
	require.ErrorIs(t, err, ErrSpillRootOwned)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = GetSpillFileService(canceled, local, "spill")
	require.ErrorIs(t, err, context.Canceled)
	release()
	worker.Wait()
	require.ErrorIs(t, <-done, ErrSpillRootClosed)
	require.Empty(t, local.dirFiles, "late startup Delete must not leave directory descriptors after Close")
}

func TestSpillRootAnonymousAndNamedRotation(t *testing.T) {
	for _, kind := range []string{"local", "etl"} {
		t.Run(kind, func(t *testing.T) {
			ctx, base := context.Background(), t.TempDir()
			var local MutableFileService
			var err error
			if kind == "local" {
				local, err = NewLocalFS(ctx, "local", base, DisabledCacheConfig, nil)
			} else {
				local, err = NewLocalETLFS("local", base)
			}
			require.NoError(t, err)
			t.Cleanup(func() { local.Close(ctx) })
			view, err := GetSpillFileService(ctx, local, "spill")
			require.NoError(t, err)
			fd, err := view.CreateAndRemoveFile(ctx, "anonymous")
			require.NoError(t, err)
			require.NoFileExists(t, filepath.Join(base, "spill", "anonymous"))
			_, err = fd.WriteString("anonymous payload")
			require.NoError(t, err)
			_, err = fd.Seek(0, io.SeekStart)
			require.NoError(t, err)
			data, err := io.ReadAll(fd)
			require.NoError(t, err)
			require.Equal(t, "anonymous payload", string(data))
			require.NoError(t, fd.Close())
			fd, err = view.CreateFile(ctx, "rotating")
			require.NoError(t, err)
			_, err = fd.WriteString("named payload")
			require.NoError(t, err)
			require.NoError(t, fd.Close())
			fd, err = view.OpenFile(ctx, "rotating")
			require.NoError(t, err)
			data, err = io.ReadAll(fd)
			require.NoError(t, err)
			require.Equal(t, "named payload", string(data))
			require.NoError(t, fd.Close())
			require.NoError(t, view.RemoveFile(ctx, "rotating"))
			require.NoError(t, view.RemoveFile(ctx, "rotating"))
		})
	}
}

func TestSpillRootRejectsScratchDirectorySymlink(t *testing.T) {
	ctx, base := context.Background(), t.TempDir()
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "keep"), []byte("keep"), 0600))
	require.NoError(t, os.Symlink(outside, filepath.Join(base, "spill")))
	local, err := NewLocalETLFS("local", base)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(ctx) })
	view, err := GetSpillFileService(ctx, local, "spill")
	require.ErrorContains(t, err, "not a symlink")
	require.Nil(t, view)
	require.FileExists(t, filepath.Join(outside, "keep"))
}

func TestSpillRootRejectsTraversalWithoutFencing(t *testing.T) {
	local, view := testSpillRoot(t)
	outside := filepath.Join(local.rootPath, "outside")
	require.NoError(t, os.WriteFile(outside, []byte("keep"), 0600))
	for _, name := range []string{"../outside", "a/../../outside", ".", "..", ""} {
		_, err := view.CreateFile(context.Background(), name)
		require.Error(t, err)
		require.Error(t, view.RemoveFile(context.Background(), name))
		require.Error(t, view.Delete(context.Background(), name))
	}
	data, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, "keep", string(data))
	require.NoError(t, view.root.Err())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = view.CreateFile(canceled, "canceled")
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, view.root.Err())
}

func TestSpillRootProcessLeaseHelper(t *testing.T) {
	base := os.Getenv("MO_TEST_SPILL_LEASE_ROOT")
	if base == "" {
		return
	}
	local, err := NewLocalETLFS("local", base)
	require.NoError(t, err)
	view, err := GetSpillFileService(context.Background(), local, "spill")
	require.NoError(t, err)
	file, err := view.CreateFile(context.Background(), "old-process")
	require.NoError(t, err)
	_, err = file.WriteString("old process still owns this file")
	require.NoError(t, err)
	fmt.Println("SPILL_LEASE_READY")
	_, _ = io.Copy(io.Discard, os.Stdin) // parent terminates this private child
	_ = file.Close()
	local.Close(context.Background())
}

func TestSpillRootProcessRestartCleansOnlyAfterOldOwnerStops(t *testing.T) {
	base := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSpillRootProcessLeaseHelper$", "-test.timeout=25s")
	cmd.Env = append(os.Environ(), "MO_TEST_SPILL_LEASE_ROOT="+base)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	waited := false
	defer func() {
		_ = stdin.Close()
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	scanner := bufio.NewScanner(stdout)
	ready := false
	for scanner.Scan() {
		if scanner.Text() == "SPILL_LEASE_READY" {
			ready = true
			break
		}
	}
	require.True(t, ready, "child must acquire the lease and create its live file")
	local, err := NewLocalETLFS("contender", base)
	require.NoError(t, err)
	_, err = GetSpillFileService(context.Background(), local, "spill")
	require.ErrorIs(t, err, ErrSpillRootOwned)
	require.FileExists(t, filepath.Join(base, "spill", "old-process"))
	local.Close(context.Background())
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait()) // SIGKILL, not a clean shutdown
	waited = true
	restarted, err := NewLocalETLFS("restarted", base)
	require.NoError(t, err)
	t.Cleanup(func() { restarted.Close(context.Background()) })
	view, err := GetSpillFileService(context.Background(), restarted, "spill")
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(base, "spill", "old-process"))
	fd, err := view.CreateFile(context.Background(), "new-process")
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	require.NoError(t, view.RemoveFile(context.Background(), "new-process"))
}

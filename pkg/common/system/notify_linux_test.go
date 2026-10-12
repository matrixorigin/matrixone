// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package system

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
)

func cgroupWatchDirectory(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{cgroupv2CPULimit, cgroupv2MemLimit} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("max\n"), 0o600))
	}
	return dir
}

func waitCgroupWatcher(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cgroup watcher did not stop after cancellation")
	}
}

func stopCgroupWatcher(t *testing.T, st *stopper.Stopper) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		st.Stop()
		close(done)
	}()
	waitCgroupWatcher(t, done)
}

// Count only inotify descriptors, so unrelated runtime/network descriptors do
// not affect the resource assertion. These tests deliberately run serially.
func inotifyDescriptors(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	count := 0
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil {
			require.True(t, os.IsNotExist(err), "%v", err)
			continue
		}
		if target == "anon_inode:inotify" {
			count++
		}
	}
	return count
}

func TestCgroupWatcherIdleStop(t *testing.T) {
	dir := cgroupWatchDirectory(t)
	before := inotifyDescriptors(t)
	for range 3 {
		st := stopper.NewStopper("idle cgroup watcher")
		require.NoError(t, watchCgroupConfig(st, dir, func(string) {
			t.Error("idle watcher unexpectedly delivered an event")
		}))
		stopCgroupWatcher(t, st)
		require.Equal(t, before, inotifyDescriptors(t))
	}
}

func TestCgroupWatcherEvents(t *testing.T) {
	dir := cgroupWatchDirectory(t)
	st := stopper.NewStopper("cgroup events")
	defer stopCgroupWatcher(t, st)
	events := make(chan string, 16)
	require.NoError(t, watchCgroupConfig(st, dir, func(name string) { events <- name }))
	modify := func(name string) {
		file, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY, 0)
		require.NoError(t, err)
		_, err = file.WriteAt([]byte("100\n"), 0)
		require.NoError(t, file.Close())
		require.NoError(t, err)
		select {
		case got := <-events:
			require.Equal(t, name, got)
		case <-time.After(5 * time.Second):
			t.Fatalf("missing %s event", name)
		}
	}
	for _, name := range []string{cgroupv2CPULimit, cgroupv2MemLimit} {
		modify(name)
	}
	// Removal emits IN_IGNORED, which must not request a quota refresh.
	require.NoError(t, os.Remove(filepath.Join(dir, cgroupv2MemLimit)))
	// Inotify delivers events in order: a subsequent CPU event proves the
	// removal was processed before cancelling, without a timing assertion.
	modify(cgroupv2CPULimit)
	stopCgroupWatcher(t, st)
	select {
	case name := <-events:
		t.Fatalf("unexpected refresh for removal: %s", name)
	default:
	}
}

func TestCgroupWatcherCancelDuringEvent(t *testing.T) {
	dir := cgroupWatchDirectory(t)
	before := inotifyDescriptors(t)
	st := stopper.NewStopper("cgroup event cancellation")
	entered, release := make(chan struct{}), make(chan struct{})
	releaseEvent := sync.OnceFunc(func() { close(release) })
	defer func() {
		releaseEvent()
		stopCgroupWatcher(t, st)
	}()
	onEnter := sync.OnceFunc(func() { close(entered) })
	require.NoError(t, watchCgroupConfig(st, dir, func(string) {
		onEnter()
		<-release
	}))
	cancelled := make(chan struct{})
	require.NoError(t, st.RunTask(func(ctx context.Context) {
		<-ctx.Done()
		close(cancelled)
	}))
	require.NoError(t, os.WriteFile(filepath.Join(dir, cgroupv2CPULimit), []byte("100\n"), 0o600))
	waitCgroupWatcher(t, entered)
	done := make(chan struct{})
	go func() {
		st.Stop()
		close(done)
	}()
	waitCgroupWatcher(t, cancelled)
	select {
	case <-done:
		t.Fatal("Stop returned while an event callback was still running")
	default:
	}
	releaseEvent()
	waitCgroupWatcher(t, done)
	require.Equal(t, before, inotifyDescriptors(t))
}

func TestCgroupWatcherSetupFailure(t *testing.T) {
	for _, failure := range []string{"missing cpu", "missing memory", "stopped"} {
		t.Run(failure, func(t *testing.T) {
			dir := cgroupWatchDirectory(t)
			st := stopper.NewStopper("cgroup setup failure")
			defer stopCgroupWatcher(t, st)
			switch failure {
			case "missing cpu":
				require.NoError(t, os.Remove(filepath.Join(dir, cgroupv2CPULimit)))
			case "missing memory":
				require.NoError(t, os.Remove(filepath.Join(dir, cgroupv2MemLimit)))
			case "stopped":
				st.Stop()
			}
			before := inotifyDescriptors(t)
			err := watchCgroupConfig(st, dir, func(string) { t.Error("event after setup failure") })
			if failure == "stopped" {
				require.ErrorIs(t, err, stopper.ErrUnavailable)
			} else {
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			require.Equal(t, before, inotifyDescriptors(t))
		})
	}
}

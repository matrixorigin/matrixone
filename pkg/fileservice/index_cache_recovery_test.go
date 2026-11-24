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
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func recoveryFiles(data []byte) []IndexCacheFile {
	return []IndexCacheFile{{Name: "data",
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }}}
}

// recoveryMove publishes name in a scratch cache and moves it into c's directory, where c
// has not indexed it (an entry of another instance on the directory).
func recoveryMove(t *testing.T, c *IndexFileCache, name string, data []byte) string {
	t.Helper()
	ctx := context.Background()
	earlier, err := newIndexFileCache(ctx, t.TempDir(), fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
	require.NoError(t, err)
	<-earlier.disk.load.done
	defer earlier.Close(ctx)
	written, err := earlier.Put(ctx, name, recoveryFiles(data))
	require.NoError(t, err)
	require.True(t, written)
	from, err := earlier.disk.pathForDir(name)
	require.NoError(t, err)
	to, err := c.disk.pathForDir(name)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(to), 0o755))
	require.NoError(t, os.Rename(from, to))
	return to
}

func recoveryChmodAll(dir string) {
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		_ = os.Chmod(p, 0o755)
		return nil
	})
	m, _ := filepath.Glob(filepath.Join(dir, "*", "*"))
	for _, p := range m {
		_ = os.Chmod(p, 0o755)
	}
}

// Idle collection of an entry the cache has not indexed and cannot list keeps it as trash
// of unknown bytes, which holds the cache, also after a restart, as a Delete does.
func TestIndexFileCacheIdleRemovalChargesUnlistableEntry(t *testing.T) {
	for _, by := range []string{"idle", "delete control"} {
		t.Run(by, func(t *testing.T) {
			if !permissionsEnforced(t) {
				t.Skip("permissions are not enforced")
			}
			ctx := context.Background()
			dir := t.TempDir()
			capacity := int64(64 << 20)
			data := bytes.Repeat([]byte{7}, 300<<10)
			c, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(capacity), "", "", 0, time.Hour, 0)
			require.NoError(t, err)
			<-c.disk.load.done
			t.Cleanup(func() { recoveryChmodAll(dir) })
			written, err := c.Put(ctx, "kept", recoveryFiles(data))
			require.NoError(t, err)
			require.True(t, written)
			hold, err := c.Hold("kept")
			require.NoError(t, err)
			entry := recoveryMove(t, c, "entry", data)
			require.NoError(t, os.Chmod(entry, 0))
			used := c.disk.cache.Used()
			require.Positive(t, used)
			if by == "idle" {
				n, err := c.collectIdle(ctx, time.Now().Add(2*time.Hour))
				t.Logf("collectIdle: n %d err %v", n, err)
			} else {
				require.NoError(t, c.Delete(ctx, "entry"))
			}
			trash, _ := filepath.Glob(entry + ".*" + cacheDirTrashSuffix)
			require.Len(t, trash, 1)
			t.Logf("trash name %s, trash.bytes %d, unknown %d, capacity %d, used %d",
				filepath.Base(trash[0]), c.disk.trash.bytes.Load(), c.disk.trash.unknown, c.disk.capacityFunc(), used)
			if !(c.disk.trash.bytes.Load() > 0 || c.disk.capacityFunc() <= used) {
				t.Errorf("the unremoved trash is charged nothing: capacity %d with %d used", c.disk.capacityFunc(), used)
			}
			hold.Release()
			c.Close(ctx)
			c2, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(capacity), "", "", 0, 0, 0)
			require.NoError(t, err)
			defer c2.Close(ctx)
			<-c2.disk.load.done
			used2 := c2.disk.cache.Used()
			t.Logf("after restart: trash.bytes %d, capacity %d, used %d", c2.disk.trash.bytes.Load(), c2.disk.capacityFunc(), used2)
			require.True(t, c2.disk.trash.bytes.Load() > 0 || c2.disk.capacityFunc() <= used2,
				"after a restart the unremoved trash is charged nothing")
		})
	}
}

// A Put that started before the load found trash of unknown bytes publishes nothing while
// the load holds publications.
func TestIndexFileCacheHoldStopsPublicationInFlight(t *testing.T) {
	for _, by := range []string{"in flight", "control"} {
		t.Run(by, func(t *testing.T) {
			if !permissionsEnforced(t) {
				t.Skip("permissions are not enforced")
			}
			ctx := context.Background()
			dir := t.TempDir()
			data := bytes.Repeat([]byte{7}, 300<<10)
			c, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			defer c.Close(ctx)
			<-c.disk.load.done
			t.Cleanup(func() { recoveryChmodAll(dir) })
			kept := recoveryMove(t, c, "kept", data)
			entry := recoveryMove(t, c, "entry", data)
			require.NoError(t, os.Chmod(entry, 0))
			fresh, err := c.disk.pathForDir("fresh")
			require.NoError(t, err)
			reached := make(chan struct{})
			release := make(chan struct{})
			putDone := make(chan bool, 1)
			startPut := func() {
				go func() {
					written, err := c.Put(ctx, "fresh", recoveryFiles(data))
					require.NoError(t, err)
					putDone <- written
				}()
			}
			c.disk.hooks.beforeFilePublication = func() {
				close(reached)
				<-release
			}
			c.disk.hooks.beforeRemoveUnlistedDir = func(path string) {
				if by == "in flight" {
					startPut()
					<-reached
				}
			}
			c.disk.hooks.beforeIndexLoadedDirs = func() {
				if by == "control" {
					startPut()
				}
				require.True(t, c.disk.trashHoldsPublications(), "the load holds publications")
				close(release)
				<-putDone
			}
			c.disk.loadCache(ctx)
			c.disk.hooks = diskCacheHooks{}
			require.True(t, c.disk.cache.Contains(kept))
			require.False(t, c.disk.cache.Contains(fresh), "published while the load holds publications")
		})
	}
}

// An eviction whose rename fails keeps the entry indexed, as a failed Delete does.
func TestIndexFileCacheEvictionRenameFailureKeepsEntry(t *testing.T) {
	for _, by := range []string{"eviction", "delete control"} {
		t.Run(by, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			data := bytes.Repeat([]byte{7}, 300<<10)
			c := newTestIndexFileCache(t, dir, 400<<10)
			t.Cleanup(func() { recoveryChmodAll(dir) })
			written, err := c.Put(ctx, "a", recoveryFiles(data))
			require.NoError(t, err)
			require.True(t, written)
			a, err := c.disk.pathForDir("a")
			require.NoError(t, err)
			size, _ := c.disk.cache.Size(a)
			shard := filepath.Dir(a)
			var other string
			for i := 0; ; i++ {
				other = "b" + string(rune('a'+i%26)) + string(rune('a'+i/26))
				p, err := c.disk.pathForDir(other)
				require.NoError(t, err)
				if filepath.Dir(p) != shard {
					break
				}
			}
			require.NoError(t, os.Chmod(shard, 0o500))
			if f, err := os.Create(filepath.Join(shard, "probe")); err == nil {
				_ = f.Close()
				t.Skip("permissions are not enforced")
			}
			if by == "eviction" {
				_, err = c.Put(ctx, other, recoveryFiles(data))
				require.NoError(t, err)
			} else {
				require.Error(t, c.Delete(ctx, "a"))
			}
			_, statErr := os.Stat(a)
			onDisk := statErr == nil
			t.Logf("a on disk %v, indexed %v, trash.bytes %d, used %d, capacity %d",
				onDisk, c.disk.cache.Contains(a), c.disk.trash.bytes.Load(), c.disk.cache.Used(), c.disk.capacityFunc())
			require.True(t, onDisk)
			require.True(t, c.disk.cache.Contains(a), "the entry left on disk is indexed")
			got, _ := c.disk.cache.Size(a)
			require.Equal(t, size, got)
			require.LessOrEqual(t, c.disk.cache.Used(), c.disk.capacityFunc())
		})
	}
}

// The load indexes an entry with the bytes it holds when indexed, also when another
// instance replaced it after it was measured.
func TestIndexFileCacheLoadIndexesCurrentSize(t *testing.T) {
	for _, by := range []string{"replaced", "control"} {
		t.Run(by, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			c, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			defer c.Close(ctx)
			<-c.disk.load.done
			entry := recoveryMove(t, c, "entry", bytes.Repeat([]byte{7}, 100<<10))
			other, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			<-other.disk.load.done
			defer other.Close(ctx)
			c.disk.hooks.beforeIndexLoadedDirs = func() {
				if by == "replaced" {
					require.NoError(t, other.Delete(ctx, "entry"))
					written, err := other.Put(ctx, "entry", recoveryFiles(bytes.Repeat([]byte{8}, 2<<20)))
					require.NoError(t, err)
					require.True(t, written)
				}
			}
			c.disk.loadCache(ctx)
			c.disk.hooks = diskCacheHooks{}
			indexed, ok := c.disk.cache.Size(entry)
			require.True(t, ok)
			actual, err := cacheDirSize(entry)
			require.NoError(t, err)
			t.Logf("indexed %d, on disk %d", indexed, actual)
			require.Equal(t, actual, indexed, "indexed with the bytes of the entry it replaced")
		})
	}
}

// An entry whose trash cannot be removed does not stop the idle collection of the entries
// after it in the same pass.
func TestIndexFileCacheIdleCollectionPassesUnremovableTrash(t *testing.T) {
	for _, by := range []string{"fault", "control"} {
		t.Run(by, func(t *testing.T) {
			if by == "fault" && !permissionsEnforced(t) {
				t.Skip("permissions are not enforced")
			}
			ctx := context.Background()
			dir := t.TempDir()
			data := bytes.Repeat([]byte{7}, 100<<10)
			c, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(64<<20), "", "", 0, time.Hour, 0)
			require.NoError(t, err)
			defer c.Close(ctx)
			<-c.disk.load.done
			t.Cleanup(func() { recoveryChmodAll(dir) })
			var paths []string
			for i := 0; i < 8; i++ {
				name := "e" + string(rune('a'+i))
				written, err := c.Put(ctx, name, recoveryFiles(data))
				require.NoError(t, err)
				require.True(t, written)
				p, _ := c.disk.pathForDir(name)
				paths = append(paths, p)
			}
			dirs, err := c.entryDirs()
			require.NoError(t, err)
			if by == "fault" {
				require.NoError(t, os.Chmod(dirs[0], 0o500))
			}
			n, err := c.collectIdle(ctx, time.Now().Add(2*time.Hour))
			left, _ := c.entryDirs()
			t.Logf("collected %d, err %v, entries left %d, indexed used %d", n, err, len(left), c.disk.cache.Used())
			require.Empty(t, left, "idle entries are left after the pass")
		})
	}
}

// Concurrent puts, opens, holds, deletes, evictions, idle collection and trash
// retries finish and leave the directory and the index in agreement.
func TestIndexFileCacheConcurrentLifecycleAgreement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(1<<20), "", "", 0, time.Hour, 0)
	require.NoError(t, err)
	defer c.Close(ctx)
	<-c.disk.load.done
	names := []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7"}
	stop := time.Now().Add(3 * time.Second)
	done := make(chan struct{})
	var workers int
	run := func(f func(i int)) {
		workers++
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; time.Now().Before(stop); i++ {
				f(i)
			}
		}()
	}
	for w := 0; w < 3; w++ {
		run(func(i int) {
			n := names[(i*7+w)%len(names)]
			_, _ = c.Put(ctx, n, recoveryFiles(bytes.Repeat([]byte{byte(i)}, (100+i%5*50)<<10)))
		})
		run(func(i int) {
			n := names[(i*5+w)%len(names)]
			if fs, err := c.Open(ctx, n, "data"); err == nil {
				for _, f := range fs {
					_ = f.Close()
				}
			}
		})
	}
	run(func(i int) { _ = c.Delete(ctx, names[i%len(names)]) })
	run(func(i int) {
		h, err := c.Hold(names[(i*3)%len(names)])
		if err == nil {
			time.Sleep(time.Millisecond)
			h.Release()
		}
	})
	run(func(i int) {
		if h := c.HoldCached(names[(i*11)%len(names)]); h != nil {
			h.ReleaseSuperseded()
		}
	})
	run(func(i int) { _, _ = c.collectIdle(ctx, time.Now().Add(2*time.Hour)); time.Sleep(time.Millisecond) })
	run(func(i int) { c.disk.retryTrash(); c.disk.cache.ForceEvict(ctx, 200<<10); time.Sleep(time.Millisecond) })
	timeout := time.After(60 * time.Second)
	for i := 0; i < workers; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatal("deadlock: workers did not finish")
		}
	}
	dirs, err := c.entryDirs()
	require.NoError(t, err)
	var sum int64
	for _, p := range dirs {
		size, ok := c.disk.cache.Size(p)
		require.True(t, ok, "entry on disk not indexed: %s", p)
		actual, err := cacheDirSize(p)
		require.NoError(t, err)
		require.Equal(t, actual, size, "indexed size of %s", p)
		sum += size
	}
	require.Equal(t, sum, c.disk.cache.Used(), "used bytes match the entries on disk")
	trash, _ := filepath.Glob(filepath.Join(dir, "*", "*"+cacheDirTrashSuffix))
	require.Empty(t, trash)
	require.Zero(t, c.disk.trash.bytes.Load())
}

// The load finishes when it indexes an entry larger than the capacity that it cannot
// rename away: the eviction that fails is not rescheduled while the load owns the path.
func TestIndexFileCacheLoadFinishesWhenEvictionCannotRemove(t *testing.T) {
	for _, by := range []string{"rename denied", "control"} {
		t.Run(by, func(t *testing.T) {
			if by != "control" && !permissionsEnforced(t) {
				t.Skip("permissions are not enforced")
			}
			ctx := context.Background()
			dir := t.TempDir()
			c1, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(64<<20), "", "", 0, 0, 0)
			require.NoError(t, err)
			<-c1.disk.load.done
			written, err := c1.Put(ctx, "entry", recoveryFiles(bytes.Repeat([]byte{7}, 300<<10)))
			require.NoError(t, err)
			require.True(t, written)
			entry, err := c1.disk.pathForDir("entry")
			require.NoError(t, err)
			c1.Close(ctx)
			shard := filepath.Dir(entry)
			if by != "control" {
				require.NoError(t, os.Chmod(shard, 0o500))
				t.Cleanup(func() { _ = os.Chmod(shard, 0o755) })
			}
			c2, err := newIndexFileCache(ctx, dir, fscache.ConstCapacity(100<<10), "", "", 0, 0, 0)
			require.NoError(t, err)
			defer c2.Close(ctx)
			select {
			case <-c2.disk.load.done:
			case <-time.After(10 * time.Second):
				t.Fatal("the load did not finish")
			}
			_, err = os.Stat(entry)
			if by == "control" {
				require.True(t, os.IsNotExist(err), "the oversized entry is evicted")
			} else {
				require.NoError(t, err)
				require.True(t, c2.disk.cache.Contains(entry), "the entry it cannot remove stays charged")
			}
		})
	}
}

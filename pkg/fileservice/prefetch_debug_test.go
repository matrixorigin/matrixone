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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// /debug/prefetch_objects lists a prefetch object waiting for a connection, and the
// download holding it.
func TestDebugPrefetchObjectsListsWaitingObjects(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	fs.diskCache.maxDownloadConns = 1
	otherKey := fs.pathToKey("other")
	g := newBlackBoxStorage(fs.storage, func(key string, _ int64) bool { return key == otherKey })
	fs.storage = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 10, Ranges: columnRanges(1, 40<<10, 1<<10), Scan: &PrefetchScan{}}))
	page := func() string {
		w := httptest.NewRecorder()
		http.DefaultServeMux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/prefetch_objects", nil))
		return w.Body.String()
	}
	objectPath := fs.diskCache.pathForFile("object")
	require.Eventually(t, func() bool {
		return strings.Contains(page(), "memory "+objectPath+" [0, 1024) waiting for a connection")
	}, 5*time.Second, time.Millisecond, "the waiting object is listed")
	out := page()
	require.Contains(t, out, "file "+fs.diskCache.pathForFile("other")+" [0, ")
	require.Contains(t, out, "download connections 1 of 1")
	release()
	require.Eventually(t, func() bool { return !strings.Contains(page(), objectPath) }, 5*time.Second, time.Millisecond,
		"an object that ended is not listed")
}

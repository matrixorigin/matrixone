// Copyright 2021 Matrix Origin
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

package colexec

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"go.uber.org/zap"
)

const (
	spillCleanupDir      = "cn-spill-cleanup"
	spillCleanupInterval = 10 * time.Second
)

type cnSpillCleanup struct {
	local  fileservice.FileService
	shared fileservice.FileService
	prefix string
	dir    string

	mu      sync.Mutex
	pending [][]byte
	blocked atomic.Bool
}

// StartCNS3Cleanup uses the CN's metadata filesystem to retain failed
// deletions beyond operator teardown. Its one task is owned by the CN stopper.
func (srv *Server) StartCNS3Cleanup(st *stopper.Stopper, serviceID string, local, shared fileservice.FileService) error {
	cleanup := &cnSpillCleanup{local: local, shared: shared, prefix: path.Join(spillCleanupDir, serviceID)}
	if local, ok := local.(interface{ RootPath() string }); ok {
		cleanup.dir = filepath.Join(local.RootPath(), filepath.FromSlash(cleanup.prefix))
	}
	if err := st.RunNamedTask("cn-spill-cleanup", cleanup.run); err != nil {
		return err
	}
	srv.spillCleanup = cleanup
	return nil
}

// WithCNS3SpillCleanup stops new uploads while the CN holds cleanup keys
// that it could neither delete nor record. Already admitted spills keep their
// owner until they finish. Reads and spill-free writes can continue.
func WithCNS3SpillCleanup(serviceID string) ioutil.SinkerOption {
	srv := GetServer(serviceID)
	if srv == nil || srv.spillCleanup == nil {
		return ioutil.WithSpillCheck(nil)
	}
	return ioutil.WithSpillCheck(srv.spillCleanup.checkSpill)
}

func (c *cnSpillCleanup) checkSpill() error {
	if c.blocked.Load() {
		return moerr.NewInvalidStateNoCtx("CN spill cleanup is waiting for shared storage or local metadata")
	}
	return nil
}

// FlushCNS3Cleanup makes one bounded attempt to retire the in-memory fallback.
// The CN calls it after stopping the worker and draining pipeline owners.
func (srv *Server) FlushCNS3Cleanup() error {
	if srv == nil || srv.spillCleanup == nil {
		return nil
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), spillCleanupInterval, moerr.CauseCleanUpUselessFiles)
	defer cancel()
	return srv.spillCleanup.retryPending(ctx)
}

func (c *cnSpillCleanup) remember(fs fileservice.FileService, stats []objectio.ObjectStats) error {
	if services, ok := fs.(*fileservice.FileServices); ok {
		// Unqualified object names are stored in the default service.
		var err error
		if fs, err = fileservice.Get[fileservice.FileService](services, services.Name()); err != nil {
			return err
		}
	}
	if fs == nil || !reflect.ValueOf(fs).Comparable() || fs != c.shared {
		return moerr.NewInternalErrorNoCtx("cannot defer spill cleanup for a different file service")
	}

	ctx, cancel := context.WithTimeoutCause(context.Background(), spillCleanupInterval, moerr.CauseCleanUpUselessFiles)
	defer cancel()
	var journalErr error
	for chunk := range slices.Chunk(stats, 1000) {
		var names strings.Builder
		for i := range chunk {
			names.WriteString(chunk[i].ObjectName().String())
			names.WriteByte('\n')
		}
		data := []byte(names.String())
		if journalErr == nil {
			journalErr = c.writeRecord(ctx, data)
			if journalErr == nil {
				continue
			}
		}

		// Keep only compact keys after a failed record write. Close releases
		// the writer and its buffers once the CN accepts these keys.
		c.mu.Lock()
		c.pending = append(c.pending, data)
		c.blocked.Store(true)
		c.mu.Unlock()
		logutil.Warn("CN retained unpublished spill keys in memory",
			zap.String("objects", names.String()), zap.Error(journalErr))
	}
	return nil
}

func (c *cnSpillCleanup) writeRecord(ctx context.Context, data []byte) error {
	return c.local.Write(ctx, fileservice.IOVector{
		FilePath: path.Join(c.prefix, uuid.NewString()+".names"),
		Entries:  []fileservice.IOEntry{{Size: int64(len(data)), Data: data}},
	})
}

func (c *cnSpillCleanup) retryPending(ctx context.Context) error {
	c.mu.Lock()
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()

	// One drainer runs at a time. Leave the upload gate closed while keys
	// are in this local slice, including when Close adds another batch.
	defer func() {
		c.mu.Lock()
		c.pending = append(c.pending, pending...)
		c.blocked.Store(len(c.pending) > 0)
		c.mu.Unlock()
	}()

	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		data := pending[0]
		writeCtx, cancel := context.WithTimeoutCause(ctx, spillCleanupInterval, moerr.CauseCleanUpUselessFiles)
		writeErr := c.writeRecord(writeCtx, data)
		cancel()
		if writeErr != nil {
			deleteCtx, cancel := context.WithTimeoutCause(ctx, spillCleanupInterval, moerr.CauseCleanUpUselessFiles)
			_, deleteErr := ioutil.DeleteUnpublishedObjects(deleteCtx, c.shared, strings.Fields(string(data))...)
			cancel()
			if deleteErr != nil {
				return errors.Join(writeErr, deleteErr)
			}
		}
		pending[0] = nil
		pending = pending[1:]
	}
	return nil
}

func (c *cnSpillCleanup) run(ctx context.Context) {
	ticker := time.NewTicker(spillCleanupInterval)
	defer ticker.Stop()
	for {
		if err := c.retryPending(ctx); err != nil && ctx.Err() == nil {
			logutil.Warn("CN spill keys retained for retry", zap.Error(err))
		}
		if !c.blocked.Load() {
			c.sweep(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *cnSpillCleanup) sweep(ctx context.Context) {
	if c.dir == "" {
		for entry, err := range c.local.List(ctx, c.prefix) {
			if c.blocked.Load() {
				return
			}
			if err != nil {
				logutil.Warn("failed to list CN spill cleanup records", zap.Error(err))
				return
			}
			if !entry.IsDir {
				c.sweepFile(ctx, entry.Name)
			}
		}
		return
	}

	// Read the disk-backed directory incrementally so an outage backlog does
	// not have to fit on the Go heap.
	dir, err := os.Open(c.dir)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		logutil.Warn("failed to open CN spill cleanup directory", zap.Error(err))
		return
	}
	defer dir.Close()
	for ctx.Err() == nil {
		entries, err := dir.ReadDir(64)
		for _, entry := range entries {
			if c.blocked.Load() {
				return
			}
			if entry.Type().IsRegular() {
				c.sweepFile(ctx, entry.Name())
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				logutil.Warn("failed to list CN spill cleanup records", zap.Error(err))
			}
			return
		}
	}
}

func (c *cnSpillCleanup) sweepFile(ctx context.Context, name string) {
	id, ok := strings.CutSuffix(name, ".names")
	if !ok || uuid.Validate(id) != nil || ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeoutCause(ctx, spillCleanupInterval, moerr.CauseCleanUpUselessFiles)
	defer cancel()
	record := fileservice.IOVector{
		FilePath: path.Join(c.prefix, name),
		Entries:  []fileservice.IOEntry{{Size: -1}},
	}
	defer record.Release()
	err := c.local.Read(ctx, &record)
	if err == nil {
		_, err = ioutil.DeleteUnpublishedObjects(ctx, c.shared, strings.Fields(string(record.Entries[0].Data))...)
	}
	if err == nil {
		err = c.local.Delete(ctx, record.FilePath)
	}
	if err != nil {
		logutil.Warn("CN spill cleanup record retained for retry", zap.String("record", name), zap.Error(err))
	}
}

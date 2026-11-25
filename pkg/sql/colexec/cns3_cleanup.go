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
	"strconv"
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
	spillRecordTimeout   = 5 * time.Second
	spillRecordMaxKeys   = 1000
	spillRecordPageSize  = 64
	spillRecordMaxBytes  = spillRecordMaxKeys * (objectio.NameStringLen + 1)
)

type cnSpillCleanup struct {
	local  fileservice.FileService
	shared fileservice.FileService
	prefix string
	dir    string

	mu      sync.Mutex
	pending []spillCleanupRecord
	blocked atomic.Bool

	// Only the cleanup worker reads or updates these cursors.
	sharedAfter string
	localAfter  string
}

type spillCleanupRecord struct {
	name string
	data []byte
}

// StartCNS3Cleanup replays unpublished spill records from the shared store.
// The CN metadata filesystem can also hold records until shared storage recovers.
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
	for chunk := range slices.Chunk(stats, spillRecordMaxKeys) {
		var names strings.Builder
		for i := range chunk {
			names.WriteString(chunk[i].ObjectName().String())
			names.WriteByte('\n')
		}
		record := spillCleanupRecord{name: uuid.NewString() + ".names", data: []byte(names.String())}
		if journalErr == nil {
			journalErr = c.storeRecord(ctx, record)
			if journalErr == nil {
				continue
			}
		}

		// Keep only compact keys after a failed record write. Close releases
		// the writer and its buffers once the CN accepts these keys.
		c.mu.Lock()
		c.pending = append(c.pending, record)
		c.blocked.Store(true)
		c.mu.Unlock()
		logutil.Warn("CN retained unpublished spill keys in memory",
			zap.String("objects", names.String()), zap.Error(journalErr))
	}
	return nil
}

func (c *cnSpillCleanup) storeRecord(ctx context.Context, record spillCleanupRecord) error {
	err := writeSpillCleanupRecord(ctx, c.shared, spillCleanupDir, record)
	if err == nil {
		return nil
	}
	// LOCAL can be discarded with the CN. Keep the in-memory owner and
	// upload gate until SHARED accepts the record or the objects are deleted.
	return errors.Join(err, writeSpillCleanupRecord(ctx, c.local, c.prefix, record))
}

func writeSpillCleanupRecord(ctx context.Context, fs fileservice.FileService, prefix string, record spillCleanupRecord) error {
	ctx, cancel := context.WithTimeoutCause(ctx, spillRecordTimeout, moerr.CauseCleanUpUselessFiles)
	defer cancel()
	err := fs.Write(ctx, fileservice.IOVector{
		FilePath: path.Join(prefix, record.name),
		Entries:  []fileservice.IOEntry{{Size: int64(len(record.data)), Data: record.data}},
	})
	// Retries and local promotion reuse the same immutable, randomly named record.
	if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
		return nil
	}
	return err
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
		record := pending[0]
		writeErr := c.storeRecord(ctx, record)
		if writeErr != nil {
			deleteCtx, cancel := context.WithTimeoutCause(ctx, spillCleanupInterval, moerr.CauseCleanUpUselessFiles)
			_, deleteErr := ioutil.DeleteUnpublishedObjects(deleteCtx, c.shared, strings.Fields(string(record.data))...)
			cancel()
			if deleteErr != nil {
				return errors.Join(writeErr, deleteErr)
			}
		}
		pending[0] = spillCleanupRecord{}
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
			c.sweepLocal(ctx)
			// Only abandoned keys are recorded here, so any CN can replay them.
			c.sweepListedRecords(ctx, c.shared, spillCleanupDir, &c.sharedAfter, false)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *cnSpillCleanup) sweepLocal(ctx context.Context) {
	if c.dir == "" {
		c.sweepListedRecords(ctx, c.local, c.prefix, &c.localAfter, true)
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
				info, err := c.local.StatFile(ctx, path.Join(c.prefix, entry.Name()))
				if err == nil {
					c.sweepFile(ctx, c.local, c.prefix, entry.Name(), info.Size, true)
				}
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

func (c *cnSpillCleanup) sweepListedRecords(ctx context.Context, fs fileservice.FileService, prefix string, after *string, local bool) {
	listCtx, cancel := context.WithTimeoutCause(ctx, spillCleanupInterval, moerr.CauseCleanUpUselessFiles)
	defer cancel()

	// FileService.List can hold a read lock until iteration finishes. Collect
	// a bounded page first, then perform reads and deletes outside the iterator.
	// Use a name cursor so records that fail repeatedly cannot starve later ones.
	page := make([]fileservice.DirEntry, 0, spillRecordPageSize)
	for entry, err := range fs.List(listCtx, prefix) {
		if c.blocked.Load() || ctx.Err() != nil {
			return
		}
		if err != nil {
			logutil.Warn("failed to list CN spill cleanup records", zap.String("directory", prefix), zap.Error(err))
			break
		}
		if entry.IsDir || entry.Name <= *after {
			continue
		}
		index, found := slices.BinarySearchFunc(page, entry.Name, func(candidate fileservice.DirEntry, name string) int {
			return strings.Compare(candidate.Name, name)
		})
		if found || index == spillRecordPageSize {
			continue
		}
		if len(page) == spillRecordPageSize {
			page = page[:spillRecordPageSize-1]
		}
		page = slices.Insert(page, index, *entry)
	}

	for _, entry := range page {
		if c.blocked.Load() || ctx.Err() != nil {
			return
		}
		c.sweepFile(ctx, fs, prefix, entry.Name, entry.Size, local)
		*after = entry.Name
	}
	if len(page) < spillRecordPageSize {
		*after = ""
	}
}

func (c *cnSpillCleanup) sweepFile(ctx context.Context, fs fileservice.FileService, prefix, name string, size int64, local bool) {
	id, ok := strings.CutSuffix(name, ".names")
	if !ok || len(id) != 36 || uuid.Validate(id) != nil || ctx.Err() != nil {
		return
	}
	if size <= 0 || size > spillRecordMaxBytes {
		logutil.Warn("invalid CN spill cleanup record size", zap.String("record", name), zap.Int64("size", size))
		return
	}
	ctx, cancel := context.WithTimeoutCause(ctx, spillCleanupInterval, moerr.CauseCleanUpUselessFiles)
	defer cancel()
	record := fileservice.IOVector{
		FilePath: path.Join(prefix, name),
		Entries:  []fileservice.IOEntry{{Size: size}},
	}
	defer record.Release()
	if err := fs.Read(ctx, &record); err != nil {
		if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			logutil.Warn("failed to read CN spill cleanup record", zap.String("record", name), zap.Error(err))
		}
		return
	}

	data := record.Entries[0].Data
	keys := strings.Fields(string(data))
	if len(keys) == 0 || len(keys) > spillRecordMaxKeys {
		logutil.Warn("invalid CN spill cleanup record", zap.String("record", name))
		return
	}
	for _, key := range keys {
		if !validSpillObjectName(key) {
			logutil.Warn("invalid object name in CN spill cleanup record", zap.String("record", name))
			return
		}
	}
	// Promote a local record even while object deletion is unavailable. The
	// same marker may already exist if a previous promotion lost its reply.
	if local && writeSpillCleanupRecord(ctx, c.shared, spillCleanupDir, spillCleanupRecord{name: name, data: data}) == nil {
		if err := fs.Delete(ctx, record.FilePath); err != nil {
			logutil.Warn("failed to remove promoted CN spill cleanup record", zap.String("record", name), zap.Error(err))
		}
		return
	}

	_, err := ioutil.DeleteUnpublishedObjects(ctx, c.shared, keys...)
	if err == nil {
		err = fs.Delete(ctx, record.FilePath)
	}
	if err != nil {
		logutil.Warn("CN spill cleanup record retained for retry", zap.String("record", name), zap.Error(err))
	}
}

func validSpillObjectName(name string) bool {
	if len(name) != objectio.NameStringLen || name[36] != '_' || uuid.Validate(name[:36]) != nil {
		return false
	}
	_, err := strconv.ParseUint(name[37:], 10, 16)
	return err == nil
}

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
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	ErrSpillRootFenced = moerr.NewInternalErrorNoCtx("spill scratch root is fenced; CN restart required")
	ErrSpillRootOwned  = moerr.NewInternalErrorNoCtx("spill scratch root has another live owner")
	ErrSpillRootClosed = moerr.NewInternalErrorNoCtx("spill scratch root is closed")
	// A failed root cannot be reinitialized in this process. Retain one OS
	// lease per fenced configured root until process exit, not per-file tasks
	// or query/owner callbacks. Healthy roots release after service drain.
	fencedSpillLeases sync.Map // canonical scratch root -> *os.File
)

// SpillRoot is a CN file-service incarnation, shared by every execution/view.
// No mutex is held across filesystem I/O. File-service Close must follow the
// existing CN shutdown order: stop admission and drain operators first.
type SpillRoot struct {
	mu         sync.Mutex
	path       string
	lease      *os.File
	failure    string
	closed     bool
	operations uint64
}

func (r *SpillRoot) errLocked() error {
	if r.failure != "" {
		return errors.Join(ErrSpillRootFenced, moerr.NewInternalErrorNoCtx(r.failure))
	}
	if r.closed {
		return ErrSpillRootClosed
	}
	return nil
}

func (r *SpillRoot) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errLocked()
}

func (r *SpillRoot) fence(err error) {
	if err == nil {
		return
	}
	// Copy bounded text only: an arbitrary error implementation may itself
	// reference a query, operator, callback or large retained allocation.
	text := err.Error()
	if len(text) > 1024 {
		text = text[:1024]
	}
	if text == "" {
		text = "spill removal failed"
	}
	text = strings.Clone(text) // a short slice must not pin a huge error string
	r.mu.Lock()
	first := r.failure == ""
	if first {
		r.failure = text
	}
	lease := r.lease
	r.mu.Unlock()
	if first {
		// Preserve exclusion even if a failed service is replaced or dropped
		// without Close. Retain only its OS lease, never the service/query.
		if lease != nil {
			fencedSpillLeases.Store(r.path, lease)
		}
		logutil.Errorf("spill scratch root fenced: root=%s cause=%s", r.path, text)
	}
}

func (r *SpillRoot) begin(ctx context.Context, create bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed || (create && r.failure != "") {
		err := r.errLocked()
		r.mu.Unlock()
		return nil, err
	}
	r.operations++
	r.mu.Unlock()
	return r.end, nil
}

func (r *SpillRoot) detachLeaseLocked() (*os.File, bool) {
	if !r.closed || r.operations != 0 {
		return nil, false
	}
	lease := r.lease
	r.lease = nil
	return lease, r.failure != ""
}

func (r *SpillRoot) finishLease(lease *os.File, fenced bool) {
	if lease == nil {
		return
	}
	if fenced {
		fencedSpillLeases.Store(r.path, lease)
	} else {
		_ = lease.Close()
	}
}

func (r *SpillRoot) end() {
	r.mu.Lock()
	r.operations--
	lease, fenced := r.detachLeaseLocked()
	r.mu.Unlock()
	r.finishLease(lease, fenced)
}

func (r *SpillRoot) close() {
	r.mu.Lock()
	r.closed = true
	lease, fenced := r.detachLeaseLocked()
	r.mu.Unlock()
	r.finishLease(lease, fenced)
}

type spillRootHolder struct {
	mu        sync.Mutex
	root      *SpillRoot
	err       error
	ready     chan struct{}
	closed    bool
	directory string
}

func (h *spillRootHolder) get(ctx context.Context, base, directory string, local MutableFileService) (*SpillRoot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, ErrSpillRootClosed
	}
	if h.ready != nil {
		ready := h.ready
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ready:
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.closed {
			return nil, ErrSpillRootClosed
		}
		if directory != h.directory {
			return nil, moerr.NewInternalErrorNoCtx("spill root directory changed within a service incarnation")
		}
		if h.err != nil {
			return nil, h.err
		}
		return h.root, h.root.Err()
	}
	h.ready = make(chan struct{})
	h.directory = directory
	h.mu.Unlock()
	root, err := newSpillRoot(ctx, base, directory, local)
	h.mu.Lock()
	h.root = root
	// Return the original startup error to its caller, but never retain an
	// injected/foreign error object in this service-lifetime holder.
	if err != nil {
		if root != nil {
			h.err = root.Err()
		} else {
			cause := moerr.NewInternalErrorNoCtx("spill root initialization failed")
			if errors.Is(err, ErrSpillRootOwned) {
				cause = ErrSpillRootOwned
			}
			text := err.Error()
			if len(text) > 1024 {
				text = text[:1024]
			}
			h.err = errors.Join(cause, moerr.NewInternalErrorNoCtx(text))
		}
	}
	closed := h.closed
	close(h.ready)
	h.mu.Unlock()
	if closed {
		if root != nil {
			root.close()
		}
		return nil, ErrSpillRootClosed
	}
	return root, err
}

func (h *spillRootHolder) close() {
	h.mu.Lock()
	h.closed = true
	root := h.root
	h.mu.Unlock()
	if root != nil {
		root.close()
	}
}

func (l *LocalFS) spillRootFor(ctx context.Context, directory string, view MutableFileService) (*SpillRoot, error) {
	root, err := l.spillRoot.get(ctx, l.rootPath, directory, view)
	if errors.Is(err, ErrSpillRootClosed) {
		// Startup cleanup may have populated the directory-FD cache after
		// service Close raced its blocked I/O. Its final owner closes that
		// cache too; do not leave late directory descriptors behind.
		closeDirFiles(&l.RWMutex, l.dirFiles)
	}
	return root, err
}

func (l *LocalETLFS) spillRootFor(ctx context.Context, directory string, view MutableFileService) (*SpillRoot, error) {
	root, err := l.spillRoot.get(ctx, l.rootPath, directory, view)
	if errors.Is(err, ErrSpillRootClosed) {
		closeDirFiles(&l.RWMutex, l.dirFiles)
	}
	return root, err
}

func newSpillRoot(ctx context.Context, base, directory string, local MutableFileService) (*SpillRoot, error) {
	if directory == "" || directory == "." || directory == ".." || strings.ContainsAny(directory, "/\\") {
		return nil, moerr.NewInternalErrorNoCtx("invalid local spill directory")
	}
	canonical, err := filepath.EvalSymlinks(base)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(canonical, directory)
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, moerr.NewInternalErrorNoCtx("spill root must be a local directory, not a symlink")
	}
	lease, err := openSpillRootLease(filepath.Join(canonical, "."+directory+".lease"))
	if err != nil {
		return nil, err
	}
	root := &SpillRoot{path: path, lease: lease}
	spill := SubPath(local, directory)
	for entry, listErr := range spill.List(ctx, "/") {
		if listErr != nil {
			err = listErr
			break
		}
		if entry == nil || entry.Name == "" || entry.Name != filepath.Base(entry.Name) || entry.Name == "." || entry.Name == ".." {
			err = moerr.NewInternalErrorNoCtx("invalid startup spill entry")
			break
		}
		if deleteErr := spill.Delete(ctx, entry.Name); deleteErr != nil && !spillFileMissing(deleteErr) {
			err = deleteErr
			break
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		// Some file-service Delete implementations normalize a dangling
		// symlink as missing. Never publish a dirty root on that basis.
		var remaining []os.DirEntry
		remaining, err = os.ReadDir(path)
		if err == nil && len(remaining) != 0 {
			err = moerr.NewInternalErrorNoCtx("spill startup cleanup left directory entries")
		}
	}
	if err != nil {
		root.fence(err)
		return root, err
	}
	return root, nil
}

// GetSpillFileService publishes a view only after exclusive startup cleanup.
// A newly-created subpath wrapper cannot reset the CN root's sticky failure.
func GetSpillFileService(ctx context.Context, local MutableFileService, directory string) (MutableFileService, error) {
	provider, ok := local.(interface {
		spillRootFor(context.Context, string, MutableFileService) (*SpillRoot, error)
	})
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("spill requires a local file service with a root owner")
	}
	root, err := provider.spillRootFor(ctx, directory, local)
	if err != nil {
		return nil, err
	}
	mutable, ok := SubPath(local, directory).(MutableFileService)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("spill subpath is not mutable")
	}
	return &spillFileService{MutableFileService: mutable, root: root}, nil
}

func spillFileMissing(err error) bool {
	return errors.Is(err, os.ErrNotExist) || moerr.IsMoErrCode(err, moerr.ErrFileNotFound)
}

type spillFileService struct {
	MutableFileService
	root *SpillRoot
}

// Spill producers use flat generated names. Reject traversal before SubPath's
// path.Join can erase it; cleanup must never address an unrelated local file.
func (s *spillFileService) validateName(name string) error {
	p, err := ParsePathAtService(name, s.Name())
	if err != nil {
		return err
	}
	if p.File == "" || p.File == "." || p.File == ".." || strings.ContainsAny(p.File, "/\\") {
		return moerr.NewInternalErrorNoCtx("spill requires a flat local filename")
	}
	return nil
}

func (s *spillFileService) OpenFile(ctx context.Context, name string) (*os.File, error) {
	if err := s.validateName(name); err != nil {
		return nil, err
	}
	end, err := s.root.begin(ctx, false)
	if err != nil {
		return nil, err
	}
	defer end()
	return s.MutableFileService.OpenFile(ctx, name)
}

func (s *spillFileService) NewMutator(ctx context.Context, name string) (Mutator, error) {
	if err := s.validateName(name); err != nil {
		return nil, err
	}
	end, err := s.root.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer end()
	return s.MutableFileService.NewMutator(ctx, name)
}

func (s *spillFileService) EnsureDir(ctx context.Context, name string) error {
	p, err := ParsePathAtService(name, s.Name())
	if err != nil {
		return err
	}
	if p.File != "" && p.File != "." {
		return moerr.NewInternalErrorNoCtx("spill subdirectories are not supported")
	}
	end, err := s.root.begin(ctx, true)
	if err != nil {
		return err
	}
	defer end()
	return nil // initialization already created the exclusive root
}

func (s *spillFileService) CreateFile(ctx context.Context, name string) (*os.File, error) {
	if err := s.validateName(name); err != nil {
		return nil, err
	}
	end, err := s.root.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer end()
	return s.MutableFileService.CreateFile(ctx, name)
}

func (s *spillFileService) CreateAndRemoveFile(ctx context.Context, name string) (*os.File, error) {
	if err := s.validateName(name); err != nil {
		return nil, err
	}
	end, err := s.root.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer end()
	file, err := s.MutableFileService.CreateAndRemoveFile(ctx, name)
	var unlink *anonymousUnlinkError
	if errors.As(err, &unlink) {
		s.root.fence(err)
	}
	return file, err
}

func (s *spillFileService) RemoveFile(ctx context.Context, name string) error {
	if err := s.validateName(name); err != nil {
		return err
	}
	end, err := s.root.begin(ctx, false)
	if err != nil {
		return err
	}
	defer end()
	err = s.MutableFileService.RemoveFile(ctx, name)
	if spillFileMissing(err) {
		return nil
	}
	if err != nil {
		s.root.fence(err)
	}
	return err
}

func (s *spillFileService) Write(ctx context.Context, vector IOVector) error {
	if err := s.validateName(vector.FilePath); err != nil {
		return err
	}
	end, err := s.root.begin(ctx, true)
	if err != nil {
		return err
	}
	defer end()
	return s.MutableFileService.Write(ctx, vector)
}

func (s *spillFileService) Delete(ctx context.Context, names ...string) error {
	for _, name := range names {
		if err := s.validateName(name); err != nil {
			return err
		}
	}
	end, err := s.root.begin(ctx, false)
	if err != nil {
		return err
	}
	defer end()
	err = s.MutableFileService.Delete(ctx, names...)
	if spillFileMissing(err) {
		return nil
	}
	if err != nil {
		s.root.fence(err)
	}
	return err
}

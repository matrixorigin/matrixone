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

// Package portlease coordinates test-only TCP port ranges across processes.
package portlease

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	MinPort = uint64(10000)
	MaxPort = uint64(60000)
	Span    = uint64(1000)
)

// Allocator holds only namespace configuration. An empty Directory uses the
// common namespace for the current UID; private directories are for lock-only tests.
// Participating processes must share a UID and this directory's filesystem.
// Other users and applications remain external to the reservation protocol.
type Allocator struct{ Directory string }

// DefaultDirectory is independent of TMPDIR and workspace. Separate test
// processes under the same account must coordinate before binding TCP ports.
func DefaultDirectory() string { return fmt.Sprintf("/tmp/mo-test-port-leases-%d", os.Getuid()) }

// Lease owns an aligned port range until Close or process exit. Lock files must
// never be unlinked: another allocator may already have the same inode open.
type Lease struct {
	base uint64
	lock *flock.Flock
}

func (l *Lease) Base() uint64 { return l.base }
func (l *Lease) Close() error { return l.lock.Close() }

// Acquire makes one bounded, nonblocking scan; it does not wait for capacity.
func (a Allocator) Acquire() (*Lease, error) {
	for base := MinPort; base+Span <= MaxPort; base += Span {
		lease, locked, err := a.TryAcquire(base)
		if err != nil {
			return nil, err
		}
		if locked {
			return lease, nil
		}
	}
	return nil, moerr.NewInternalErrorNoCtx("no test port range is available")
}

func (a Allocator) TryAcquire(base uint64) (*Lease, bool, error) {
	if base < MinPort || base > MaxPort-Span || (base-MinPort)%Span != 0 {
		return nil, false, moerr.NewInvalidInputNoCtxf("invalid test port range base %d", base)
	}
	dir := a.Directory
	if dir == "" {
		dir = DefaultDirectory()
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, false, err
	}
	lock := flock.New(filepath.Join(dir, fmt.Sprintf("%d.lock", base)))
	locked, err := lock.TryLock()
	if err != nil {
		return nil, false, errors.Join(err, lock.Close())
	}
	if !locked {
		return nil, false, lock.Close()
	}
	return &Lease{base: base, lock: lock}, true, nil
}

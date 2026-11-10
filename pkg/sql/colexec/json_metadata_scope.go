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

package colexec

import (
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// JSONMetadataScope accounts bounded Go control maps, slices and their tokens.
// It belongs to one execution generation and is explicitly closed on reset.
type JSONMetadataScope struct {
	selection *vector.AllocationAccountSelection
	admission vector.JSONReaderMemoryAdmission
	releases  []func()
	entries   int
	closed    bool
}

func NewJSONMetadataScope(proc *process.Process, selection *vector.AllocationAccountSelection, entries int) (*JSONMetadataScope, error) {
	admission, err := function.JSONReaderMemoryAdmission(proc)
	if err != nil {
		return nil, err
	}
	return newJSONMetadataScopeWithAdmission(selection, admission, entries)
}

func newJSONMetadataScopeWithAdmission(selection *vector.AllocationAccountSelection, admission vector.JSONReaderMemoryAdmission, entries int) (*JSONMetadataScope, error) {
	if entries <= 0 || selection == nil && admission == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON metadata scope requires entries and accounting")
	}
	capacity, err := vector.JSONReaderContainerMetadataCapacity(unsafe.Sizeof(JSONMetadataScope{}), entries)
	if err != nil {
		return nil, err
	}
	release, err := vector.ReserveJSONReaderMetadata(selection, admission, capacity)
	if err != nil {
		return nil, err
	}
	return &JSONMetadataScope{selection: selection, admission: admission, releases: []func(){release}, entries: entries}, nil
}

func (scope *JSONMetadataScope) Ensure(entries int) error {
	if scope == nil || scope.closed {
		return io.ErrClosedPipe
	}
	if entries < 0 {
		return moerr.NewInvalidInputNoCtx("invalid JSON metadata entries")
	}
	if entries <= scope.entries {
		return nil
	}
	next := entries
	if scope.entries <= int(^uint(0)>>1)/2 && 2*scope.entries > next {
		next = 2 * scope.entries
	}
	capacity, err := vector.JSONReaderContainerMetadataCapacity(0, next-scope.entries)
	if err != nil {
		return err
	}
	release, err := vector.ReserveJSONReaderMetadata(scope.selection, scope.admission, capacity)
	if err != nil {
		return err
	}
	scope.releases = append(scope.releases, release)
	scope.entries = next
	return nil
}

func (scope *JSONMetadataScope) Admission() vector.JSONReaderMemoryAdmission {
	if scope == nil || scope.closed {
		return nil
	}
	return scope.admission
}

func (scope *JSONMetadataScope) Close() {
	if scope == nil || scope.closed {
		return
	}
	scope.closed = true
	for i := len(scope.releases) - 1; i >= 0; i-- {
		if scope.releases[i] != nil {
			scope.releases[i]()
		}
	}
	scope.releases = nil
	scope.selection = nil
	scope.admission = nil
}

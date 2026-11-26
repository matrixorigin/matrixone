// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

func IsMetaEntry(tblName string) bool {
	return IsDataObjectList(tblName) || IsTombstoneObjectList(tblName)
}

func IsDataObjectList(tblName string) bool {
	return matchesObjectListName(tblName, "_data_meta")
}

func IsTombstoneObjectList(tblName string) bool {
	return matchesObjectListName(tblName, "_tombstone_meta")
}

// Object-list labels match an underscore, ASCII digits and a fixed suffix
// anywhere in the name. Keep the containing-match contract of logtail replay.
func matchesObjectListName(name, suffix string) bool {
	// A match needs an underscore and at least one digit before the suffix.
	if len(name) < len(suffix)+2 {
		return false
	}
	for {
		end := strings.Index(name, suffix)
		if end < 0 {
			return false
		}
		start := end
		for start > 0 && name[start-1] >= '0' && name[start-1] <= '9' {
			start--
		}
		if start < end && start > 0 && name[start-1] == '_' {
			return true
		}
		name = name[end+len(suffix):]
	}
}

func IsTransferredDels(name string) bool {
	return strings.HasPrefix(name, "trans_del")
}

// objectListViews pools the view batches of object-list entries, whose
// batches are read only while the entry is handled.
var objectListViews = sync.Pool{New: func() any { return new(batch.Batch) }}

// viewObjectList is mustViewEntryBatch for a batch read only during the
// call; it decodes into a pooled batch and allocates nothing once warm.
// Release the batch with releaseObjectList.
func viewObjectList(data []byte) *batch.Batch {
	bat := objectListViews.Get().(*batch.Batch)
	if err := batch.ViewTrustedBinaryInto(bat, data); err != nil {
		panic(err)
	}
	return bat
}

func releaseObjectList(bat *batch.Batch) {
	batch.DropViews(bat)
	objectListViews.Put(bat)
}

// mustViewEntryBatch decodes the batch of a logtail entry as read-only views
// of data. TN encodes the entry from its own valid batches, so the payload is
// trusted; a malformed frame is a TN bug, and replay cannot continue past it.
func mustViewEntryBatch(data []byte) *batch.Batch {
	bat, err := batch.ViewTrustedBinary(data)
	if err != nil {
		panic(err)
	}
	return bat
}

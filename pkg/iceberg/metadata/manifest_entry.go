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

package metadata

import "github.com/matrixorigin/matrixone/pkg/iceberg/api"

// NormalizeManifestEntry resolves ADDED inheritance exactly once, before an
// entry is paired with deletes or copied to another manifest. V1 absent fields
// and explicit v2 zero values are marked set by the decoder and remain zero.
func NormalizeManifestEntry(entry api.ManifestEntry, manifest api.ManifestFile) (api.ManifestEntry, error) {
	if entry.SequenceNumber < 0 || entry.FileSequence < 0 || manifest.SequenceNumber < 0 {
		return api.ManifestEntry{}, api.NewError(api.ErrMetadataInvalid, "Iceberg manifest entry has negative sequence numbers", nil)
	}
	if entry.Status == api.ManifestEntryAdded {
		if !entry.SequenceNumberSet {
			entry.SequenceNumber = manifest.SequenceNumber
		}
		if !entry.FileSequenceSet {
			entry.FileSequence = manifest.SequenceNumber
		}
	}
	entry.SequenceNumberSet = true
	entry.FileSequenceSet = true
	if entry.SnapshotID == 0 {
		entry.SnapshotID = manifest.AddedSnapshotID
	}
	entry.DataFile.SequenceNumber = entry.SequenceNumber
	entry.DataFile.FileSequenceNumber = entry.FileSequence
	entry.DataFile.SpecID = manifest.PartitionSpecID
	return entry, nil
}

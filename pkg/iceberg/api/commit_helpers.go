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

package api

func NewAddSnapshotUpdate(snapshot Snapshot) CommitUpdate {
	snapshot.Summary = cloneCommitStringMap(snapshot.Summary)
	return CommitUpdate{Type: "add-snapshot", Snapshot: &snapshot}
}

func NewSetSnapshotRefUpdate(refName, refType string, snapshotID int64) CommitUpdate {
	if refType == "" {
		refType = "branch"
	}
	return CommitUpdate{
		Type:       "set-snapshot-ref",
		Ref:        refName,
		RefType:    refType,
		SnapshotID: snapshotID,
	}
}

func NewCommitSnapshot(snapshotID, parentSnapshotID, sequenceNumber int64, schemaID int, timestampMS int64, manifestList string, summary map[string]string) Snapshot {
	schemaIDCopy := schemaID
	snapshot := Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNumber,
		TimestampMS:    timestampMS,
		ManifestList:   manifestList,
		SchemaID:       &schemaIDCopy,
		Summary:        cloneCommitStringMap(summary),
	}
	if parentSnapshotID > 0 {
		parentIDCopy := parentSnapshotID
		snapshot.ParentSnapshotID = &parentIDCopy
	}
	return snapshot
}

func cloneCommitStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

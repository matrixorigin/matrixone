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

// ExpectedAddedSnapshot returns the first valid add-snapshot update in an attempt.
func ExpectedAddedSnapshot(attempt *CommitAttempt) (Snapshot, bool) {
	if attempt == nil {
		return Snapshot{}, false
	}
	for _, update := range attempt.Updates {
		if update.Type == "add-snapshot" && update.Snapshot != nil && update.Snapshot.SnapshotID > 0 {
			return *update.Snapshot, true
		}
	}
	return Snapshot{}, false
}

func NewAddSnapshotUpdate(snapshot Snapshot) CommitUpdate {
	snapshot.Summary = CloneNonEmptyMap(snapshot.Summary)
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

func NewSetSnapshotRefUpdateWithRetention(refName, refType string, snapshotID int64, minSnapshotsToKeep int) CommitUpdate {
	update := NewSetSnapshotRefUpdate(refName, refType, snapshotID)
	if update.RefType == "branch" && minSnapshotsToKeep > 0 {
		update.MinSnapshotsToKeep = minSnapshotsToKeep
	}
	return update
}

func NewSetSnapshotRefUpdatePreservingRetention(refName, refType string, snapshotID int64, retention SnapshotRef) CommitUpdate {
	update := NewSetSnapshotRefUpdate(refName, refType, snapshotID)
	if retention.MaxRefAgeMS > 0 {
		update.MaxRefAgeMS = retention.MaxRefAgeMS
	}
	if update.RefType == "branch" {
		if retention.MinSnapshotsToKeep > 0 {
			update.MinSnapshotsToKeep = retention.MinSnapshotsToKeep
		}
		if retention.MaxSnapshotAgeMS > 0 {
			update.MaxSnapshotAgeMS = retention.MaxSnapshotAgeMS
		}
	}
	return update
}

func NewCommitSnapshot(snapshotID, parentSnapshotID, sequenceNumber int64, schemaID int, timestampMS int64, manifestList string, summary map[string]string) Snapshot {
	schemaIDCopy := schemaID
	snapshot := Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNumber,
		TimestampMS:    timestampMS,
		ManifestList:   manifestList,
		SchemaID:       &schemaIDCopy,
		Summary:        CloneNonEmptyMap(summary),
	}
	if parentSnapshotID > 0 {
		parentIDCopy := parentSnapshotID
		snapshot.ParentSnapshotID = &parentIDCopy
	}
	return snapshot
}

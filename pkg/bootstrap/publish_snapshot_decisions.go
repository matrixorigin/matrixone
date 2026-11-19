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

package bootstrap

import (
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// PublishSnapshotUpgradeDecisions validates the complete input before publishing
// it through the shared file service. All CNs must be stopped and drained first.
// A failed write leaves no new decision file, so the upgrade fails closed.
func PublishSnapshotUpgradeDecisions(ctx context.Context, fs fileservice.FileService, input io.ReadSeeker) error {
	reader, err := newSnapshotDecisionReader(input)
	if err != nil {
		return err
	}
	for {
		record, err := reader.next()
		if err != nil {
			return err
		}
		if record == nil {
			break
		}
	}
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return err
	}
	vector := fileservice.IOVector{FilePath: SnapshotUpgradeDecisionPath, Policy: fileservice.SkipAllCache,
		Entries: []fileservice.IOEntry{{Size: -1, ReaderForWrite: input}}}
	if replace, ok := fs.(fileservice.ReplaceableFileService); ok {
		return replace.Replace(ctx, vector)
	}
	// Object services write a whole new object atomically but may not expose
	// Replace. With all readers stopped, a missing object between delete and
	// write is safe; a failed write must be repaired before restarting CNs.
	if err := fs.Delete(ctx, SnapshotUpgradeDecisionPath); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return err
	}
	return fs.Write(ctx, vector)
}

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

package multi_update

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Only EOF-preflushed legacy objects use this private spool trailer. Keeping
// short names in the existing frame avoids another growing object registry or
// decoding a large raw-delete payload during error cleanup.
const metadataObjectNamesFlag byte = 1 << 7

func pendingObjectCleanupContext(proc *process.Process) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(context.WithoutCancel(proc.Ctx), time.Minute, moerr.CauseCleanUpUselessFiles)
}

func (writer *s3WriterDelegate) metadataObjectNames(offset, size int64) (int64, uint64, error) {
	if size < 8 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	var tail [8]byte
	if _, err := writer.metadataSpool.ReadAt(tail[:], offset+size-8); err != nil {
		return 0, 0, err
	}
	count := binary.LittleEndian.Uint64(tail[:])
	if count > uint64((size-8)/int64(objectio.ObjectNameShortLen)) {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return offset + size - 8 - int64(count)*int64(objectio.ObjectNameShortLen), count, nil
}

func (writer *s3WriterDelegate) cleanupPendingObjects(proc *process.Process, ctx context.Context) error {
	if writer.metadataPass == 1 && writer.metadataSpoolRead == writer.metadataWritten {
		return nil // A drained writer has handed every object to its consumer.
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = pendingObjectCleanupContext(proc)
		defer cancel()
	}
	var first error
	record := func(err error) {
		if first == nil {
			first = err
		}
	}
	remove := func(name string) {
		fs, err := colexec.GetSharedFSFromProc(proc)
		if err == nil {
			err = fs.Delete(ctx, name)
		}
		record(err)
	}
	removeStats := func(vec *vector.Vector) {
		for i := range vec.Length() {
			data := vec.GetBytesAt(i)
			if len(data) != objectio.ObjectStatsLen {
				record(moerr.NewInternalErrorNoCtx("invalid pending object stats"))
				continue
			}
			stats := objectio.ObjectStats(data)
			remove(stats.ObjectName().String())
		}
	}
	// A failed Sync may still own successful earlier uploads. These have not
	// reached block-info/metadata; their existing sinker remains their owner.
	for i, sinker := range writer.insertSinkers {
		if sinker != nil {
			record(sinker.Discard(ctx))
			writer.insertSinkers[i] = nil
		}
	}
	for _, info := range writer.deleteBlockInfo {
		if info != nil {
			removeStats(info.bat.Vecs[0])
		}
	}
	for _, bat := range writer.insertBlockInfo {
		if bat != nil {
			removeStats(bat.Vecs[1])
		}
	}
	// Only unread frames remain producer-owned. Delete and insert passes have
	// separate publication watermarks, so partial Eval preserves handed-off files.
	for pos := int64(0); pos < writer.metadataWritten; {
		var head [9]byte
		if _, err := writer.metadataSpool.ReadAt(head[:], pos); err != nil {
			record(err)
			break
		}
		n := binary.LittleEndian.Uint64(head[:8])
		if n > uint64(writer.metadataWritten-pos-9) {
			record(io.ErrUnexpectedEOF)
			break
		}
		action := actionType(head[8] &^ metadataObjectNamesFlag)
		current := writer.metadataPass == 0 && action == actionDelete || writer.metadataPass == 1 && action == actionInsert
		delivered := writer.metadataPass == 1 && action == actionDelete || current && pos < writer.metadataSpoolRead
		if !delivered && head[8]&metadataObjectNamesFlag != 0 {
			offset, count, err := writer.metadataObjectNames(pos+9, int64(n))
			if err != nil {
				record(err)
				break
			}
			for i := uint64(0); i < count; i++ {
				var name objectio.ObjectNameShort
				if _, err := writer.metadataSpool.ReadAt(name[:], offset+int64(i)*int64(objectio.ObjectNameShortLen)); err != nil {
					record(err)
					break
				}
				remove(objectio.BuildObjectName(name.Segmentid(), name.Num()).String())
			}
		}
		pos += 9 + int64(n)
	}
	return first
}

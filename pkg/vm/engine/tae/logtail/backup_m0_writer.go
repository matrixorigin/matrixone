//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func init() {
	backupDataWriter = newM0BackupDataWriter
}

// The backup reads one appendable block and removes storage-only columns. Its
// output must retain the source object's schema version and the physical
// identities of the remaining columns; the destination's current schema is
// not evidence of the source object's history.
func newM0BackupDataWriter(ctx context.Context, src, dst fileservice.FileService, name objectio.ObjectName, stats *objectio.ObjectStats) (*ioutil.BlockWriter, bool, error) {
	if stats == nil {
		return nil, false, moerr.NewInternalErrorNoCtx("M0: backup source object is missing")
	}
	location := stats.ObjectLocation()
	reader, err := objectio.NewObjectReaderWithStr(location.Name().String(), src)
	if err != nil {
		return nil, false, err
	}
	header, err := reader.ReadHeader(ctx, common.DebugAllocator)
	if err != nil {
		return nil, false, err
	}
	version, err := objectio.ObjectSchemaVersion(header)
	if err != nil {
		return nil, false, err
	}
	if version == 0 {
		writer, err := ioutil.NewBlockWriter(dst, name.String())
		return writer, false, err
	}
	meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, src)
	if err != nil {
		return nil, false, err
	}
	data := meta.MustDataMeta()
	if data.IsEmpty() || data.BlockCount() != 1 {
		return nil, false, moerr.NewInternalErrorNoCtx("M0: backup source is not a single data block")
	}
	block := data.GetBlockMeta(uint32(location.ID()))
	special := objectio.ResolveSpecialColumnLayout(block)
	if special.CommitTS == objectio.InvalidSpecialColumnPosition {
		return nil, false, moerr.NewInternalErrorNoCtx("M0: backup source has no commit timestamp")
	}
	seqnums := make([]uint16, 0, block.GetColumnCount())
	for pos := uint16(0); pos < block.GetColumnCount(); pos++ {
		if pos == special.PhysicalAddr || pos == special.CommitTS || pos == special.Abort {
			continue
		}
		if pos > block.GetMaxSeqnum() || block.MustGetColumn(pos).DataType() == 0 {
			return nil, false, moerr.NewInternalErrorNoCtx("M0: backup source has an invalid physical column")
		}
		seqnums = append(seqnums, pos)
	}
	if len(seqnums) == 0 {
		return nil, false, moerr.NewInternalErrorNoCtx("M0: backup source has no data columns")
	}
	// A fresh object ID gives this rewrite exclusive cleanup ownership even if
	// another backup already owns the legacy deterministic destination name.
	id := objectio.NewObjectid()
	writer, err := ioutil.NewBlockWriterNew(dst, objectio.BuildObjectNameWithObjectID(&id), version, seqnums, false)
	return writer, err == nil, err
}

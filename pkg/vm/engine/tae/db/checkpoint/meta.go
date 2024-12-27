// Copyright 2021 Matrix Origin
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

package checkpoint

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

func ReadEntriesFromMeta(
	ctx context.Context,
	sid string,
	dir string,
	name string,
	verbose int,
	onEachEntry func(entry *CheckpointEntry),
	mp *mpool.MPool,
	fs fileservice.FileService,
) (entries []*CheckpointEntry, err error) {
	reader := NewMetafilesReader(sid, dir, []string{name}, verbose, fs)
	getter := MetadataEntryGetter{reader: reader}
	var batchEntries []*CheckpointEntry
	for {
		if batchEntries, err = getter.NextBatch(
			ctx, onEachEntry, mp,
		); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
			}
			return
		}
		if len(entries) == 0 {
			entries = batchEntries
		} else {
			entries = append(entries, batchEntries...)
		}
	}
}

type MetadataEntryGetter struct {
	reader *MetafilesReader
}

func (getter *MetadataEntryGetter) NextBatch(
	ctx context.Context,
	onEachEntry func(entry *CheckpointEntry),
	mp *mpool.MPool,
) (
	entries []*CheckpointEntry, err error,
) {
	bats, release, err := getter.reader.Next(ctx, mp)
	if err != nil {
		return
	}
	if release != nil {
		defer release()
	}
	rows := 0
	for _, bat := range bats {
		rows += bat.RowCount()
	}
	entries = make([]*CheckpointEntry, 0, rows)

	for _, bat := range bats {
		if err = getter.processOneBatch(
			bat, onEachEntry, &entries,
		); err != nil {
			return
		}
	}
	return
}

func (getter *MetadataEntryGetter) Close() {
	getter.reader.Close()
	getter.reader = nil
}

func (getter *MetadataEntryGetter) processOneBatch(
	bat *batch.Batch,
	onEachEntry func(entry *CheckpointEntry),
	entries *[]*CheckpointEntry,
) (err error) {
	// NOTE:
	// from v2.0 till now, there is just one checkpint version
	// if we have more versions in the future, we need to check the version here
	startCol := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[CheckpointAttr_StartTSIdx])
	endCol := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[CheckpointAttr_EndTSIdx])
	versionCol := vector.MustFixedColWithTypeCheck[uint32](bat.Vecs[CheckpointAttr_VersionIdx])
	lsnCol := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[CheckpointAttr_CheckpointLSNIdx])
	trancateLsnCol := vector.MustFixedColWithTypeCheck[uint64](bat.Vecs[CheckpointAttr_TruncateLSNIdx])
	typeCol := vector.MustFixedColWithTypeCheck[int8](bat.Vecs[CheckpointAttr_TypeIdx])
	for i, length := 0, bat.RowCount(); i < length; i++ {
		start := startCol[i]
		end := endCol[i]
		version := versionCol[i]
		lsn := lsnCol[i]
		trancateLSN := trancateLsnCol[i]
		typ := EntryType(typeCol[i])
		cnLoc := objectio.Location(bat.Vecs[CheckpointAttr_MetaLocationIdx].GetBytesAt(i))
		tnLoc := objectio.Location(bat.Vecs[CheckpointAttr_AllLocationsIdx].GetBytesAt(i))
		entry := &CheckpointEntry{
			start:         start,
			end:           end,
			version:       version,
			ckpLSN:        lsn,
			truncateLSN:   trancateLSN,
			state:         ST_Finished,
			entryType:     typ,
			flushChecked:  true,
			policyChecked: true,
			cnLocation:    cnLoc.Clone(),
			tnLocation:    tnLoc.Clone(),
		}
		if onEachEntry != nil {
			onEachEntry(entry)
		}
		*entries = append(*entries, entry)
	}
	return
}

// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

const (
	scopedChangedTablesTimeout          = 15 * time.Second
	scopedChangedTablesMaxWork          = 1_000_000
	scopedChangedTablesMaxIndexBlocks   = 1_024
	scopedChangedTablesIndexGroupBlocks = 16
	// Sixteen full blocks of the five fixed-width columns are under 6 MiB
	// before vector framing and compression overhead.
	scopedChangedTablesIndexGroupBytes = 8 << 20
)

var errScopedChangedTablesDeadline = moerr.NewInternalErrorNoCtx("scoped changed-table request deadline")

// scopedChangedTablesAdmission admits one scan and at most one waiting request.
// A request holds its admission slot until it finishes or leaves the wait.
type scopedChangedTablesAdmission struct {
	once     sync.Once
	admitted chan struct{}
	active   chan struct{}
}

func (a *scopedChangedTablesAdmission) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.once.Do(func() {
		a.admitted = make(chan struct{}, 2)
		a.active = make(chan struct{}, 1)
	})
	select {
	case a.admitted <- struct{}{}:
	default:
		return moerr.NewChangeWatchRetry(ctx, "scoped changed-table scan is busy")
	}
	select {
	case a.active <- struct{}{}:
		if ctx.Err() == nil {
			return nil
		}
		a.release()
	case <-ctx.Done():
		<-a.admitted
	}
	return ctx.Err()
}

func (a *scopedChangedTablesAdmission) release() {
	<-a.active
	<-a.admitted
}

func (h *Handle) HandleGetScopedChangedTables(
	ctx context.Context,
	_ txn.TxnMeta,
	req *cmd_util.GetScopedChangedTablesReq,
	resp *cmd_util.GetScopedChangedTablesResp,
) (_ func(), err error) {
	*resp = cmd_util.GetScopedChangedTablesResp{}
	if !h.db.IsWriteMode() {
		return nil, moerr.NewNotSupported(ctx, "scoped changed-table query requires TN write mode")
	}
	ctx, cancel := context.WithTimeoutCause(ctx, scopedChangedTablesTimeout, errScopedChangedTablesDeadline)
	defer cancel()
	defer func() {
		err = cmd_util.ClassifyChangeWatchDeadline(ctx, errScopedChangedTablesDeadline, err, "the TN scoped scan deadline expired")
	}()

	scan := req != nil && len(req.Tables) != 0 && req.From.Less(req.To)
	if scan {
		if err := h.scopedChangedTablesAdmission.acquire(ctx); err != nil {
			return nil, err
		}
		defer h.scopedChangedTablesAdmission.release()
	}
	allowed, err := req.AllowedTables(ctx)
	if err != nil {
		return nil, err
	}
	// Confirm the CN's frozen upper bound belongs to this TN's publication
	// history. The FIFO barrier also establishes that the dirty transaction
	// rows through To have reached their terminal states.
	published, err := h.db.LogtailMgr.ReadBarrier(ctx)
	if err != nil {
		return nil, err
	}
	if req.To.Greater(published) {
		return nil, moerr.NewChangeWatchRetry(ctx, "scoped changed-table window exceeds the TN publication frontier")
	}

	var changed []cmd_util.ScopedChangedTable
	if scan {
		started := time.Now()
		changed, err = h.scanScopedChangedTables(ctx, req, allowed)
		v2.ChangeWatchTNScanDurationHistogram.Observe(time.Since(started).Seconds())
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !h.db.IsWriteMode() {
		return nil, moerr.NewChangeWatchRetry(ctx, "TN left write mode during scoped changed-table query")
	}
	*resp = cmd_util.GetScopedChangedTablesResp{
		From:     req.From,
		To:       req.To,
		Complete: true,
		Tables:   changed,
	}
	return nil, nil
}

func (h *Handle) scanScopedChangedTables(
	ctx context.Context,
	req *cmd_util.GetScopedChangedTablesReq,
	allowed map[cmd_util.ScopedChangedTable]struct{},
) ([]cmd_util.ScopedChangedTable, error) {
	// Logtail traverses variable-size transaction and dirty identity lists.
	// Keep its work limit separate from the scope and fixed-width index blocks.
	work := 0
	chargeLogtail := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if work >= scopedChangedTablesMaxWork {
			return moerr.NewChangeWatchCapacity(ctx, "scoped changed-table logtail work budget exceeded")
		}
		work++
		return nil
	}
	dbAccounts := make(map[uint64]uint32)
	for _, table := range req.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if account, ok := dbAccounts[table.DatabaseID]; ok && account != table.AccountID {
			return nil, moerr.NewInvalidInput(ctx, "scoped changed-table database has multiple account identities")
		}
		dbAccounts[table.DatabaseID] = table.AccountID
	}

	var changed []cmd_util.ScopedChangedTable
	add := func(table cmd_util.ScopedChangedTable) {
		if _, ok := allowed[table]; ok {
			changed = append(changed, table)
			delete(allowed, table)
		}
	}

	from, to := types.TimestampToTS(req.From), types.TimestampToTS(req.To)
	previous := from
	floor := h.db.LogtailMgr.GetDirtyHistoryFloor()
	if from.LT(&floor) {
		end := floor
		if end.GT(&to) {
			end = to
		}
		if err := h.scanScopedChangedTableIndex(ctx, nextChangeTimestamp(from), end, add); err != nil {
			return nil, err
		}
		previous = end
	}
	if previous.EQ(&to) {
		return changed, nil
	}

	verifiedDatabases := make(map[uint64]struct{})
	if err := h.db.LogtailMgr.GetReader(nextChangeTimestamp(previous), to).ForEachDirtyTable(chargeLogtail, func(record model.TableRecord) error {
		account, ok := dbAccounts[record.DbID]
		if !ok {
			return nil
		}
		table := cmd_util.ScopedChangedTable{AccountID: account, DatabaseID: record.DbID, TableID: record.ID}
		if _, ok := allowed[table]; !ok {
			return nil
		}
		if _, ok := verifiedDatabases[record.DbID]; !ok {
			if err := chargeLogtail(); err != nil {
				return err
			}
			database, err := h.db.Catalog.GetDatabaseByID(record.DbID)
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				return moerr.NewChangeWatchRetry(ctx, "scoped changed-table database left the TN catalog during query")
			}
			if err != nil || database.GetTenantID() != account {
				return moerr.NewInternalError(ctx, "scoped changed-table database identity could not be verified")
			}
			verifiedDatabases[record.DbID] = struct{}{}
		}
		add(table)
		return nil
	}); err != nil {
		return nil, err
	}
	// GC publishes its frontier before removing blocks. It may race with the
	// snapshot or the scan, so only accept an unchanged coverage boundary.
	if floor := h.db.LogtailMgr.GetDirtyHistoryFloor(); floor.GT(&previous) {
		return nil, moerr.NewChangeWatchRetry(ctx, "scoped changed-table history crossed checkpoint coverage during GC")
	}
	return changed, nil
}

func (h *Handle) scanScopedChangedTableIndex(
	ctx context.Context,
	from, to types.TS,
	add func(cmd_util.ScopedChangedTable),
) error {
	checkpoint := h.db.BGCheckpointRunner.MaxCheckpoint()
	if checkpoint == nil {
		return moerr.NewChangeWatchHistoryUnavailable(ctx, "scoped changed-table history has no covering checkpoint")
	}
	return scanScopedChangedTableIndexLocations(ctx, checkpoint.GetTableIDLocation(), h.db.Runtime.Fs, from, to, add)
}

func scanScopedChangedTableIndexLocations(
	ctx context.Context,
	locations objectio.LocationSlice,
	fs fileservice.FileService,
	from, to types.TS,
	add func(cmd_util.ScopedChangedTable),
) error {
	if len(locations)%objectio.LocationLen != 0 {
		return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint index locations")
	}
	if locations.Len() == 0 {
		return moerr.NewChangeWatchHistoryUnavailable(ctx, "scoped changed-table history has no checkpoint index")
	}
	if locations.Len() > scopedChangedTablesMaxIndexBlocks {
		return moerr.NewChangeWatchCapacity(ctx, "scoped changed-table checkpoint block budget exceeded")
	}
	blocks := 0
	for i := 0; i < locations.Len(); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		count := int(locations.Get(i).ID())
		if count == 0 {
			return moerr.NewInternalError(ctx, "scoped changed-table index location has no blocks")
		}
		blocks += count
		if blocks > scopedChangedTablesMaxIndexBlocks {
			return moerr.NewChangeWatchCapacity(ctx, "scoped changed-table checkpoint block budget exceeded")
		}
	}
	// Coverage rows can occur anywhere and multiple rows can narrow the
	// proven interval. Scan every block even after all allowed tables match.
	var start, end types.TS
	var known bool
	visit := func(vecs []vector.Vector) error {
		accounts := vector.MustFixedColNoTypeCheck[uint32](&vecs[0])
		databases := vector.MustFixedColNoTypeCheck[uint64](&vecs[1])
		tables := vector.MustFixedColNoTypeCheck[uint64](&vecs[2])
		starts := vector.MustFixedColNoTypeCheck[types.TS](&vecs[3])
		ends := vector.MustFixedColNoTypeCheck[types.TS](&vecs[4])
		for i, table := range tables {
			if i%256 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if ends[i].LT(&starts[i]) || starts[i].Physical() < 0 {
				return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint interval")
			}
			if table == logtail.CKPTableIDBatch_SpecialTableID {
				if accounts[i] != 0 || databases[i] != 0 {
					return moerr.NewInternalError(ctx, "invalid scoped changed-table history range identity")
				}
				// Unexpected multiple markers can prove only their intersection.
				if !known || start.LT(&starts[i]) {
					start = starts[i]
				}
				if !known || end.GT(&ends[i]) {
					end = ends[i]
				}
				known = true
				continue
			}
			if databases[i] == 0 {
				return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint identity")
			}
			if !starts[i].GT(&to) && !ends[i].LT(&from) {
				add(cmd_util.ScopedChangedTable{AccountID: accounts[i], DatabaseID: databases[i], TableID: table})
			}
		}
		return nil
	}
	for i := 0; i < locations.Len(); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		location := locations.Get(i)
		extent := location.Extent()
		if !validScopedChangedTableExtent(extent, math.MaxUint32) ||
			extent.Length() > scopedChangedTablesIndexGroupBytes || extent.OriginSize() > scopedChangedTablesIndexGroupBytes {
			return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint metadata extent")
		}
		meta, err := objectio.FastLoadObjectMeta(ctx, location, false, fs)
		if err != nil {
			return err
		}
		blockMetas, ok := meta.CheckedDataBlocks(location.ID())
		if !ok {
			return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint block metadata")
		}
		name := location.Name().String()
		for first := 0; first < len(blockMetas); first += scopedChangedTablesIndexGroupBlocks {
			last := min(first+scopedChangedTablesIndexGroupBlocks, len(blockMetas))
			if err := readScopedChangedTableIndexGroup(ctx, fs, name, blockMetas[first:last], extent.Offset(), visit); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !known || start.GT(&from) || end.LT(&to) {
		return moerr.NewChangeWatchHistoryUnavailable(ctx, "scoped changed-table checkpoint history is incomplete")
	}
	return nil
}

// Callers use this successor only below a strictly greater upper bound.
func nextChangeTimestamp(ts types.TS) types.TS {
	return types.TimestampToTS(ts.ToTimestamp().Next())
}

func readScopedChangedTableIndexGroup(
	ctx context.Context,
	fs fileservice.FileService,
	name string,
	blocks []objectio.BlockObject,
	metaOffset uint32,
	visit func([]vector.Vector) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, block := range blocks {
		if block.GetRows() > objectio.BlockMaxRows || int(block.GetColumnCount()) < len(logtail.TableIDSeqnums) {
			return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint block dimensions")
		}
	}
	// ObjectWriter stores each column across successive blocks before placing
	// the next column. Keep that order so a complete small object needs one read.
	entries := make([]fileservice.IOEntry, 0, len(logtail.TableIDSeqnums)*len(blocks))
	var readBytes, decodedBytes int64
	var previousEnd int64
	for i, seqnum := range logtail.TableIDSeqnums {
		for _, block := range blocks {
			if seqnum > block.GetMaxSeqnum() || seqnum >= block.GetMetaColumnCount() {
				return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint index column metadata")
			}
			column := block.ColumnMeta(seqnum)
			extent := column.Location()
			if column.DataType() != uint8(logtail.TableIDTypes[i].Oid) ||
				!validScopedChangedTableExtent(extent, metaOffset) || int64(extent.Offset()) < previousEnd {
				return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint index column metadata")
			}
			entry := objectio.NewColumnReadEntry(extent)
			readBytes += entry.Size
			decodedBytes += entry.CachedDataSize
			previousEnd = entry.Offset + entry.Size
			entries = append(entries, entry)
		}
	}
	if readBytes > scopedChangedTablesIndexGroupBytes || decodedBytes > scopedChangedTablesIndexGroupBytes {
		return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint index group size")
	}

	// Split at physical gaps, so FileService cannot turn a sparse group into
	// a single large range. Keep decoded buffers alive through each block visit.
	var read int
	defer func() {
		(&fileservice.IOVector{Entries: entries[:read]}).Release()
	}()
	for read < len(entries) {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := read + 1
		for end < len(entries) && entries[end-1].Offset+entries[end-1].Size == entries[end].Offset {
			end++
		}
		ioVec := fileservice.IOVector{
			FilePath: name,
			Policy:   fileservice.SkipFullFilePreloads,
			Entries:  entries[read:end:end],
		}
		if err := fs.Read(ctx, &ioVec); err != nil {
			ioVec.ReleaseReadResultOnError()
			return err
		}
		size := entries[end-1].Offset + entries[end-1].Size - entries[read].Offset
		perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
			counter.FileService.ReadSize.Add(size)
		})
		read = end
	}

	vecs := make([]vector.Vector, len(logtail.TableIDTypes))
	for b, block := range blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i, typ := range logtail.TableIDTypes {
			v := &vecs[i]
			*v = vector.Vector{}
			entry := &entries[i*len(blocks)+b]
			if entry.CachedData == nil || entry.CachedData.Size() != entry.CachedDataSize {
				return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint index column size")
			}
			if err := objectio.MustVectorToCached(v, entry.CachedData); err != nil {
				return err
			}
			if v.GetType().Oid != typ.Oid || v.Length() != int(block.GetRows()) ||
				v.IsConst() || v.GetNulls().Any() || len(v.GetData()) != v.Length()*typ.TypeSize() {
				return moerr.NewInternalError(ctx, "invalid scoped changed-table checkpoint index columns")
			}
		}
		if err := visit(vecs); err != nil {
			return err
		}
	}
	return nil
}

func validScopedChangedTableExtent(extent objectio.Extent, end uint32) bool {
	// Fixed-width checkpoint columns and their metadata use unchunked extents.
	return extent.Offset() >= objectio.HeaderSize && extent.Length() > 0 && extent.OriginSize() > 0 &&
		uint64(extent.Offset())+uint64(extent.Length()) <= uint64(end) &&
		(extent.Alg() == compress.None || extent.Alg() == compress.Lz4)
}

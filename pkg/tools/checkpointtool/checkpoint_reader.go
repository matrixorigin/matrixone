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

package checkpointtool

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// CheckpointReader reads checkpoint data from a directory
type CheckpointReader struct {
	ctx     context.Context
	fs      fileservice.FileService
	dir     string
	entries []*checkpoint.CheckpointEntry
	mp      *mpool.MPool
	closeFS bool
}

// Option configures CheckpointReader
type Option func(*CheckpointReader)

// WithMPool sets memory pool
func WithMPool(mp *mpool.MPool) Option {
	return func(r *CheckpointReader) {
		r.mp = mp
	}
}

// WithCloseFS closes the file service when the reader is closed.
func WithCloseFS() Option {
	return func(r *CheckpointReader) {
		r.closeFS = true
	}
}

// Open opens checkpoint data from a directory
func Open(ctx context.Context, dir string, opts ...Option) (*CheckpointReader, error) {
	fs, err := fileservice.NewLocalFS(ctx, "local", dir, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "create file service: %v", err)
	}

	return OpenWithFS(ctx, fs, dir, append(opts, WithCloseFS())...)
}

// OpenWithFS opens checkpoint data from an existing file service.
func OpenWithFS(ctx context.Context, fs fileservice.FileService, dir string, opts ...Option) (*CheckpointReader, error) {
	r := &CheckpointReader{
		ctx: ctx,
		fs:  fs,
		dir: dir,
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.mp == nil {
		r.mp = mpool.MustNewZero()
	}

	if err := r.loadEntries(); err != nil {
		return nil, err
	}
	return r, nil
}

// FS returns the file service used by this reader.
func (r *CheckpointReader) FS() fileservice.FileService {
	return r.fs
}

// Fork returns a lightweight reader that shares the file service and already
// loaded checkpoint entries, but uses an independent memory pool and does not
// own the file service.
func (r *CheckpointReader) Fork(ctx context.Context) *CheckpointReader {
	if ctx == nil {
		ctx = r.ctx
	}
	return &CheckpointReader{
		ctx:     ctx,
		fs:      r.fs,
		dir:     r.dir,
		entries: r.entries,
		mp:      mpool.MustNewZero(),
	}
}

func (r *CheckpointReader) loadEntries() error {
	names, err := ckputil.ListCKPMetaNames(r.ctx, r.fs)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}

	for _, name := range names {
		entries, err := checkpoint.ReadEntriesFromMeta(
			r.ctx, "", ioutil.GetCheckpointDir(), name, 0, nil, r.mp, r.fs,
		)
		if err != nil {
			return err
		}
		r.entries = append(r.entries, entries...)
	}

	// Deduplicate by (start, end, type, location)
	seen := make(map[string]bool)
	unique := make([]*checkpoint.CheckpointEntry, 0, len(r.entries))
	for _, e := range r.entries {
		key := e.GetStart().ToString() + "|" + e.GetEnd().ToString() + "|" + e.GetType().String() + "|" + e.GetLocation().String()
		if !seen[key] {
			seen[key] = true
			unique = append(unique, e)
		}
	}
	r.entries = unique

	// Sort by end timestamp (newest first)
	sort.Slice(r.entries, func(i, j int) bool {
		ei, ej := r.entries[i].GetEnd(), r.entries[j].GetEnd()
		return ei.GT(&ej)
	})
	return nil
}

// Info returns checkpoint summary
func (r *CheckpointReader) Info() *CheckpointInfo {
	info := &CheckpointInfo{Dir: r.dir, TotalEntries: len(r.entries)}
	for _, e := range r.entries {
		switch e.GetType() {
		case checkpoint.ET_Global:
			info.GlobalCount++
		case checkpoint.ET_Incremental:
			info.IncrCount++
		case checkpoint.ET_Compacted:
			info.CompactCount++
		}
		start := e.GetStart()
		end := e.GetEnd()
		if !start.IsEmpty() {
			if info.EarliestTS.IsEmpty() || start.LT(&info.EarliestTS) {
				info.EarliestTS = start
			}
		}
		if !end.IsEmpty() {
			if info.LatestTS.IsEmpty() || end.GT(&info.LatestTS) {
				info.LatestTS = end
			}
			if info.EarliestTS.IsEmpty() || end.LT(&info.EarliestTS) {
				info.EarliestTS = end
			}
		}
	}
	return info
}

// Dir returns the checkpoint directory
func (r *CheckpointReader) Dir() string {
	return r.dir
}

// Entries returns all checkpoint entries
func (r *CheckpointReader) Entries() []*checkpoint.CheckpointEntry {
	return r.entries
}

// GetEntry returns entry at index
func (r *CheckpointReader) GetEntry(index int) (*checkpoint.CheckpointEntry, error) {
	if index < 0 || index >= len(r.entries) {
		return nil, moerr.NewInternalErrorf(r.ctx, "index out of range: %d", index)
	}
	return r.entries[index], nil
}

// EntryInfo converts CheckpointEntry to EntryInfo
func (r *CheckpointReader) EntryInfo(index int, e *checkpoint.CheckpointEntry) *EntryInfo {
	tableIDLocs := e.GetTableIDLocation()
	var tableIDStrs []string
	for i := 0; i < tableIDLocs.Len(); i++ {
		tableIDStrs = append(tableIDStrs, tableIDLocs.Get(i).String())
	}

	return &EntryInfo{
		Index:            index,
		Start:            e.GetStart(),
		End:              e.GetEnd(),
		Type:             e.GetType(),
		State:            e.GetState(),
		Version:          e.GetVersion(),
		Location:         e.GetLocation().String(),
		TNLocation:       e.GetTNLocation().String(),
		TableIDLocations: tableIDStrs,
		CKPLSN:           e.LSN(),
		TruncLSN:         e.GetTruncateLsn(),
	}
}

// GetTableRanges reads table ranges from an entry
func (r *CheckpointReader) GetTableRanges(entry *checkpoint.CheckpointEntry) ([]ckputil.TableRange, error) {
	loc := entry.GetLocation()
	if loc.IsEmpty() {
		return nil, nil
	}

	_, err := r.fs.StatFile(r.ctx, loc.Name().String())
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) || os.IsNotExist(err) {
			return nil, moerr.NewFileNotFoundErrorf(r.ctx, "checkpoint data file not found (may have been GC'd): %s", loc.Name().String())
		}
		return nil, err
	}

	reader := logtail.NewCKPReader(entry.GetVersion(), loc, r.mp, r.fs)
	if err := reader.ReadMeta(r.ctx); err != nil {
		return nil, err
	}
	return reader.GetTableRanges(r.ctx)
}

// GetAccounts extracts unique accounts from table ranges
func (r *CheckpointReader) GetAccounts(entry *checkpoint.CheckpointEntry) ([]*AccountInfo, error) {
	ranges, err := r.GetTableRanges(entry)
	if err != nil {
		return nil, err
	}

	accountMap := make(map[uint32]*AccountInfo)
	for _, rng := range ranges {
		// Extract account ID from table ID (high 32 bits)
		accountID := uint32(rng.TableID >> 32)
		acc, ok := accountMap[accountID]
		if !ok {
			acc = &AccountInfo{AccountID: accountID}
			accountMap[accountID] = acc
		}
		acc.TableCount++
		if rng.ObjectType == ckputil.ObjectType_Data {
			acc.DataRanges++
		} else {
			acc.TombRanges++
		}
	}

	accounts := make([]*AccountInfo, 0, len(accountMap))
	for _, acc := range accountMap {
		accounts = append(accounts, acc)
	}
	sort.Slice(accounts, func(i, j int) bool {
		return accounts[i].AccountID < accounts[j].AccountID
	})
	return accounts, nil
}

// GetTables extracts tables from an entry
func (r *CheckpointReader) GetTables(entry *checkpoint.CheckpointEntry) ([]*TableInfo, error) {
	ranges, err := r.GetTableRanges(entry)
	if err != nil {
		return nil, err
	}
	return r.rangesToTables(ranges), nil
}

// GetTablesByAccount filters tables by account
func (r *CheckpointReader) GetTablesByAccount(entry *checkpoint.CheckpointEntry, accountID uint32) ([]*TableInfo, error) {
	ranges, err := r.GetTableRanges(entry)
	if err != nil {
		return nil, err
	}

	var filtered []ckputil.TableRange
	for _, rng := range ranges {
		if uint32(rng.TableID>>32) == accountID {
			filtered = append(filtered, rng)
		}
	}
	return r.rangesToTables(filtered), nil
}

func (r *CheckpointReader) rangesToTables(ranges []ckputil.TableRange) []*TableInfo {
	tableMap := make(map[uint64]*TableInfo)
	for _, rng := range ranges {
		tbl, ok := tableMap[rng.TableID]
		if !ok {
			tbl = &TableInfo{
				TableID:   rng.TableID,
				AccountID: uint32(rng.TableID >> 32),
			}
			tableMap[rng.TableID] = tbl
		}
		if rng.ObjectType == ckputil.ObjectType_Data {
			tbl.DataRanges = append(tbl.DataRanges, rng)
		} else {
			tbl.TombRanges = append(tbl.TombRanges, rng)
		}
	}

	tables := make([]*TableInfo, 0, len(tableMap))
	for _, tbl := range tableMap {
		tables = append(tables, tbl)
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableID < tables[j].TableID
	})
	return tables
}

// ReadTableData reads actual data from a table range
func (r *CheckpointReader) ReadTableData(ctx context.Context, rng ckputil.TableRange) (*batch.Batch, func(), error) {
	reader, err := ioutil.NewFileReader(r.fs, rng.ObjectStats.ObjectName().String())
	if err != nil {
		return nil, nil, err
	}

	bats, release, err := reader.LoadAllColumns(ctx, nil, r.mp)
	if err != nil {
		return nil, nil, err
	}

	if len(bats) == 0 {
		release()
		return nil, nil, nil
	}
	return bats[0], release, nil
}

// ComposeAt creates a logical view at timestamp
func (r *CheckpointReader) ComposeAt(ts types.TS) (*ComposedView, error) {
	view := &ComposedView{Timestamp: ts, Tables: make(map[uint64]*TableInfo)}

	// Find latest GCKP <= ts that still has data on disk.
	// GC may have cleaned up older GCKP data files; skip those and use the next one.
	var baseEntry *checkpoint.CheckpointEntry
	var baseEntryIdx int
	for i := len(r.entries) - 1; i >= 0; i-- {
		e := r.entries[i]
		end := e.GetEnd()
		if e.IsGlobal() && end.LE(&ts) {
			tables, err := r.GetTables(e)
			if err != nil {
				if isDataFileNotFound(err) {
					continue // GC'd entry; try the next older GCKP
				}
				return nil, err
			}
			baseEntry = e
			baseEntryIdx = i
			for _, t := range tables {
				view.Tables[t.TableID] = t
			}
			view.BaseEntry = r.EntryInfo(baseEntryIdx, baseEntry)
			break
		}
	}

	// Add ICKPs after GCKP.end and <= ts
	baseEnd := types.TS{}
	if baseEntry != nil {
		baseEnd = baseEntry.GetEnd()
	}
	for i, e := range r.entries {
		start := e.GetStart()
		end := e.GetEnd()
		if e.IsIncremental() && start.GT(&baseEnd) && end.LE(&ts) {
			tables, err := r.GetTables(e)
			if err != nil {
				if isDataFileNotFound(err) {
					continue // GC'd entry; skip
				}
				return nil, err
			}
			view.Incrementals = append(view.Incrementals, r.EntryInfo(i, e))
			for _, t := range tables {
				if existing, ok := view.Tables[t.TableID]; ok {
					existing.DataRanges = append(existing.DataRanges, t.DataRanges...)
					existing.TombRanges = append(existing.TombRanges, t.TombRanges...)
				} else {
					view.Tables[t.TableID] = t
				}
			}
		}
	}
	return view, nil
}

// ValidateSnapshot checks that ts can compose a catalog-backed checkpoint view.
func (r *CheckpointReader) ValidateSnapshot(ctx context.Context, ts types.TS) error {
	view, err := r.ComposeAt(ts)
	if err != nil {
		return err
	}
	if len(view.Tables) == 0 {
		return moerr.NewInternalErrorf(ctx, "checkpoint snapshot %s is not usable: no tables are available", ts.ToString())
	}
	if _, ok := view.Tables[moTablesID]; !ok {
		return moerr.NewInternalErrorf(ctx, "checkpoint snapshot %s is not usable: mo_tables catalog is not available; wait for a global checkpoint or choose a later timestamp", ts.ToString())
	}
	if _, ok := view.Tables[moColumnsID]; !ok {
		return moerr.NewInternalErrorf(ctx, "checkpoint snapshot %s is not usable: mo_columns catalog is not available; wait for a global checkpoint or choose a later timestamp", ts.ToString())
	}
	return nil
}

// isDataFileNotFound checks if err indicates a checkpoint data file was GC'd/missing.
func isDataFileNotFound(err error) bool {
	if err == nil {
		return false
	}
	return moerr.IsMoErrCode(err, moerr.ErrFileNotFound) || os.IsNotExist(err)
}

// Close releases resources
func (r *CheckpointReader) Close() error {
	r.entries = nil
	if r.closeFS && r.fs != nil {
		r.fs.Close(r.ctx)
	}
	return nil
}

// GetObjectEntries reads detailed object entries with timestamps for a table
func (r *CheckpointReader) GetObjectEntries(entry *checkpoint.CheckpointEntry, tableID uint64) ([]*ObjectEntryInfo, []*ObjectEntryInfo, error) {
	loc := entry.GetLocation()
	if loc.IsEmpty() {
		return nil, nil, nil
	}

	_, err := r.fs.StatFile(r.ctx, loc.Name().String())
	if err != nil {
		if isDataFileNotFound(err) {
			return nil, nil, moerr.NewFileNotFoundErrorf(r.ctx, "checkpoint data file not found (may have been GC'd): %s", loc.Name().String())
		}
		return nil, nil, err
	}

	var dataEntries, tombEntries []*ObjectEntryInfo

	reader := logtail.NewCKPReaderWithTableID_V2(entry.GetVersion(), loc, tableID, r.mp, r.fs)
	if err := reader.ReadMeta(r.ctx); err != nil {
		return nil, nil, err
	}

	err = reader.ConsumeCheckpointWithTableID(r.ctx, func(
		_ context.Context,
		_ fileservice.FileService,
		obj objectio.ObjectEntry,
		isTombstone bool,
	) error {
		info := &ObjectEntryInfo{
			ObjectStats: obj.ObjectStats,
			CreateTime:  obj.CreateTime,
			DeleteTime:  obj.DeleteTime,
		}
		if isTombstone {
			tombEntries = append(tombEntries, info)
		} else {
			dataEntries = append(dataEntries, info)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return dataEntries, tombEntries, nil
}

// GetObjectEntriesForTables reads detailed object entries for multiple tables
// from one checkpoint entry in a single pass.
func (r *CheckpointReader) GetObjectEntriesForTables(
	entry *checkpoint.CheckpointEntry,
	tableIDs map[uint64]struct{},
) (map[uint64][]*ObjectEntryInfo, map[uint64][]*ObjectEntryInfo, error) {
	loc := entry.GetLocation()
	if loc.IsEmpty() {
		return nil, nil, nil
	}

	_, err := r.fs.StatFile(r.ctx, loc.Name().String())
	if err != nil {
		if isDataFileNotFound(err) {
			return nil, nil, moerr.NewFileNotFoundErrorf(r.ctx, "checkpoint data file not found (may have been GC'd): %s", loc.Name().String())
		}
		return nil, nil, err
	}

	dataByTable := make(map[uint64][]*ObjectEntryInfo)
	tombByTable := make(map[uint64][]*ObjectEntryInfo)
	reader := logtail.NewCKPReader(entry.GetVersion(), loc, r.mp, r.fs)
	if err := reader.ReadMeta(r.ctx); err != nil {
		return nil, nil, err
	}
	err = reader.ForEachRow(r.ctx, func(
		_ uint32,
		_, tid uint64,
		objectType int8,
		objectStats objectio.ObjectStats,
		create, delete types.TS,
		_ types.Rowid,
	) error {
		if _, ok := tableIDs[tid]; !ok {
			return nil
		}
		info := &ObjectEntryInfo{
			ObjectStats: objectStats,
			CreateTime:  create,
			DeleteTime:  delete,
		}
		switch objectType {
		case ckputil.ObjectType_Data:
			dataByTable[tid] = append(dataByTable[tid], info)
		case ckputil.ObjectType_Tombstone:
			tombByTable[tid] = append(tombByTable[tid], info)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return dataByTable, tombByTable, nil
}

// ReadRangeData reads actual data from a range and returns column names and row data as strings
func (r *CheckpointReader) ReadRangeData(entry *checkpoint.CheckpointEntry, rng ckputil.TableRange) ([]string, [][]string, error) {
	objName := rng.ObjectStats.ObjectName().String()
	reader, err := ioutil.NewFileReader(r.fs, objName)
	if err != nil {
		return nil, nil, err
	}

	bats, release, err := reader.LoadAllColumns(r.ctx, nil, r.mp)
	if err != nil {
		return nil, nil, err
	}
	defer release()

	if len(bats) == 0 {
		return nil, nil, nil
	}

	columns := checkpointRangeColumns(len(bats[0].Vecs))

	// Extract rows within the range
	startRow := uint32(rng.Start.GetRowOffset())
	endRow := uint32(rng.End.GetRowOffset())

	var rows [][]string
	for _, b := range bats {
		for rowIdx := 0; rowIdx < b.RowCount(); rowIdx++ {
			if uint32(rowIdx) >= startRow && uint32(rowIdx) <= endRow {
				row := make([]string, len(b.Vecs))
				for colIdx, vec := range b.Vecs {
					row[colIdx] = vecValueToString(vec, rowIdx)
				}
				rows = append(rows, row)
			}
		}
	}

	return columns, rows, nil
}

func checkpointRangeColumns(width int) []string {
	if width <= 0 {
		return nil
	}
	columns := append([]string(nil), ckputil.TableObjectsAttrs...)
	if len(columns) > width {
		return columns[:width]
	}
	for len(columns) < width {
		columns = append(columns, fmt.Sprintf("col_%d", len(columns)))
	}
	return columns
}

// vecValueToString converts a vector value at index to string
func vecValueToString(vec *vector.Vector, idx int) string {
	if vec.IsNull(uint64(idx)) {
		return "NULL"
	}

	if vec.GetType().Oid == types.T_time {
		rowIdx := idx
		if vec.IsConst() {
			rowIdx = 0
		}
		values := vector.MustFixedColWithTypeCheck[types.Time](vec)
		if rowIdx >= 0 && rowIdx < len(values) {
			return values[rowIdx].String2(vec.GetType().Scale)
		}
	}
	value := vec.RowToString(idx)
	if value == "null" {
		return "NULL"
	}
	return value
}

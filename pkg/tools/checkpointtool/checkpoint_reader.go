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
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

var (
	newCheckpointOfflineFS = objectio.NewOfflineFS
	loadCheckpointEntries  = (*CheckpointReader).loadEntries
)

// CheckpointReader reads checkpoint data from a directory
type CheckpointReader struct {
	ctx     context.Context
	fs      fileservice.FileService
	dir     string
	kind    string
	entries []*checkpoint.CheckpointEntry
	mp      *mpool.MPool
	closeFS bool

	getTableRangesForTest   func(*CheckpointReader, *checkpoint.CheckpointEntry) ([]ckputil.TableRange, error)
	getTablesForTest        func(*CheckpointReader, *checkpoint.CheckpointEntry) ([]*TableInfo, error)
	getObjectEntriesForTest func(*CheckpointReader, *checkpoint.CheckpointEntry, uint64) ([]*ObjectEntryInfo, []*ObjectEntryInfo, error)
	getObjectsForTablesTest func(*CheckpointReader, *checkpoint.CheckpointEntry, map[uint64]struct{}) (map[uint64][]*ObjectEntryInfo, map[uint64][]*ObjectEntryInfo, error)
	getLogicalViewForTest   func(*CheckpointReader, uint64) (*LogicalTableView, error)
	filterTombstonesForTest func(*objectio.ObjectId, []objectio.ObjectStats) ([]objectio.ObjectStats, error)
	buildDeleteMaskForTest  func(*types.TS, objectio.ObjectStats, uint16, []objectio.ObjectStats) (objectio.Bitmap, error)
}

func (r *CheckpointReader) SetGetTableRangesForTest(fn func(*CheckpointReader, *checkpoint.CheckpointEntry) ([]ckputil.TableRange, error)) {
	r.getTableRangesForTest = fn
}

func (r *CheckpointReader) SetGetTablesForTest(fn func(*CheckpointReader, *checkpoint.CheckpointEntry) ([]*TableInfo, error)) {
	r.getTablesForTest = fn
}

func (r *CheckpointReader) SetGetObjectEntriesForTest(fn func(*CheckpointReader, *checkpoint.CheckpointEntry, uint64) ([]*ObjectEntryInfo, []*ObjectEntryInfo, error)) {
	r.getObjectEntriesForTest = fn
}

func (r *CheckpointReader) SetGetObjectsForTablesTest(fn func(*CheckpointReader, *checkpoint.CheckpointEntry, map[uint64]struct{}) (map[uint64][]*ObjectEntryInfo, map[uint64][]*ObjectEntryInfo, error)) {
	r.getObjectsForTablesTest = fn
}

func (r *CheckpointReader) SetGetLogicalViewForTest(fn func(*CheckpointReader, uint64) (*LogicalTableView, error)) {
	r.getLogicalViewForTest = fn
}

// Option configures CheckpointReader
type Option func(*CheckpointReader)

// WithMPool sets memory pool
func WithMPool(mp *mpool.MPool) Option {
	return func(r *CheckpointReader) {
		r.mp = mp
	}
}

// WithKind selects the on-disk format of the data dir: "local" (default,
// DISK/CRC), "local2" (DISK-V2/raw) or "s3" (S3FS-on-disk/raw).
func WithKind(kind string) Option {
	return func(r *CheckpointReader) {
		r.kind = kind
	}
}

// Kind returns the offline fs kind the reader was opened with.
func (r *CheckpointReader) Kind() string {
	return r.kind
}

// WithCloseFS closes the file service when the reader is closed.
func WithCloseFS() Option {
	return func(r *CheckpointReader) {
		r.closeFS = true
	}
}

// Open opens checkpoint data from a directory
func Open(ctx context.Context, dir string, opts ...Option) (*CheckpointReader, error) {
	r := &CheckpointReader{
		ctx:  ctx,
		dir:  dir,
		kind: objectio.OfflineKindLocal,
	}
	for _, opt := range opts {
		opt(r)
	}

	fs, err := newCheckpointOfflineFS(ctx, dir, r.kind)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "create file service: %v", err)
	}
	initialized := false
	defer func() {
		if !initialized {
			fs.Close(ctx)
		}
	}()

	r.fs = fs
	r.closeFS = true

	if r.mp == nil {
		r.mp = mpool.MustNewZero()
	}

	if err := loadCheckpointEntries(r); err != nil {
		return nil, err
	}
	initialized = true
	return r, nil
}

// OpenWithFS opens checkpoint data from an existing file service.
func OpenWithFS(ctx context.Context, fs fileservice.FileService, dir string, opts ...Option) (*CheckpointReader, error) {
	r := &CheckpointReader{
		ctx:  ctx,
		dir:  dir,
		kind: objectio.OfflineKindLocal,
	}
	for _, opt := range opts {
		opt(r)
	}

	r.fs = fs
	initialized := false
	defer func() {
		if !initialized && r.closeFS && fs != nil {
			fs.Close(ctx)
		}
	}()

	if r.mp == nil {
		r.mp = mpool.MustNewZero()
	}

	if err := loadCheckpointEntries(r); err != nil {
		return nil, err
	}
	initialized = true
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
		ctx:                     ctx,
		fs:                      r.fs,
		dir:                     r.dir,
		kind:                    r.kind,
		entries:                 r.entries,
		mp:                      mpool.MustNewZero(),
		getTableRangesForTest:   r.getTableRangesForTest,
		getTablesForTest:        r.getTablesForTest,
		getObjectEntriesForTest: r.getObjectEntriesForTest,
		getObjectsForTablesTest: r.getObjectsForTablesTest,
		getLogicalViewForTest:   r.getLogicalViewForTest,
		filterTombstonesForTest: r.filterTombstonesForTest,
		buildDeleteMaskForTest:  r.buildDeleteMaskForTest,
	}
}

func (r *CheckpointReader) loadEntries() error {
	ckpDir := ioutil.GetCheckpointDir()
	logutil.Infof("[loadEntries] reading checkpoint dir: %s (display dir: %s)", ckpDir, r.dir)

	// Diagnostic: list all raw files in the checkpoint directory
	rawEntries, listErr := fileservice.SortedList(r.fs.List(r.ctx, ckpDir))
	if listErr != nil {
		logutil.Infof("[loadEntries] failed to list dir %s: %v", ckpDir, listErr)
	} else {
		logutil.Infof("[loadEntries] raw listing: total=%d dirs=%d files=%d",
			len(rawEntries), countDirs(rawEntries), countFiles(rawEntries))
		for _, e := range rawEntries {
			if !e.IsDir {
				valid := ""
				if f := ioutil.DecodeTSRangeFile(e.Name); f.IsValid() {
					valid = " [valid tsrange]"
					if f.IsMetadataFile() {
						valid += " [meta]"
					} else {
						valid += " [non-meta ext=" + f.GetExt() + "]"
					}
				}
				logutil.Infof("[loadEntries]   file: %s%s", e.Name, valid)
			}
		}
	}

	names, err := ckputil.ListCKPMetaNames(r.ctx, r.fs)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		logutil.Infof("[loadEntries] no checkpoint meta files found in dir=%s", r.dir)
		return nil
	}
	logutil.Infof("[loadEntries] found %d meta file(s) to read", len(names))

	totalRead := 0
	for _, name := range names {
		entries, err := checkpoint.ReadEntriesFromMeta(
			r.ctx, "", ckpDir, name, 0, nil, r.mp, r.fs,
		)
		if err != nil {
			logutil.Infof("[loadEntries] failed to read meta file %s: %v", name, err)
			return err
		}
		logutil.Infof("[loadEntries] meta file=%s entries=%d", name, len(entries))
		for _, e := range entries {
			logutil.Infof("[loadEntries]   entry: start=%s end=%s type=%s state=%d",
				e.GetStart().ToString(), e.GetEnd().ToString(), e.GetType().String(), e.GetState())
		}
		r.entries = append(r.entries, entries...)
		totalRead += len(entries)
	}
	logutil.Infof("[loadEntries] total raw entries read: %d", totalRead)

	// Deduplicate by (start, end, type, location)
	removed := 0
	seen := make(map[string]bool)
	unique := make([]*checkpoint.CheckpointEntry, 0, len(r.entries))
	for _, e := range r.entries {
		key := e.GetStart().ToString() + "|" + e.GetEnd().ToString() + "|" + e.GetType().String() + "|" + e.GetLocation().String()
		if !seen[key] {
			seen[key] = true
			unique = append(unique, e)
		} else {
			removed++
		}
	}
	r.entries = unique
	logutil.Infof("[loadEntries] dedup: removed=%d remaining=%d", removed, len(unique))

	// Sort by end timestamp (newest first)
	slices.SortFunc(r.entries, func(a, b *checkpoint.CheckpointEntry) int {
		ai, bi := a.GetEnd(), b.GetEnd()
		return bi.Compare(&ai)
	})

	// Log final usable entries
	for _, e := range r.entries {
		logutil.Infof("[loadEntries] usable ckp: start=%s end=%s type=%s",
			e.GetStart().ToString(), e.GetEnd().ToString(), e.GetType().String())
	}

	return nil
}

func countDirs(entries []fileservice.DirEntry) int {
	n := 0
	for _, e := range entries {
		if e.IsDir {
			n++
		}
	}
	return n
}

func countFiles(entries []fileservice.DirEntry) int {
	return len(entries) - countDirs(entries)
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
	if r.getTableRangesForTest != nil {
		return r.getTableRangesForTest(r, entry)
	}
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
	slices.SortFunc(accounts, func(a, b *AccountInfo) int {
		return cmp.Compare(a.AccountID, b.AccountID)
	})
	return accounts, nil
}

// GetTables extracts tables from an entry
func (r *CheckpointReader) GetTables(entry *checkpoint.CheckpointEntry) ([]*TableInfo, error) {
	if r.getTablesForTest != nil {
		return r.getTablesForTest(r, entry)
	}
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
	slices.SortFunc(tables, func(a, b *TableInfo) int {
		return cmp.Compare(a.TableID, b.TableID)
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
	for i, e := range r.entries {
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

	// Add ICKPs from GCKP.end through the first entry that covers ts. The
	// checkpoint store uses the selected GCKP end as the start of the first
	// following ICKP, then previous.end.Next() for each subsequent ICKP.
	baseEnd := types.TS{}
	if baseEntry != nil {
		baseEnd = baseEntry.GetEnd()
	}
	type incrementalEntry struct {
		index int
		entry *checkpoint.CheckpointEntry
	}
	incrementals := make([]incrementalEntry, 0, len(r.entries))
	for i, e := range r.entries {
		if e.IsIncremental() {
			incrementals = append(incrementals, incrementalEntry{index: i, entry: e})
		}
	}
	slices.SortFunc(incrementals, func(a, b incrementalEntry) int {
		aStart := a.entry.GetStart()
		bStart := b.entry.GetStart()
		if c := aStart.Compare(&bStart); c != 0 {
			return c
		}
		aEnd := a.entry.GetEnd()
		bEnd := b.entry.GetEnd()
		return aEnd.Compare(&bEnd)
	})
	expectedStart := baseEnd
	covered := baseEntry != nil && ts.LE(&baseEnd)
	for _, incr := range incrementals {
		e := incr.entry
		start := e.GetStart()
		end := e.GetEnd()
		if end.LE(&baseEnd) || end.LT(&expectedStart) {
			continue
		}
		hasChain := baseEntry != nil || len(view.Incrementals) > 0
		if !shouldIncludeIncrementalCheckpoint(start, end, expectedStart, ts, hasChain) {
			if hasChain && !covered && start.GT(&expectedStart) && expectedStart.LE(&ts) {
				return nil, moerr.NewInternalErrorf(
					r.ctx,
					"checkpoint chain cannot cover snapshot %s: missing incremental checkpoint at start=%s before next start=%s end=%s",
					ts.ToString(),
					expectedStart.ToString(),
					start.ToString(),
					end.ToString(),
				)
			}
			continue
		}
		tables, err := r.GetTables(e)
		if err != nil {
			if isDataFileNotFound(err) {
				return nil, moerr.NewFileNotFoundErrorf(
					r.ctx,
					"required incremental checkpoint data file not found (may have been GC'd): start=%s end=%s",
					start.ToString(),
					end.ToString(),
				)
			}
			return nil, err
		}
		view.Incrementals = append(view.Incrementals, r.EntryInfo(incr.index, e))
		for _, t := range tables {
			if existing, ok := view.Tables[t.TableID]; ok {
				existing.DataRanges = append(existing.DataRanges, t.DataRanges...)
				existing.TombRanges = append(existing.TombRanges, t.TombRanges...)
			} else {
				view.Tables[t.TableID] = t
			}
		}
		if ts.LE(&end) {
			covered = true
			break
		}
		expectedStart = end.Next()
	}
	if !covered && expectedStart.LE(&ts) && (baseEntry != nil || len(view.Incrementals) > 0) {
		return nil, moerr.NewInternalErrorf(
			r.ctx,
			"checkpoint chain cannot cover snapshot %s: missing incremental checkpoint at start=%s",
			ts.ToString(),
			expectedStart.ToString(),
		)
	}
	return view, nil
}

func shouldIncludeIncrementalCheckpoint(start, end, baseEnd, ts types.TS, hasBase bool) bool {
	if !hasBase {
		return start.GE(&baseEnd) && start.LE(&ts)
	}
	if start.GT(&baseEnd) {
		return false
	}
	if start.LT(&baseEnd) {
		return false
	}
	return end.GE(&baseEnd) && start.LE(&ts)
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
	return moerr.IsMoErrCode(err, moerr.ErrFileNotFound) ||
		os.IsNotExist(err) ||
		strings.Contains(err.Error(), " is not found")
}

// Close releases resources
func (r *CheckpointReader) Close() error {
	r.entries = nil
	if r.closeFS && r.fs != nil {
		fs := r.fs
		r.fs = nil
		r.closeFS = false
		fs.Close(r.ctx)
	}
	return nil
}

// GetObjectEntries reads detailed object entries with timestamps for a table
func (r *CheckpointReader) GetObjectEntries(entry *checkpoint.CheckpointEntry, tableID uint64) ([]*ObjectEntryInfo, []*ObjectEntryInfo, error) {
	if r.getObjectEntriesForTest != nil {
		return r.getObjectEntriesForTest(r, entry, tableID)
	}
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
	if r.getObjectsForTablesTest != nil {
		return r.getObjectsForTablesTest(r, entry, tableIDs)
	}
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
	switch vec.GetType().Oid {
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text, types.T_datalink, types.T_geometry:
		return string(vec.GetBytesAt(idx))
	}
	value := vec.RowToString(idx)
	if value == "null" {
		return "NULL"
	}
	return value
}

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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

// CheckpointReader reads checkpoint data from a directory
type CheckpointReader struct {
	ctx     context.Context
	fs      fileservice.FileService
	dir     string
	entries []*checkpoint.CheckpointEntry
	mp      *mpool.MPool
}

// Option configures CheckpointReader
type Option func(*CheckpointReader)

// WithMPool sets memory pool
func WithMPool(mp *mpool.MPool) Option {
	return func(r *CheckpointReader) {
		r.mp = mp
	}
}

// Open opens checkpoint data from a directory
func Open(ctx context.Context, dir string, opts ...Option) (*CheckpointReader, error) {
	fs, err := fileservice.NewLocalFS(ctx, "local", dir, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "create file service: %v", err)
	}

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
		if info.EarliestTS.IsEmpty() || start.LT(&info.EarliestTS) {
			info.EarliestTS = start
		}
		if end.GT(&info.LatestTS) {
			info.LatestTS = end
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

	// Check if file exists before trying to read
	fileName := loc.Name().String()
	_, err := r.fs.StatFile(r.ctx, fileName)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, moerr.NewInternalErrorf(r.ctx, "checkpoint data file not found (may have been GC'd): %s", fileName)
		}
		return nil, err
	}

	reader, err := ioutil.NewFileReader(r.fs, fileName)
	if err != nil {
		return nil, err
	}

	bats, release, err := reader.LoadAllColumns(r.ctx, nil, r.mp)
	if err != nil {
		return nil, err
	}
	defer release()

	var ranges []ckputil.TableRange
	for _, bat := range bats {
		ranges = append(ranges, ckputil.ExportToTableRanges(bat)...)
	}
	return ranges, nil
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

	// Find latest GCKP <= ts
	var baseEntry *checkpoint.CheckpointEntry
	for i := len(r.entries) - 1; i >= 0; i-- {
		e := r.entries[i]
		end := e.GetEnd()
		if e.IsGlobal() && end.LE(&ts) {
			baseEntry = e
			break
		}
	}

	if baseEntry != nil {
		view.BaseEntry = r.EntryInfo(0, baseEntry)
		tables, err := r.GetTables(baseEntry)
		if err != nil {
			return nil, err
		}
		for _, t := range tables {
			view.Tables[t.TableID] = t
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
			view.Incrementals = append(view.Incrementals, r.EntryInfo(i, e))
			tables, err := r.GetTables(e)
			if err != nil {
				return nil, err
			}
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

// Close releases resources
func (r *CheckpointReader) Close() error {
	r.entries = nil
	return nil
}

// GetObjectEntries reads detailed object entries with timestamps for a table
func (r *CheckpointReader) GetObjectEntries(entry *checkpoint.CheckpointEntry, tableID uint64) ([]*ObjectEntryInfo, []*ObjectEntryInfo, error) {
	// First get all table ranges
	allRanges, err := r.GetTableRanges(entry)
	if err != nil {
		return nil, nil, err
	}

	// Now read the checkpoint file again to get timestamps
	loc := entry.GetLocation()
	if loc.IsEmpty() {
		return nil, nil, nil
	}

	fileName := loc.Name().String()
	reader, err := ioutil.NewFileReader(r.fs, fileName)
	if err != nil {
		return nil, nil, err
	}

	bats, release, err := reader.LoadAllColumns(r.ctx, nil, r.mp)
	if err != nil {
		return nil, nil, err
	}
	defer release()

	// Build a map of ranges with their timestamps
	var dataEntries, tombEntries []*ObjectEntryInfo
	rangeIdx := 0

	for _, bat := range bats {
		if bat.RowCount() == 0 {
			continue
		}
		
		// Check if timestamp columns exist
		var createTSVec, deleteTSVec []types.TS
		if len(bat.Vecs) > ckputil.TableObjectsAttr_CreateTS_Idx {
			createTSVec = vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx])
		}
		if len(bat.Vecs) > ckputil.TableObjectsAttr_DeleteTS_Idx {
			deleteTSVec = vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx])
		}

		for i := 0; i < bat.RowCount(); i++ {
			if rangeIdx >= len(allRanges) {
				break
			}
			
			rng := allRanges[rangeIdx]
			rangeIdx++
			
			if rng.TableID != tableID {
				continue
			}

			entry := &ObjectEntryInfo{
				Range: rng,
			}
			
			// Set timestamps if available
			if createTSVec != nil && i < len(createTSVec) {
				entry.CreateTime = createTSVec[i]
			}
			if deleteTSVec != nil && i < len(deleteTSVec) {
				entry.DeleteTime = deleteTSVec[i]
			}

			if rng.ObjectType == ckputil.ObjectType_Data {
				dataEntries = append(dataEntries, entry)
			} else if rng.ObjectType == ckputil.ObjectType_Tombstone {
				tombEntries = append(tombEntries, entry)
			}
		}
	}

	return dataEntries, tombEntries, nil
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

	// Use checkpoint schema for column names
	columns := ckputil.TableObjectsAttrs
	if len(columns) == 0 {
		// Fallback: generate column names from vector count
		bat := bats[0]
		columns = make([]string, len(bat.Vecs))
		for i := range columns {
			columns[i] = fmt.Sprintf("col_%d", i)
		}
	}

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

	// Trim columns to match actual vector count
	if len(bats) > 0 && len(columns) > len(bats[0].Vecs) {
		columns = columns[:len(bats[0].Vecs)]
	}

	return columns, rows, nil
}

// vecValueToString converts a vector value at index to string
func vecValueToString(vec *vector.Vector, idx int) string {
	if vec.IsNull(uint64(idx)) {
		return "NULL"
	}

	switch vec.GetType().Oid {
	case types.T_bool:
		v := vector.MustFixedColWithTypeCheck[bool](vec)
		return fmt.Sprintf("%v", v[idx])
	case types.T_int8:
		v := vector.MustFixedColWithTypeCheck[int8](vec)
		return fmt.Sprintf("%d", v[idx])
	case types.T_int16:
		v := vector.MustFixedColWithTypeCheck[int16](vec)
		return fmt.Sprintf("%d", v[idx])
	case types.T_int32:
		v := vector.MustFixedColWithTypeCheck[int32](vec)
		return fmt.Sprintf("%d", v[idx])
	case types.T_int64:
		v := vector.MustFixedColWithTypeCheck[int64](vec)
		return fmt.Sprintf("%d", v[idx])
	case types.T_uint8:
		v := vector.MustFixedColWithTypeCheck[uint8](vec)
		return fmt.Sprintf("%d", v[idx])
	case types.T_uint16:
		v := vector.MustFixedColWithTypeCheck[uint16](vec)
		return fmt.Sprintf("%d", v[idx])
	case types.T_uint32:
		v := vector.MustFixedColWithTypeCheck[uint32](vec)
		return fmt.Sprintf("%d", v[idx])
	case types.T_uint64:
		v := vector.MustFixedColWithTypeCheck[uint64](vec)
		return fmt.Sprintf("%d", v[idx])
	case types.T_float32:
		v := vector.MustFixedColWithTypeCheck[float32](vec)
		return fmt.Sprintf("%.4f", v[idx])
	case types.T_float64:
		v := vector.MustFixedColWithTypeCheck[float64](vec)
		return fmt.Sprintf("%.4f", v[idx])
	case types.T_char, types.T_varchar, types.T_text:
		return vec.GetStringAt(idx)
	case types.T_TS:
		v := vector.MustFixedColWithTypeCheck[types.TS](vec)
		return v[idx].ToString()
	case types.T_Rowid:
		v := vector.MustFixedColWithTypeCheck[types.Rowid](vec)
		return fmt.Sprintf("%d-%d", v[idx].GetBlockOffset(), v[idx].GetRowOffset())
	case types.T_Blockid:
		v := vector.MustFixedColWithTypeCheck[types.Blockid](vec)
		return v[idx].String()
	case types.T_Objectid:
		v := vector.MustFixedColWithTypeCheck[types.Objectid](vec)
		return v[idx].ShortStringEx()
	default:
		// For binary/blob types, show hex or truncated
		if vec.GetType().IsVarlen() {
			bs := vec.GetBytesAt(idx)
			if len(bs) > 16 {
				return fmt.Sprintf("%x...", bs[:16])
			}
			return fmt.Sprintf("%x", bs)
		}
		return fmt.Sprintf("<%s>", vec.GetType().String())
	}
}


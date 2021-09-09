package metadata

import (
	"encoding/json"
	dump "encoding/json"
	"errors"
	"fmt"
	"io"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/google/btree"
	// dump "github.com/vmihailenco/msgpack/v5"
	// log "github.com/sirupsen/logrus"
)

func NewMetaInfo(mu *sync.RWMutex, conf *Configuration) *MetaInfo {
	info := &MetaInfo{
		RWMutex:   mu,
		Tables:    make(map[uint64]*Table),
		Conf:      conf,
		TableIds:  make(map[uint64]bool),
		NameMap:   make(map[string]uint64),
		NameTree:  btree.New(2),
		Tombstone: make(map[uint64]bool),
	}
	return info
}

func MockInfo(mu *sync.RWMutex, blkRows, blks uint64) *MetaInfo {
	info := NewMetaInfo(mu, &Configuration{
		BlockMaxRows:     blkRows,
		SegmentMaxBlocks: blks,
	})
	return info
}

func (info *MetaInfo) SoftDeleteTable(name string, logIndex uint64) (id uint64, err error) {
	id, ok := info.NameMap[name]
	if !ok {
		return id, errors.New(fmt.Sprintf("Table %s not existed", name))
	}
	ts := NowMicro()
	info.Lock()
	defer info.Unlock()
	delete(info.NameMap, name)
	info.Tombstone[id] = true
	table := info.Tables[id]
	info.NameTree.Delete(table)
	table.Delete(ts)
	table.LogHistry.DeletedIndex = logIndex
	atomic.AddUint64(&info.CheckPoint, uint64(1))
	table.UpdateVersion()
	return id, nil
}

func (info *MetaInfo) ReferenceTableByName(name string) (tbl *Table, err error) {
	info.RLock()
	defer info.RUnlock()
	id, ok := info.NameMap[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified table %s not found in info", name))
	}
	return info.Tables[id], nil
}

func (info *MetaInfo) ReferenceTable(table_id uint64) (tbl *Table, err error) {
	info.RLock()
	defer info.RUnlock()
	tbl, ok := info.Tables[table_id]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified table %d not found in info", table_id))
	}
	return tbl, nil
}

func (info *MetaInfo) ReferenceBlock(table_id, segment_id, block_id uint64) (blk *Block, err error) {
	info.RLock()
	tbl, ok := info.Tables[table_id]
	if !ok {
		info.RUnlock()
		return nil, errors.New(fmt.Sprintf("specified table %d not found in info", table_id))
	}
	info.RUnlock()
	blk, err = tbl.ReferenceBlock(segment_id, block_id)

	return blk, err
}

func (info *MetaInfo) TableSegmentIDs(tableID uint64, args ...int64) (ids map[uint64]uint64, err error) {
	info.RLock()
	tbl, ok := info.Tables[tableID]
	info.RUnlock()
	if !ok {
		return ids, errors.New(fmt.Sprintf("Specified table %d not found", tableID))
	}
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids = tbl.SegmentIDs(ts)
	return ids, err
}

func (info *MetaInfo) UpdateCheckpointTime(ts int64) {
	curr := atomic.LoadInt64(&info.CkpTime)
	for curr < ts {
		if atomic.CompareAndSwapInt64(&info.CkpTime, curr, ts) {
			return
		}
		curr = atomic.LoadInt64(&info.CkpTime)
	}
}

func (info *MetaInfo) GetCheckpointTime() int64 {
	return atomic.LoadInt64(&info.CkpTime)
}

func (info *MetaInfo) GetTablesByNamePrefix(prefix string) (tbls []*Table) {
	ts := NowMicro()
	upperBound := []byte(prefix)
	upperBound = append(upperBound, byte(255))
	info.RLock()
	defer info.RUnlock()
	info.NameTree.AscendRange(
		&Table{Schema: &Schema{Name: prefix}},
		&Table{Schema: &Schema{Name: string(upperBound)}},
		func(item btree.Item) bool {
			t := item.(*Table)
			if !t.Select(ts) {
				return false
			}
			tbls = append(tbls, t)
			return true
		})
	return tbls
}

func (info *MetaInfo) TableNames(args ...int64) []string {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	names := make([]string, 0)
	info.RLock()
	defer info.RUnlock()
	for _, t := range info.Tables {
		if !t.Select(ts) {
			continue
		}
		names = append(names, t.Schema.Name)
	}
	return names
}

func (info *MetaInfo) TableIDs(args ...int64) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids := make(map[uint64]uint64)
	info.RLock()
	defer info.RUnlock()
	for _, t := range info.Tables {
		if !t.Select(ts) {
			continue
		}
		ids[t.GetID()] = t.GetID()
	}
	return ids
}

func (info *MetaInfo) CreateTable(logIdx uint64, schema *Schema) (tbl *Table, err error) {
	if !schema.Valid() {
		return nil, errors.New("invalid schema")
	}
	tbl = NewTable(logIdx, info, schema, info.Sequence.GetTableID())
	return tbl, err
}

func (info *MetaInfo) UpdateCheckpoint(id uint64) error {
	if !atomic.CompareAndSwapUint64(&info.CheckPoint, id-1, id) {
		return errors.New(fmt.Sprintf("Cannot update checkpoint from %d to %d", info.CheckPoint, id))
	}
	return nil
}

func (info *MetaInfo) String() string {
	s := fmt.Sprintf("Info(ck=%d)", info.CheckPoint)
	s += "["
	for i, t := range info.Tables {
		if i != 0 {
			s += "\n"
		}
		s += t.String()
	}
	if len(info.Tables) > 0 {
		s += "\n"
	}
	s += "]"
	return s
}

func (info *MetaInfo) RegisterTable(tbl *Table) error {
	buf := make([]byte, 4096)
	runtime.Stack(buf, true)
	logutil.Infof("call local RegisterTable, %v, %v", tbl.Schema.Name, string(buf))
	info.Lock()
	defer info.Unlock()

	_, ok := info.Tables[tbl.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate table %d found in info", tbl.ID))
	}
	_, ok = info.NameMap[tbl.Schema.Name]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate table %s found in info", tbl.Schema.Name))
	}
	err := tbl.Attach()
	if err != nil {
		return err
	}

	info.Tables[tbl.ID] = tbl
	info.NameMap[tbl.Schema.Name] = tbl.ID
	info.NameTree.ReplaceOrInsert(tbl)
	info.TableIds[tbl.ID] = true
	atomic.AddUint64(&info.CheckPoint, uint64(1))
	return nil
}

func (info *MetaInfo) CreateTableFromTableInfo(tinfo *aoe.TableInfo, ctx dbi.TableOpCtx) (*Table, error) {
	schema := &Schema{
		Name:      tinfo.Name,
		ColDefs:   make([]*ColDef, 0),
		Indices:   make([]*IndexInfo, 0),
		NameIdMap: make(map[string]int),
	}
	for idx, colInfo := range tinfo.Columns {
		newInfo := &ColDef{
			Name: colInfo.Name,
			Idx:  idx,
			Type: colInfo.Type,
		}
		schema.NameIdMap[newInfo.Name] = len(schema.ColDefs)
		schema.ColDefs = append(schema.ColDefs, newInfo)
	}
	for _, indexInfo := range tinfo.Indices {
		newInfo := &IndexInfo{
			ID:      info.Sequence.GetIndexID(),
			Type:    IndexType(indexInfo.Type),
			Columns: make([]uint16, 0),
		}
		for _, col := range indexInfo.Columns {
			newInfo.Columns = append(newInfo.Columns, uint16(col))
		}
		schema.Indices = append(schema.Indices, newInfo)
	}
	tbl, err := info.CreateTable(ctx.OpIndex, schema)
	if err != nil {
		return nil, err
	}
	err = info.RegisterTable(tbl)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func (info *MetaInfo) GetLastFileName() string {
	return fmt.Sprintf("%d", info.CheckPoint-1)
}

func (info *MetaInfo) GetFileName() string {
	return fmt.Sprintf("%d", info.CheckPoint)
}

func (info *MetaInfo) GetResourceType() ResourceType {
	return ResInfo
}

func (info *MetaInfo) Unmarshal(buf []byte) error {
	type Alias MetaInfo
	v := &struct {
		*Alias
		Tables map[uint64]GenericTableWrapper
	}{
		Alias: (*Alias)(info),
	}
	err := json.Unmarshal(buf, v)
	if err != nil {
		return err
	}
	info.Tables = make(map[uint64]*Table)
	for _, wrapped := range v.Tables {
		info.Tables[wrapped.ID] = &Table{ID: wrapped.ID, TimeStamp: wrapped.TimeStamp}
	}
	return nil
}

func (info *MetaInfo) MarshalJSON() ([]byte, error) {
	tables := make(map[uint64]GenericTableWrapper)
	for _, tbl := range info.Tables {
		tables[tbl.ID] = GenericTableWrapper{
			ID:        tbl.ID,
			TimeStamp: tbl.TimeStamp,
			LogHistry: tbl.LogHistry,
		}
	}
	type Alias MetaInfo
	return json.Marshal(&struct {
		Tables map[uint64]GenericTableWrapper
		*Alias
	}{
		Tables: tables,
		Alias:  (*Alias)(info),
	})
}

func (info *MetaInfo) ReadFrom(r io.Reader) (int64, error) {
	decoder := dump.NewDecoder(r)
	err := decoder.Decode(info)
	return decoder.InputOffset(), err
}

func (info *MetaInfo) GetTableId() uint64 {
	panic("logic error")
}

func (info *MetaInfo) Copy(ctx CopyCtx) *MetaInfo {
	if ctx.Ts == 0 {
		ctx.Ts = NowMicro()
	}
	new_info := NewMetaInfo(info.RWMutex, info.Conf)
	new_info.CheckPoint = info.CheckPoint
	new_info.CkpTime = ctx.Ts
	for k, v := range info.Tables {
		var tbl *Table
		if !v.Select(ctx.Ts) {
			tbl = v.LiteCopy()
		} else {
			tbl = v.Copy(ctx)
		}

		new_info.Tables[k] = tbl
	}

	return new_info
}

func (info *MetaInfo) Serialize(w io.Writer) error {
	return dump.NewEncoder(w).Encode(info)
}

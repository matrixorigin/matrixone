package md

import (
	dump "encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	// "os"
	// "path"
	// dump "github.com/vmihailenco/msgpack/v5"
	// log "github.com/sirupsen/logrus"
)

func NewMetaInfo(conf *Configuration) *MetaInfo {
	info := &MetaInfo{
		Tables:    make(map[uint64]*Table),
		Conf:      conf,
		TableIds:  make(map[uint64]bool),
		NameMap:   make(map[string]uint64),
		Tombstone: make(map[uint64]bool),
	}
	return info
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

func (info *MetaInfo) CreateTable(schema *Schema) (tbl *Table, err error) {
	if !schema.Valid() {
		return nil, errors.New("invalid schema")
	}
	tbl = NewTable(info, schema, info.Sequence.GetTableID())
	return tbl, err
}

func (info *MetaInfo) UpdateCheckpoint(id uint64) error {
	if !atomic.CompareAndSwapUint64(&info.CheckPoint, id-1, id) {
		return errors.New(fmt.Sprintf("Cannot update checkpoint to %d", id))
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
	info.TableIds[tbl.ID] = true
	return nil
}

func (info *MetaInfo) Copy(ts ...int64) *MetaInfo {
	var t int64
	if len(ts) == 0 {
		t = NowMicro()
	} else {
		t = ts[0]
	}
	new_info := NewMetaInfo(info.Conf)
	new_info.CheckPoint = info.CheckPoint
	for k, v := range info.Tables {
		if !v.Select(t) {
			continue
		}
		tbl := v.Copy(ts...)
		new_info.Tables[k] = tbl
	}

	return new_info
}

func (info *MetaInfo) Serialize(w io.Writer) error {
	return dump.NewEncoder(w).Encode(info)
}

func Deserialize(r io.Reader) (info *MetaInfo, err error) {
	info = NewMetaInfo(nil)
	err = dump.NewDecoder(r).Decode(info)
	if err != nil {
		return nil, err
	}
	// TODO: make it faster
	info.Sequence.NextBlockID = 0
	info.Sequence.NextSegmentID = 0
	info.Sequence.NextTableID = 0
	ts := NowMicro()
	for k, tbl := range info.Tables {
		max_tbl_segid, max_tbl_blkid := tbl.GetMaxSegIDAndBlkID()
		if k > info.Sequence.NextTableID {
			info.Sequence.NextTableID = k
		}
		if max_tbl_segid > info.Sequence.NextSegmentID {
			info.Sequence.NextSegmentID = max_tbl_segid
		}
		if max_tbl_blkid > info.Sequence.NextBlockID {
			info.Sequence.NextBlockID = max_tbl_blkid
		}
		tbl.Info = info
		if tbl.IsDeleted(ts) {
			info.Tombstone[tbl.ID] = true
		} else {
			info.TableIds[tbl.ID] = true
			info.NameMap[tbl.Schema.Name] = tbl.ID
		}
	}

	return info, err
}

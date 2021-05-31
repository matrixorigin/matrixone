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

const (
	META_FILE_NAME = "META"
)

// var (
// 	Meta = *NewMetaInfo(nil)
// )

// func init() {
// 	Meta.Conf = &Configuration{
// 		Dir:              "/tmp",
// 		BlockMaxRows:     BLOCK_ROW_COUNT,
// 		SegmentMaxBlocks: SEGMENT_BLOCK_COUNT,
// 	}
// }

func NewMetaInfo(conf *Configuration) *MetaInfo {
	info := &MetaInfo{
		Tables: make(map[uint64]*Table),
		Conf:   conf,
	}
	return info
}

// func InitMeta(conf *Configuration) error {
// 	r, err := os.OpenFile(path.Join(conf.Dir, META_FILE_NAME), os.O_RDONLY, 0666)
// 	defer r.Close()
// 	if err != nil {
// 		return err
// 	}
// 	info, err := Deserialize(r)
// 	info.Conf = *conf
// 	Meta = *info
// 	return err
// }

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

func (info *MetaInfo) CreateTable() (tbl *Table, err error) {
	tbl = NewTable(info, info.Sequence.GetTableID())
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
	err := tbl.Attach()
	if err != nil {
		return err
	}

	info.Tables[tbl.ID] = tbl
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
	info = &MetaInfo{
		Tables: make(map[uint64]*Table),
	}
	err = dump.NewDecoder(r).Decode(info)
	if err != nil {
		return nil, err
	}
	// TODO: make it faster
	info.Sequence.NextBlockID = 0
	info.Sequence.NextSegmentID = 0
	info.Sequence.NextTableID = 0
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
	}

	return info, err
}

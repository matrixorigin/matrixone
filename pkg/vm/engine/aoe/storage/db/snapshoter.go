package db

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type SSWriter interface {
	io.Closer
	metadata.SSWriter
	GetIndex() uint64
}

type SSLoader = metadata.SSLoader

type Snapshoter interface {
	SSWriter
	SSLoader
}

var (
	CopyTableFn func(t iface.ITableData, destDir string) error
	CopyFileFn  func(src, dest string) error
)

func init() {
	CopyTableFn = func(t iface.ITableData, destDir string) error {
		return t.LinkTo(destDir)
	}
	// CopyTableFn = func(t iface.ITableData, destDir string) error {
	// 	return t.CopyTo(destDir)
	// }
	CopyFileFn = os.Link
	// CopyFileFn = func(src, dest string) error {
	// 	_, err := dataio.CopyFile(src, dest)
	// 	return err
	// }
}

type ssWriter struct {
	mwriter metadata.SSWriter
	meta    *metadata.Database
	data    map[uint64]iface.ITableData
	tables  *table.Tables
	dir     string
	index   uint64
}

type ssLoader struct {
	mloader  metadata.SSLoader
	src      string
	database *metadata.Database
	tables   *table.Tables
	index    uint64
}

func NewDBSSWriter(database *metadata.Database, dir string, tables *table.Tables) *ssWriter {
	w := &ssWriter{
		data:   make(map[uint64]iface.ITableData),
		tables: tables,
		dir:    dir,
		meta:   database,
	}
	return w
}

func NewDBSSLoader(database *metadata.Database, tables *table.Tables, index uint64, src string) *ssLoader {
	ss := &ssLoader{
		database: database,
		src:      src,
		tables:   tables,
		index:    index,
	}
	return ss
}

func (ss *ssWriter) Close() error {
	for _, t := range ss.data {
		t.Unref()
	}
	return nil
}

func (ss *ssWriter) onTableData(data iface.ITableData) error {
	meta := data.GetMeta()
	if meta.Database == ss.meta {
		data.Ref()
		ss.data[data.GetID()] = data
	}
	return nil
}

func (ss *ssWriter) validatePrepare() error {
	// TODO
	return nil
}

func (ss *ssWriter) GetIndex() uint64 {
	return ss.index
}

func (ss *ssWriter) PrepareWrite() error {
	ss.tables.RLock()
	ss.index = ss.meta.GetCheckpointId()
	err := ss.tables.ForTablesLocked(ss.onTableData)
	ss.tables.RUnlock()
	if err != nil {
		return err
	}
	ss.mwriter = metadata.NewDBSSWriter(ss.meta, ss.dir, ss.index)
	if err = ss.mwriter.PrepareWrite(); err != nil {
		return err
	}
	return ss.validatePrepare()
}

func (ss *ssWriter) CommitWrite() error {
	err := ss.mwriter.CommitWrite()
	if err != nil {
		return err
	}
	for _, t := range ss.data {
		if err = CopyTableFn(t, ss.dir); err != nil {
			break
		}
	}
	return err
}

func (ss *ssLoader) validateMetaLoader() error {
	if ss.index != ss.mloader.GetIndex() {
		return errors.New(fmt.Sprintf("index mismatch: %d, %d", ss.index, ss.mloader.GetIndex()))
	}
	if ss.database.GetShardId() != ss.mloader.GetShardId() {
		return errors.New(fmt.Sprintf("shard id mismatch: %d, %d", ss.index, ss.mloader.GetIndex()))
	}
	return nil
}

func (ss *ssLoader) execCopyTBlk(dir, file string) error {
	name, _ := common.ParseTBlockfileName(file)
	count, tag, id, err := dataio.ParseTBlockfileName(name)
	if err != nil {
		return err
	}
	nid, err := ss.mloader.Addresses().GetBlkAddr(&id)
	if err != nil {
		return err
	}
	src := filepath.Join(dir, file)
	dest := dataio.MakeTblockFileName(ss.database.Catalog.Cfg.Dir, tag, count, *nid, false)
	err = CopyFileFn(src, dest)
	return err
}

func (ss *ssLoader) execCopyBlk(dir, file string) error {
	name, _ := common.ParseBlockfileName(file)
	id, err := common.ParseBlkNameToID(name)
	if err != nil {
		return err
	}
	nid, err := ss.mloader.Addresses().GetBlkAddr(&id)
	if err != nil {
		return err
	}
	src := filepath.Join(dir, file)
	dest := common.MakeBlockFileName(ss.database.Catalog.Cfg.Dir, nid.ToBlockFileName(), nid.TableID, false)
	err = CopyFileFn(src, dest)
	return err
}

func (ss *ssLoader) execCopySeg(dir, file string) error {
	name, _ := common.ParseSegmentfileName(file)
	id, err := common.ParseSegmentFileName(name)
	if err != nil {
		return err
	}
	nid, err := ss.mloader.Addresses().GetSegAddr(&id)
	if err != nil {
		return err
	}
	src := filepath.Join(dir, file)
	dest := common.MakeSegmentFileName(ss.database.Catalog.Cfg.Dir, nid.ToSegmentFileName(), nid.TableID, false)
	logutil.Infof("Copy \"%s\" to \"%s\"", src, dest)
	err = CopyFileFn(src, dest)
	return err
}

func (ss *ssLoader) prepareData(tblks, blks, segs []string) error {
	var err error
	for _, tblk := range tblks {
		if ss.execCopyTBlk(ss.src, tblk); err != nil {
			return err
		}
	}
	for _, blk := range blks {
		if ss.execCopyBlk(ss.src, blk); err != nil {
			return err
		}
	}
	for _, seg := range segs {
		if ss.execCopySeg(ss.src, seg); err != nil {
			return err
		}
	}

	return err
}

func (ss *ssLoader) PrepareLoad() error {
	files, err := ioutil.ReadDir(ss.src)
	if err != nil {
		return err
	}
	var (
		metas, blks, tblks, segs []string
	)
	for _, file := range files {
		name := file.Name()
		if common.IsSegmentFile(name) {
			segs = append(segs, name)
		} else if common.IsBlockFile(name) {
			blks = append(blks, name)
		} else if common.IsTBlockFile(name) {
			tblks = append(tblks, name)
		} else if strings.HasSuffix(name, ".meta") {
			metas = append(metas, name)
		}
	}

	if len(metas) != 1 {
		return errors.New("invalid meta data to apply")
	}

	ss.mloader = metadata.NewDBSSLoader(ss.database.Catalog, filepath.Join(ss.src, metas[0]))
	err = ss.mloader.PrepareLoad()
	if err != nil {
		return err
	}
	if err = ss.validateMetaLoader(); err != nil {
		return err
	}

	if err = ss.prepareData(tblks, blks, segs); err != nil {
		return err
	}

	return err
}

func (ss *ssLoader) CommitLoad() error {
	return ss.mloader.CommitLoad()
}

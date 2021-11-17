package db

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
)

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

func CopyTBlockFileToDestDir(file, srcDir, destDir string, idMapFn func(*common.ID) (*common.ID, error)) error {
	name, _ := common.ParseTBlockfileName(file)
	count, tag, id, err := dataio.ParseTBlockfileName(name)
	if err != nil {
		return err
	}
	nid, err := idMapFn(&id)
	if err != nil {
		return err
	}
	src := filepath.Join(srcDir, file)
	dest := dataio.MakeTblockFileName(destDir, tag, count, *nid, false)
	logutil.Infof("Copy \"%s\" to \"%s\"", src, dest)
	err = CopyFileFn(src, dest)
	return err
}

func CopyBlockFileToDestDir(file, srcDir, destDir string, idMapFn func(*common.ID) (*common.ID, error)) error {
	name, _ := common.ParseBlockfileName(file)
	id, err := common.ParseBlkNameToID(name)
	if err != nil {
		return err
	}
	nid, err := idMapFn(&id)
	if err != nil {
		return err
	}
	src := filepath.Join(srcDir, file)
	dest := common.MakeBlockFileName(destDir, nid.ToBlockFileName(), nid.TableID, false)
	logutil.Infof("Copy \"%s\" to \"%s\"", src, dest)
	err = CopyFileFn(src, dest)
	return err
}

func CopySegmentFileToDestDir(file, srcDir, destDir string, idMapFn func(*common.ID) (*common.ID, error)) error {
	name, _ := common.ParseSegmentFileName(file)
	id, err := common.ParseSegmentNameToID(name)
	if err != nil {
		return err
	}
	nid, err := idMapFn(&id)
	if err != nil {
		return err
	}
	src := filepath.Join(srcDir, file)
	dest := common.MakeSegmentFileName(destDir, nid.ToSegmentFileName(), nid.TableID, false)
	logutil.Infof("Copy \"%s\" to \"%s\"", src, dest)
	err = CopyFileFn(src, dest)
	return err
}

func ScanMigrationDir(path string) (metas []string, tblks []string, blks []string, segs []string, err error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
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
	return
}

package db

import (
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
)

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
	err = CopyFileFn(src, dest)
	return err
}

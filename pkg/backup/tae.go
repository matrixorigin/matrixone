package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"path"
	"strings"
)

func getFileNames(ctx context.Context, retBytes [][][]byte) ([]string, error) {
	var err error
	cr := pb.CtlResult{}
	err = json.Unmarshal(retBytes[0][0], &cr)
	if err != nil {
		return nil, err
	}
	rsSlice, ok := cr.Data.([]interface{})
	if !ok {
		return nil, moerr.NewInternalError(ctx, "invalid ctl result")
	}
	var fileName []string
	for _, rs := range rsSlice {
		str, ok := rs.(string)
		if !ok {
			return nil, moerr.NewInternalError(ctx, "invalid ctl string")
		}

		for _, x := range strings.Split(str, ";") {
			if len(x) == 0 {
				continue
			}
			fileName = append(fileName, x)
		}
	}
	return fileName, err
}

func BackupData(ctx context.Context, srcFs, dstFs fileservice.FileService, dir string) error {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(ctx, "no implement sqlExecutor")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}
	sql := fmt.Sprintf("select mo_ctl('dn','Backup','');")
	res, err := exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}

	var retByts [][][]byte
	res.ReadRows(func(cols []*vector.Vector) bool {
		retByts = append(retByts, executor.GetBytesRows(cols[0]))
		return true
	})

	fileName, err := getFileNames(ctx, retByts)
	if err != nil {
		return err
	}

	files := make(map[string]*fileservice.DirEntry, 0)
	for _, name := range fileName {
		if len(fileName) == 0 {
			continue
		}
		key, err := blockio.EncodeLocationFromString(name)
		if err != nil {
			return err
		}
		bat, err := logtail.LoadCheckpointEntriesFromKey(ctx, srcFs, key)
		if err != nil {
			return err
		}
		for i := 0; i < bat.Vecs[3].Length(); i++ {
			metaLoc := objectio.Location(bat.Vecs[3].GetBytesAt(i))
			if metaLoc == nil {
				continue
			}
			if files[metaLoc.Name().String()] == nil {
				dentry, err := srcFs.StatFile(ctx, metaLoc.Name().String())
				if err != nil {
					return err
				}
				files[metaLoc.Name().String()] = dentry
			}
		}
	}

	for _, dentry := range files {
		if dentry.IsDir {
			panic("not support dir")
		}
		err = CopyFile(ctx, srcFs, dstFs, dentry, "")
		if err != nil {
			return err
		}
	}
	err = CopyDir(ctx, srcFs, dstFs, "ckp")
	if err != nil {
		return err
	}
	err = CopyDir(ctx, srcFs, dstFs, "gc")
	if err != nil {
		return err
	}
	return nil
}

func collectCkpData(
	ckp *checkpoint.CheckpointEntry,
	catalog *catalog.Catalog,
) (data *logtail.CheckpointData, err error) {
	factory := logtail.IncrementalCheckpointDataFactory(
		ckp.GetStart(),
		ckp.GetEnd(),
	)
	data, err = factory(catalog)
	return
}

func CopyDir(ctx context.Context, srcFs, dstFs fileservice.FileService, dir string) error {
	files, err := srcFs.List(ctx, dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir {
			panic("not support dir")
		}
		err = CopyFile(ctx, srcFs, dstFs, &file, dir)
		if err != nil {
			return err
		}
	}
	return nil
}

func CopyFile(ctx context.Context, srcFs, dstFs fileservice.FileService, dentry *fileservice.DirEntry, dstDir string) error {
	name := dentry.Name
	if dstDir != "" {
		name = path.Join(dstDir, name)
	}
	ioVec := &fileservice.IOVector{
		FilePath:    name,
		Entries:     make([]fileservice.IOEntry, 1),
		CachePolicy: fileservice.SkipAll,
	}
	logutil.Infof("copy file %v", dentry)
	ioVec.Entries[0] = fileservice.IOEntry{
		Offset: 0,
		Size:   dentry.Size,
	}
	err := srcFs.Read(ctx, ioVec)
	if err != nil {
		return err
	}
	dstIoVec := fileservice.IOVector{
		FilePath:    name,
		Entries:     make([]fileservice.IOEntry, 1),
		CachePolicy: fileservice.SkipAll,
	}
	dstIoVec.Entries[0] = fileservice.IOEntry{
		Offset: 0,
		Data:   ioVec.Entries[0].Data,
		Size:   dentry.Size,
	}
	err = dstFs.Write(ctx, dstIoVec)
	return err
}

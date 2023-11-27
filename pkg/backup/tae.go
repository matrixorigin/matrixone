// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"os"
	"path"
	"strconv"
	"strings"
)

func getFileNames(ctx context.Context, retBytes [][][]byte) ([]string, error) {
	var err error
	cr := ctl.Result{}
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
	sql := "select mo_ctl('dn','Backup','')"
	res, err := exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}

	var retByts [][][]byte
	res.ReadRows(func(cols []*vector.Vector) bool {
		retByts = append(retByts, executor.GetBytesRows(cols[0]))
		return true
	})
	res.Close()

	fileName, err := getFileNames(ctx, retByts)
	if err != nil {
		return err
	}
	return execBackup(ctx, srcFs, dstFs, fileName)
}

func execBackup(ctx context.Context, srcFs, dstFs fileservice.FileService, names []string) error {
	backupTime := names[0]
	trimInfo := names[1]
	names = names[1:]
	files := make(map[string]*fileservice.DirEntry, 0)
	table := gc.NewGCTable()
	gcFileMap := make(map[string]string)
	softDeletes := make(map[string]map[uint16]bool)
	for i, name := range names {
		if len(name) == 0 {
			continue
		}
		ckpStr := strings.Split(name, ":")
		if len(ckpStr) != 2 && i > 0 {
			return moerr.NewInternalError(ctx, fmt.Sprintf("invalid checkpoint string: %v", ckpStr))
		}
		metaLoc := ckpStr[0]
		version, err := strconv.ParseUint(ckpStr[1], 10, 32)
		if err != nil {
			return err
		}
		key, err := blockio.EncodeLocationFromString(metaLoc)
		if err != nil {
			return err
		}
		var locations []objectio.Location
		var data *logtail.CheckpointData
		if i == 0 {
			locations, data, err = logtail.LoadCheckpointEntriesFromKey(ctx, srcFs, key, uint32(version), nil)
		} else {
			locations, data, err = logtail.LoadCheckpointEntriesFromKey(ctx, srcFs, key, uint32(version), &softDeletes)
		}
		if err != nil {
			return err
		}
		defer data.Close()
		table.UpdateTable(data)
		gcFiles := table.SoftGC()
		mergeGCFile(gcFiles, gcFileMap)
		for _, location := range locations {
			if files[location.Name().String()] == nil {
				dentry, err := srcFs.StatFile(ctx, location.Name().String())
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) &&
						isGC(gcFileMap, location.Name().String()) {
						continue
					} else {
						return err
					}
				}
				files[location.Name().String()] = dentry
			}
		}
	}

	// record files
	taeFileList := make([]*taeFile, 0, len(files))
	for _, dentry := range files {
		if dentry.IsDir {
			panic("not support dir")
		}
		checksum, err := CopyFile(ctx, srcFs, dstFs, dentry, "")
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) &&
				isGC(gcFileMap, dentry.Name) {
				continue
			} else {
				return err
			}

		}
		taeFileList = append(taeFileList, &taeFile{
			path:     dentry.Name,
			size:     dentry.Size,
			checksum: checksum,
		})
	}

	sizeList, err := CopyDir(ctx, srcFs, dstFs, "ckp")
	if err != nil {
		return err
	}
	taeFileList = append(taeFileList, sizeList...)
	sizeList, err = CopyDir(ctx, srcFs, dstFs, "gc")
	if err != nil {
		return err
	}
	taeFileList = append(taeFileList, sizeList...)
	if trimInfo != "" {
		ckpStr := strings.Split(trimInfo, ":")
		if len(ckpStr) != 5 {
			return moerr.NewInternalError(ctx, fmt.Sprintf("invalid checkpoint string: %v", ckpStr))
		}
		cnLoc := ckpStr[0]
		mergeEnd := ckpStr[2]
		tnLoc := ckpStr[3]
		mergeStart := ckpStr[4]
		version, err := strconv.ParseUint(ckpStr[1], 10, 32)
		if err != nil {
			return err
		}
		cnLocation, err := blockio.EncodeLocationFromString(cnLoc)
		if err != nil {
			return err
		}
		tnLocation, err := blockio.EncodeLocationFromString(tnLoc)
		if err != nil {
			return err
		}
		end := types.StringToTS(mergeEnd)
		start := types.StringToTS(mergeStart)
		var checkpointFiles []string
		cnLocation, tnLocation, checkpointFiles, err = logtail.ReWriteCheckpointAndBlockFromKey(ctx, srcFs, dstFs,
			cnLocation, tnLocation, uint32(version), start, softDeletes)
		for _, name := range checkpointFiles {
			dentry, err := dstFs.StatFile(ctx, name)
			if err != nil {
				return err
			}
			taeFileList = append(taeFileList, &taeFile{
				path: dentry.Name,
				size: dentry.Size,
			})
		}
		if err != nil {
			return err
		}
		file, err := checkpoint.MergeCkpMeta(ctx, dstFs, cnLocation, tnLocation, start, end)
		if err != nil {
			return err
		}
		dentry, err := dstFs.StatFile(ctx, file)
		if err != nil {
			return err
		}
		taeFileList = append(taeFileList, &taeFile{
			path: "ckp/" + dentry.Name,
			size: dentry.Size,
		})
	}
	//save tae files size
	err = saveTaeFilesList(ctx, dstFs, taeFileList, backupTime)
	if err != nil {
		return err
	}
	return nil
}

func CopyDir(ctx context.Context, srcFs, dstFs fileservice.FileService, dir string) ([]*taeFile, error) {
	var checksum []byte
	files, err := srcFs.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	taeFileList := make([]*taeFile, 0, len(files))
	for _, file := range files {
		if file.IsDir {
			panic("not support dir")
		}
		checksum, err = CopyFile(ctx, srcFs, dstFs, &file, dir)
		if err != nil {
			return nil, err
		}
		taeFileList = append(taeFileList, &taeFile{
			path:     dir + string(os.PathSeparator) + file.Name,
			size:     file.Size,
			checksum: checksum,
		})
	}
	return taeFileList, nil
}

// CopyFile copy file from srcFs to dstFs and return checksum of the written file.
func CopyFile(ctx context.Context, srcFs, dstFs fileservice.FileService, dentry *fileservice.DirEntry, dstDir string) ([]byte, error) {
	name := dentry.Name
	if dstDir != "" {
		name = path.Join(dstDir, name)
	}
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 1),
		Policy:   fileservice.SkipAllCache,
	}
	ioVec.Entries[0] = fileservice.IOEntry{
		Offset: 0,
		Size:   dentry.Size,
	}
	err := srcFs.Read(ctx, ioVec)
	if err != nil {
		return nil, err
	}
	dstIoVec := fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 1),
		Policy:   fileservice.SkipAllCache,
	}
	dstIoVec.Entries[0] = fileservice.IOEntry{
		Offset: 0,
		Data:   ioVec.Entries[0].Data,
		Size:   dentry.Size,
	}
	err = dstFs.Write(ctx, dstIoVec)
	if err != nil {
		return nil, err
	}
	checksum := sha256.Sum256(ioVec.Entries[0].Data)
	return checksum[:], err
}

func mergeGCFile(gcFiles []string, gcFileMap map[string]string) {
	for _, gcFile := range gcFiles {
		if gcFileMap[gcFile] == "" {
			gcFileMap[gcFile] = gcFile
		}
	}
}

func isGC(gcFileMap map[string]string, name string) bool {
	return gcFileMap[name] != ""
}

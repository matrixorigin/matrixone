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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"io"
	"os"
	"path"
	runtime2 "runtime"
	"strconv"
	"strings"
	"sync"
	"time"
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

func BackupData(ctx context.Context, srcFs, dstFs fileservice.FileService, dir string, count int) error {
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
	return execBackup(ctx, srcFs, dstFs, fileName, count)
}

func getParallelCount(count int) int {
	if count > 0 && count < 512 {
		return count
	}
	cupNum := runtime2.NumCPU()
	if cupNum < 8 {
		return 50
	} else if cupNum < 16 {
		return 80
	} else if cupNum < 32 {
		return 128
	} else if cupNum < 64 {
		return 256
	}
	return 512
}

// parallelCopyData copy data from srcFs to dstFs in parallel
func parallelCopyData(srcFs, dstFs fileservice.FileService,
	files map[string]objectio.Location,
	parallelCount int,
	gcFileMap map[string]string,
) ([]*taeFile, error) {
	var copyCount, skipCount, copySize int64
	var printMutex, fileMutex sync.Mutex
	stopPrint := false
	defer func() {
		printMutex.Lock()
		if !stopPrint {
			stopPrint = true
		}
		printMutex.Unlock()
	}()
	// record files
	taeFileList := make([]*taeFile, 0, len(files))

	jobScheduler := tasks.NewParallelJobScheduler(parallelCount)
	defer jobScheduler.Stop()
	go func() {
		for {
			printMutex.Lock()
			if stopPrint {
				printMutex.Unlock()
				break
			}
			printMutex.Unlock()
			fileMutex.Lock()
			logutil.Info("backup", common.OperationField("copy file"),
				common.AnyField("copy file size", copySize),
				common.AnyField("copy file num", copyCount),
				common.AnyField("skip file num", skipCount),
				common.AnyField("total file num", len(files)))
			fileMutex.Unlock()
			time.Sleep(time.Second * 5)
		}
	}()

	backupJobs := make([]*tasks.Job, len(files))
	getJob := func(srcFs, dstFs fileservice.FileService, location objectio.Location) *tasks.Job {
		job := new(tasks.Job)
		job.Init(context.Background(), location.Name().String(), tasks.JTAny,
			func(_ context.Context) *tasks.JobResult {

				name := location.Name().String()
				size := location.Extent().End() + objectio.FooterSize
				checksum, err := CopyFile(context.Background(), srcFs, dstFs, location.Name().String(), "")
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) &&
						isGC(gcFileMap, name) {
						fileMutex.Lock()
						skipCount++
						fileMutex.Unlock()
						return &tasks.JobResult{
							Res: nil,
						}
					} else {
						return &tasks.JobResult{
							Err: err,
							Res: nil,
						}
					}
				}
				fileMutex.Lock()
				copyCount++
				copySize += int64(size)
				taeFileList = append(taeFileList, &taeFile{
					path:     name,
					size:     int64(size),
					checksum: checksum,
				})
				fileMutex.Unlock()
				return &tasks.JobResult{
					Res: nil,
				}
			})
		return job
	}

	idx := 0
	for n := range files {
		backupJobs[idx] = getJob(srcFs, dstFs, files[n])
		idx++
	}

	for n := range backupJobs {
		err := jobScheduler.Schedule(backupJobs[n])
		if err != nil {
			logutil.Infof("schedule job failed %v", err.Error())
			return nil, err
		}

	}

	for n := range backupJobs {

		ret := backupJobs[n].WaitDone()
		if ret.Err != nil {
			logutil.Infof("wait job done failed %v", ret.Err.Error())
			return nil, ret.Err
		}
	}

	logutil.Info("backup", common.OperationField("copy file"),
		common.AnyField("copy file size", copySize),
		common.AnyField("copy file num", copyCount),
		common.AnyField("skip file num", skipCount),
		common.AnyField("total file num", len(files)))
	return taeFileList, nil
}

func execBackup(ctx context.Context, srcFs, dstFs fileservice.FileService, names []string, count int) error {
	backupTime := names[0]
	trimInfo := names[1]
	names = names[1:]
	files := make(map[string]objectio.Location, 0)
	table := gc.NewGCTable()
	gcFileMap := make(map[string]string)
	softDeletes := make(map[string]bool)
	var loadDuration, copyDuration, reWriteDuration time.Duration
	var oNames []objectio.Location
	parallelNum := getParallelCount(count)
	logutil.Info("backup", common.OperationField("start backup"),
		common.AnyField("backup time", backupTime),
		common.AnyField("checkpoint num", len(names)),
		common.AnyField("parallel num", parallelNum))
	defer func() {
		logutil.Info("backup", common.OperationField("end backup"),
			common.AnyField("load checkpoint cost", loadDuration),
			common.AnyField("copy file cost", copyDuration),
			common.AnyField("rewrite checkpoint cost", reWriteDuration))
	}()
	now := time.Now()
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
		var oneNames []objectio.Location
		var data *logtail.CheckpointData
		if i == 0 {
			oneNames, data, err = logtail.LoadCheckpointEntriesFromKey(ctx, srcFs, key, uint32(version), nil)
		} else {
			oneNames, data, err = logtail.LoadCheckpointEntriesFromKey(ctx, srcFs, key, uint32(version), &softDeletes)
		}
		if err != nil {
			return err
		}
		defer data.Close()
		table.UpdateTable(data)
		gcFiles := table.SoftGC()
		mergeGCFile(gcFiles, gcFileMap)
		oNames = append(oNames, oneNames...)
	}
	loadDuration += time.Since(now)
	now = time.Now()
	for _, oName := range oNames {
		if files[oName.Name().String()] == nil {
			files[oName.Name().String()] = oName
		}
	}

	// trim checkpoint and block
	var cnLoc, tnLoc, mergeStart, mergeEnd string
	var end, start types.TS
	var version uint64
	if trimInfo != "" {
		var err error
		ckpStr := strings.Split(trimInfo, ":")
		if len(ckpStr) != 5 {
			return moerr.NewInternalError(ctx, fmt.Sprintf("invalid checkpoint string: %v", ckpStr))
		}
		cnLoc = ckpStr[0]
		mergeEnd = ckpStr[2]
		tnLoc = ckpStr[3]
		mergeStart = ckpStr[4]
		end = types.StringToTS(mergeEnd)
		start = types.StringToTS(mergeStart)
		version, err = strconv.ParseUint(ckpStr[1], 10, 32)
		if err != nil {
			return err
		}
	}

	// copy data
	taeFileList, err := parallelCopyData(srcFs, dstFs, files, parallelNum, gcFileMap)
	if err != nil {
		return err
	}

	// copy checkpoint and gc meta
	sizeList, err := CopyDir(ctx, srcFs, dstFs, "ckp", start)
	if err != nil {
		return err
	}
	taeFileList = append(taeFileList, sizeList...)
	sizeList, err = CopyDir(ctx, srcFs, dstFs, "gc", start)
	if err != nil {
		return err
	}
	copyDuration += time.Since(now)
	taeFileList = append(taeFileList, sizeList...)
	now = time.Now()
	if trimInfo != "" {
		cnLocation, err := blockio.EncodeLocationFromString(cnLoc)
		if err != nil {
			return err
		}
		tnLocation, err := blockio.EncodeLocationFromString(tnLoc)
		if err != nil {
			return err
		}
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
	reWriteDuration += time.Since(now)
	//save tae files size
	err = saveTaeFilesList(ctx, dstFs, taeFileList, backupTime)
	if err != nil {
		return err
	}
	return nil
}

func CopyDir(ctx context.Context, srcFs, dstFs fileservice.FileService, dir string, backup types.TS) ([]*taeFile, error) {
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
		start, _ := blockio.DecodeCheckpointMetadataFileName(file.Name)
		if !backup.IsEmpty() && start.GreaterEq(backup) {
			logutil.Infof("[Backup] skip file %v", file.Name)
			continue
		}
		checksum, err = CopyFile(ctx, srcFs, dstFs, file.Name, dir)
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
func CopyFile(ctx context.Context, srcFs, dstFs fileservice.FileService, name, dstDir string) ([]byte, error) {
	if dstDir != "" {
		name = path.Join(dstDir, name)
	}
	var reader io.ReadCloser
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries: []fileservice.IOEntry{
			{
				ReadCloserForRead: &reader,
				Offset:            0,
				Size:              -1,
			},
		},
		Policy: fileservice.SkipAllCache,
	}

	err := srcFs.Read(ctx, ioVec)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	// hash
	hasher := sha256.New()
	hashingReader := io.TeeReader(reader, hasher)
	dstIoVec := fileservice.IOVector{
		FilePath: name,
		Entries: []fileservice.IOEntry{
			{
				ReaderForWrite: hashingReader,
				Offset:         0,
				Size:           -1,
			},
		},
		Policy: fileservice.SkipAllCache,
	}

	err = dstFs.Write(ctx, dstIoVec)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
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

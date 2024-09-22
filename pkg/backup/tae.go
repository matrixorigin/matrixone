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
	"io"
	"os"
	"path"
	runtime2 "runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
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

func BackupData(
	ctx context.Context,
	sid string,
	srcFs, dstFs fileservice.FileService,
	dir string,
	config *Config,
) error {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.InternalSQLExecutor)
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
	res.ReadRows(func(_ int, cols []*vector.Vector) bool {
		retByts = append(retByts, executor.GetBytesRows(cols[0]))
		return true
	})
	res.Close()

	fileName, err := getFileNames(ctx, retByts)
	if err != nil {
		return err
	}
	count := config.Parallelism
	return execBackup(ctx, sid, srcFs, dstFs, fileName, int(count), config.BackupTs, config.BackupType)
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
	files map[string]*objectio.BackupObject,
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
	errC := make(chan error, 1)
	defer close(errC)
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
	getJob := func(srcFs, dstFs fileservice.FileService, backupObject *objectio.BackupObject) *tasks.Job {
		job := new(tasks.Job)
		job.Init(context.Background(), backupObject.Location.Name().String(), tasks.JTAny,
			func(_ context.Context) *tasks.JobResult {

				name := backupObject.Location.Name().String()
				size := backupObject.Location.Extent().End() + objectio.FooterSize
				if !backupObject.NeedCopy {
					fileMutex.Lock()
					copyCount++
					copySize += int64(size)
					taeFileList = append(taeFileList, &taeFile{
						path:     name,
						size:     int64(size),
						needCopy: false,
						ts:       backupObject.CrateTS,
					})
					fileMutex.Unlock()
					return &tasks.JobResult{
						Res: nil,
					}
				}
				checksum, err := CopyFileWithRetry(context.Background(), srcFs, dstFs, backupObject.Location.Name().String(), "")
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
						// TODO: handle file not found, maybe GC
						fileMutex.Lock()
						skipCount++
						fileMutex.Unlock()
						return &tasks.JobResult{
							Res: nil,
						}
					} else {
						errC <- err
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
					needCopy: backupObject.NeedCopy,
					ts:       backupObject.CrateTS,
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
		select {
		case err = <-errC:
			logutil.Infof("copy file failed %v", err.Error())
			return nil, err
		default:
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

func execBackup(
	ctx context.Context,
	sid string,
	srcFs, dstFs fileservice.FileService,
	names []string,
	count int,
	ts types.TS,
	typ string,
) error {
	backupTime := names[0]
	trimInfo := names[1]
	names = names[1:]
	files := make(map[string]*objectio.BackupObject, 0)
	gcFileMap := make(map[string]string)
	softDeletes := make(map[string]bool)
	var loadDuration, copyDuration, reWriteDuration time.Duration
	var oNames []*objectio.BackupObject
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
	baseTS := ts
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
		var oneNames []*objectio.BackupObject
		var data *logtail.CheckpointData
		if i == 0 {
			oneNames, data, err = logtail.LoadCheckpointEntriesFromKey(ctx, sid, srcFs, key, uint32(version), nil, &baseTS)
		} else {
			oneNames, data, err = logtail.LoadCheckpointEntriesFromKey(ctx, sid, srcFs, key, uint32(version), &softDeletes, &baseTS)
		}
		if err != nil {
			return err
		}
		defer data.Close()
		oNames = append(oNames, oneNames...)
	}
	loadDuration += time.Since(now)
	now = time.Now()
	for _, oName := range oNames {
		if files[oName.Location.Name().String()] == nil {
			files[oName.Location.Name().String()] = oName
		}
	}

	// trim checkpoint and block
	var cnLoc, mergeStart, mergeEnd string
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
		// tnLoc = ckpStr[3]
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
	sizeList, minTs, err := CopyCheckpointDir(ctx, srcFs, dstFs, "ckp", start)
	if err != nil {
		return err
	}
	taeFileList = append(taeFileList, sizeList...)
	sizeList, err = CopyGCDir(ctx, srcFs, dstFs, "gc", start, minTs)
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
		var (
			checkpointFiles []string
			tnLocation      objectio.Location
		)
		cnLocation, tnLocation, checkpointFiles, err = logtail.ReWriteCheckpointAndBlockFromKey(ctx, sid, srcFs, dstFs,
			cnLocation, uint32(version), start)
		for _, name := range checkpointFiles {
			dentry, err := dstFs.StatFile(ctx, name)
			if err != nil {
				return err
			}
			taeFileList = append(taeFileList, &taeFile{
				path:     dentry.Name,
				size:     dentry.Size,
				needCopy: true,
				ts:       start,
			})
		}
		if err != nil {
			return err
		}
		file, err := checkpoint.MergeCkpMeta(ctx, sid, dstFs, cnLocation, tnLocation, start, end)
		if err != nil {
			return err
		}
		dentry, err := dstFs.StatFile(ctx, file)
		if err != nil {
			return err
		}
		taeFileList = append(taeFileList, &taeFile{
			path:     "ckp/" + dentry.Name,
			size:     dentry.Size,
			needCopy: true,
			ts:       start,
		})
	}
	reWriteDuration += time.Since(now)
	//save tae files size
	err = saveTaeFilesList(ctx, dstFs, taeFileList, backupTime, start.ToString(), typ)
	if err != nil {
		return err
	}
	return nil
}

// CopyCheckpointDir copy checkpoint dir from srcFs to dstFs
// return taeFile list
// copy: if copy is true,it means not to check the suffix name and copy all files.
func copyFileAndGetMetaFiles(
	ctx context.Context,
	srcFs, dstFs fileservice.FileService,
	dir string,
	backup types.TS,
	decodeFunc func(string) (types.TS, types.TS, string),
	copy bool,
) ([]*taeFile, []*checkpoint.MetaFile, []fileservice.DirEntry, error) {
	files, err := srcFs.List(ctx, dir)
	if err != nil {
		return nil, nil, nil, err
	}
	taeFileList := make([]*taeFile, 0, len(files))
	metaFiles := make([]*checkpoint.MetaFile, 0)
	var checksum []byte
	for i, file := range files {
		if file.IsDir {
			panic("not support dir")
		}
		start, end, ext := decodeFunc(file.Name)
		if !backup.IsEmpty() && start.GE(&backup) {
			logutil.Infof("[Backup] skip file %v", file.Name)
			continue
		}
		if copy || ext == blockio.AcctExt || ext == blockio.SnapshotExt {
			checksum, err = CopyFileWithRetry(ctx, srcFs, dstFs, file.Name, dir)
			if err != nil {
				return nil, nil, nil, err
			}
			taeFileList = append(taeFileList, &taeFile{
				path:     dir + string(os.PathSeparator) + file.Name,
				size:     file.Size,
				checksum: checksum,
				needCopy: true,
				ts:       backup,
			})
		}

		if copy || ext == blockio.CheckpointExt || ext == blockio.GCFullExt {
			metaFile := checkpoint.NewMetaFile(i, start, end, file.Name)
			metaFiles = append(metaFiles, metaFile)
		}
	}

	if len(metaFiles) == 0 {
		return taeFileList, metaFiles, files, nil
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		end1 := metaFiles[i].GetEnd()
		end2 := metaFiles[j].GetEnd()
		return end1.LT(&end2)
	})

	return taeFileList, metaFiles, files, nil
}

func CopyGCDir(
	ctx context.Context,
	srcFs, dstFs fileservice.FileService,
	dir string,
	backup, min types.TS,
) ([]*taeFile, error) {
	var checksum []byte

	taeFileList, metaFiles, files, err := copyFileAndGetMetaFiles(
		ctx, srcFs, dstFs, dir, backup, blockio.DecodeGCMetadataFileName, false)
	if err != nil {
		return nil, err
	}
	for i, metaFile := range metaFiles {
		name := metaFile.GetName()
		if i == len(metaFiles)-1 {
			end := metaFile.GetEnd()
			if !min.IsEmpty() && end.LT(&min) {
				// It means that the gc consumption is too slow, and the gc water level needs to be raised.
				// Otherwise, the gc will not work after the cluster is restored because it cannot find the checkpoint.
				// The gc water level is determined by the name of the meta,
				// so the name of the last gc meta needs to be modified.
				name = blockio.UpdateGCMetadataFileName(name, end, min)
			}
		}
		checksum, err = CopyFileWithRetry(ctx, srcFs, dstFs, metaFile.GetName(), dir, name)
		if err != nil {
			return nil, err
		}
		taeFileList = append(taeFileList, &taeFile{
			path:     dir + string(os.PathSeparator) + name,
			size:     files[metaFile.GetIndex()].Size,
			checksum: checksum,
			needCopy: true,
			ts:       backup,
		})
	}
	return taeFileList, nil
}

func CopyCheckpointDir(
	ctx context.Context,
	srcFs, dstFs fileservice.FileService,
	dir string, backup types.TS,
) ([]*taeFile, types.TS, error) {
	decodeFunc := func(name string) (types.TS, types.TS, string) {
		start, end := blockio.DecodeCheckpointMetadataFileName(name)
		return start, end, ""
	}
	taeFileList, metaFiles, _, err := copyFileAndGetMetaFiles(ctx, srcFs, dstFs, dir, backup, decodeFunc, true)
	if err != nil {
		return nil, types.TS{}, err
	}

	// minTs is the end of the last global checkpoint, which is needed when copying gc meta
	minTs := types.TS{}
	for i := len(metaFiles) - 1; i >= 0; i-- {
		ckpStart := metaFiles[i].GetStart()
		if ckpStart.IsEmpty() {
			minTs = metaFiles[i].GetEnd()
			break
		}
	}
	return taeFileList, minTs, nil
}

func CopyFileWithRetry(ctx context.Context, srcFs, dstFs fileservice.FileService, name, dstDir string, newName ...string) ([]byte, error) {
	return fileservice.DoWithRetry(
		"CopyFile",
		func() ([]byte, error) {
			return CopyFile(ctx, srcFs, dstFs, name, dstDir, newName...)
		},
		64,
		fileservice.IsRetryableError,
	)
}

// CopyFile copy file from srcFs to dstFs and return checksum of the written file.
func CopyFile(ctx context.Context, srcFs, dstFs fileservice.FileService, name, dstDir string, newNames ...string) ([]byte, error) {
	newName := name
	if dstDir != "" {
		name = path.Join(dstDir, name)
		if len(newNames) > 0 {
			newName = path.Join(dstDir, newNames[0])
		} else {
			newName = name
		}
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
		FilePath: newName,
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

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
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// setupFilesystem returns a FileService for ETL which the reader outside the matrixone
// can read the content. a FileService for Backup which only the matrixone
// can read the content.
func setupFilesystem(ctx context.Context, path string, forETL bool) (res fileservice.FileService, readPath string, err error) {
	return setupFileservice(ctx, &pathConfig{
		isS3:             false,
		forETL:           forETL,
		filesystemConfig: filesystemConfig{path: path},
	})
}

// setupS3 returns a FileService for ETL which the reader outside the matrixone
// can read the content.a FileService for Backup which only the matrixone
// can read the content.
func setupS3(ctx context.Context, s3 *s3Config, forETL bool) (res fileservice.FileService, readPath string, err error) {
	return setupFileservice(ctx, &pathConfig{
		isS3:     true,
		forETL:   forETL,
		s3Config: *s3,
	})
}

func setupFileservice(ctx context.Context, conf *pathConfig) (res fileservice.FileService, readPath string, err error) {
	var s3opts string
	if conf.isS3 {
		s3opts, err = makeS3Opts(&conf.s3Config)
		if err != nil {
			return nil, "", err
		}
		if conf.forETL {
			s3path := fileservice.JoinPath(s3opts, etlFSDir(conf.filepath))
			//TODO:remove debug
			logutil.Debugf("==>s3path: %s", s3path)
			res, readPath, err = fileservice.GetForETL(ctx, nil, s3path)
			if err != nil {
				return nil, "", err
			}
		} else {
			s3path := fileservice.JoinPath(s3opts, conf.filepath)
			res, err = fileservice.GetForBackup(ctx, s3path)
			if err != nil {
				return nil, "", err
			}
		}
		res = fileservice.SubPath(res, conf.filepath)
	} else {
		if conf.forETL {
			res, readPath, err = fileservice.GetForETL(ctx, nil, etlFSDir(conf.path))
			if err != nil {
				return nil, "", err
			}
		} else {
			res, err = fileservice.GetForBackup(ctx, conf.path)
			if err != nil {
				return nil, "", err
			}
		}
	}

	return res, readPath, err
}

func makeS3Opts(s3 *s3Config) (string, error) {
	var err error
	buf := new(strings.Builder)
	w := csv.NewWriter(buf)
	opts := []string{
		"s3-opts",
		"endpoint=" + s3.endpoint,
		"region=" + s3.region,
		"key=" + s3.accessKeyId,
		"secret=" + s3.secretAccessKey,
		"bucket=" + s3.bucket,
		"role-arn=" + s3.roleArn,
		"is-minio=" + strconv.FormatBool(s3.isMinio),
		//"external-id="              /*+ param.S3Param.ExternalId*/,
	}
	if err = w.Write(opts); err != nil {
		return "", err
	}
	w.Flush()
	return buf.String(), nil
}

func etlFSDir(filepath string) string {
	return filepath + "/_"
}

func writeFile(ctx context.Context, fs fileservice.FileService, path string, data []byte) error {
	var err error
	//write file
	_, err = fileservice.DoWithRetry(
		"BackupWrite",
		func() (int, error) {
			return 0, fs.Write(ctx, fileservice.IOVector{
				FilePath: path,
				Entries: []fileservice.IOEntry{
					{
						Offset: 0,
						Size:   int64(len(data)),
						Data:   data,
					},
				},
			})
		},
		64,
		fileservice.IsRetryableError,
	)
	if err != nil {
		return err
	}

	checksum := sha256.Sum256(data)

	//write checksum file for the file
	checksumFile := path + ".sha256"
	_, err = fileservice.DoWithRetry(
		"BackupWrite",
		func() (int, error) {
			return 0, fs.Write(ctx, fileservice.IOVector{
				FilePath: checksumFile,
				Entries: []fileservice.IOEntry{
					{
						Offset: 0,
						Size:   int64(len(checksum)),
						Data:   checksum[:],
					},
				},
			})
		},
		64,
		fileservice.IsRetryableError,
	)
	return err
}

func readFile(ctx context.Context, fs fileservice.FileService, path string) ([]byte, error) {
	var (
		err error
	)
	iov := &fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   -1,
			},
		},
		Module: v2.Backup,
	}
	err = fs.Read(ctx, iov)
	if err != nil {
		return nil, err
	}
	return iov.Entries[0].Data, err
}

func hexStr(d []byte) string {
	return fmt.Sprintf("%x", d)
}

// readFileAndCheck reads data and compare the checksum with the one in checksum file.
// if the checksum is equal, it returns the data of the file.
func readFileAndCheck(ctx context.Context, fs fileservice.FileService, path string) ([]byte, error) {
	var (
		err               error
		data              []byte
		savedChecksumData []byte
		newChecksumData   []byte
		savedChecksum     string
		newChecksum       string
	)
	data, err = readFile(ctx, fs, path)
	if err != nil {
		return nil, err
	}

	//calculate the checksum
	hash := sha256.New()
	hash.Write(data)
	newChecksumData = hash.Sum(nil)
	newChecksum = hexStr(newChecksumData)

	checksumFile := path + ".sha256"
	savedChecksumData, err = readFile(ctx, fs, checksumFile)
	if err != nil {
		return nil, err
	}
	savedChecksum = hexStr(savedChecksumData)
	//3. compare the checksum
	if strings.Compare(savedChecksum, newChecksum) != 0 {
		return nil, moerr.NewInternalError(ctx, checksumErrorInfo(newChecksum, savedChecksum, path))
	}
	return data, err
}

func checksumErrorInfo(newChecksum, savedChecksum, path string) string {
	return fmt.Sprintf("checksum %s of %s is not equal to %s ", newChecksum, path, savedChecksum)
}

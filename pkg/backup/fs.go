package backup

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"strconv"
	"strings"
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
			res, readPath, err = fileservice.GetForETL(nil, s3path)
			if err != nil {
				return nil, "", err
			}
		} else {
			s3path := fileservice.JoinPath(s3opts, conf.filepath)
			res, err = fileservice.GetForBackup(s3path)
			if err != nil {
				return nil, "", err
			}
		}
		res = fileservice.SubPath(res, conf.filepath)
	} else {
		if conf.forETL {
			res, readPath, err = fileservice.GetForETL(nil, etlFSDir(conf.path))
			if err != nil {
				return nil, "", err
			}
		} else {
			res, err = fileservice.GetForBackup(conf.path)
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

func directoryIsEmpty(ctx context.Context, fs fileservice.FileService, dir string) (bool, error) {
	var err error
	var dirEntries []fileservice.DirEntry
	dirEntries, err = fs.List(ctx, "")
	if err != nil {
		return false, err
	}
	for i, entry := range dirEntries {
		logutil.Debugf("%d: %s", i, dirEntryToString(entry))
	}
	return len(dirEntries) == 0, err
}

func writeFile(ctx context.Context, fs fileservice.FileService, path string, data []byte) error {
	return fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(data)),
				Data:   data,
			},
		},
	})
}

func dirEntryToString(dir fileservice.DirEntry) string {
	return fmt.Sprintf("Name %s Dir %v Size %d", dir.Name, dir.IsDir, dir.Size)
}

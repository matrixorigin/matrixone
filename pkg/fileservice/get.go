// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func Get[T any](fs FileService, name string) (res T, err error) {
	lowerName := strings.ToLower(name)
	if fs, ok := fs.(*FileServices); ok {
		f, ok := fs.mappings[lowerName]
		if !ok {
			err = moerr.NewNoServiceNoCtx(name)
			return
		}
		res, ok = f.(T)
		if !ok {
			err = moerr.NewNoServiceNoCtx(name)
			return
		}
		return
	}
	var ok bool
	res, ok = fs.(T)
	if !ok {
		err = moerr.NewNoServiceNoCtx(name)
		return
	}
	if !strings.EqualFold(fs.Name(), lowerName) {
		err = moerr.NewNoServiceNoCtx(name)
		return
	}
	return
}

func etlParallelMode(ctx context.Context) ParallelMode {
	if mode, ok := parallelModeFromContext(ctx); ok {
		return mode
	}
	if mode, ok := parseParallelMode(strings.TrimSpace(os.Getenv("MO_ETL_PARALLEL_MODE"))); ok {
		return mode
	}
	return ParallelOff
}

// parseFileServiceArguments is shared by admission and construction. ETL has
// always selected HDFS through its service name; backup also accepts is-hdfs.
func parseFileServiceArguments(path Path, forETL bool) (args ObjectStorageArguments, err error) {
	argv := path.ServiceArguments
	switch strings.ToLower(path.Service) {
	case "s3", "minio":
		if len(argv) < 6 {
			return args, moerr.NewInvalidInputNoCtx("invalid S3 arguments")
		}
		args = ObjectStorageArguments{Endpoint: argv[0], Region: argv[1], Bucket: argv[2], KeyID: argv[3], KeySecret: argv[4], KeyPrefix: argv[5], IsMinio: strings.EqualFold(path.Service, "minio")}
		if len(argv) > 6 {
			args.Name = argv[6]
		}
	case "s3-no-key":
		if len(argv) < 4 {
			return args, moerr.NewInvalidInputNoCtx("invalid S3 arguments")
		}
		args = ObjectStorageArguments{Endpoint: argv[0], Region: argv[1], Bucket: argv[2], KeyPrefix: argv[3]}
		if len(argv) > 4 {
			args.Name = argv[4]
		}
	case "s3-opts", "opts", "options", "hdfs":
		if err = args.SetFromString(argv); err != nil {
			return args, err
		}
		if forETL || strings.EqualFold(path.Service, "hdfs") {
			args.IsHDFS = strings.EqualFold(path.Service, "hdfs")
		}
	default:
		return args, moerr.NewInvalidInputNoCtxf("no such service: %s", path.Service)
	}
	args.NoBucketValidation = true
	return args, nil
}

// GetForETL get or creates a FileService instance for ETL operations
// if service part of path is empty, a LocalETLFS will be created
// if service part of path is not empty, a ETLFileService typed instance will be extracted from fs argument
// if service part of path is argumented, a FileService instance will be created dynamically with those arguments
// supported dynamic file service:
// s3,<endpoint>,<region>,<bucket>,<key>,<secret>,<prefix>
// s3-no-key,<endpoint>,<region>,<bucket>,<prefix>
// minio,<endpoint>,<region>,<bucket>,<key>,<secret>,<prefix>
// s3-opts,endpoint=<endpoint>,region=<region>,bucket=<bucket>,key=<key>,secret=<secret>,prefix=<prefix>,role-arn=<role arn>,external-id=<external id>
//
//	key value pairs can be in any order
func GetForETL(ctx context.Context, fs FileService, path string) (res ETLFileService, readPath string, err error) {
	fsPath, err := ParsePath(path)
	if err != nil {
		return nil, "", err
	}
	return getForETLWithParsedPath(ctx, fs, path, fsPath)
}

// GetForETLWithParsedPath verifies the raw target against its supplied parse.
// Construction uses the authoritative parse and the current SQL policy.
func GetForETLWithParsedPath(ctx context.Context, fs FileService, rawPath string, fsPath Path) (res ETLFileService, readPath string, err error) {
	actual, err := ParsePath(rawPath)
	if err != nil {
		return nil, "", err
	}
	if actual.Service != fsPath.Service || actual.File != fsPath.File || !slices.Equal(actual.ServiceArguments, fsPath.ServiceArguments) {
		return nil, "", moerr.NewInvalidInputNoCtx("SQL file target disagrees with its parsed path")
	}
	return getForETLWithParsedPath(ctx, fs, rawPath, actual)
}

func getForETLWithParsedPath(ctx context.Context, fs FileService, rawPath string, fsPath Path) (res ETLFileService, readPath string, err error) {
	args, err := admitSQLFilePath(ctx, fsPath, true)
	if err != nil {
		return nil, "", err
	}

	if fsPath.Service == "" {
		// no service, create local ETL fs
		dir, file := filepath.Split(rawPath)
		res, err = NewLocalETLFS("etl", dir)
		if err != nil {
			return nil, "", err
		}
		readPath = file

	} else if len(fsPath.ServiceArguments) > 0 {
		args.ParallelMode = etlParallelMode(ctx)
		res, err = NewS3FS(ctx, args, DisabledCacheConfig, nil, true, true)
		if err != nil {
			return nil, "", err
		}

		readPath = fsPath.File

	} else {
		// get etl fs
		res, err = Get[ETLFileService](fs, fsPath.Service)
		if err != nil {
			return nil, "", err
		}
		readPath = rawPath
	}

	return
}

// LocalBackendOf returns the LocalFS backend ("DISK" or "DISK-V2") whose on-disk
// format matches fs, for creating a local copy (e.g. a filesystem backup) in the
// same framing. Raw services (DISK-V2 LocalFS, S3FS, ...) map to "DISK-V2"; only
// the legacy CRC32-framed LocalFS maps to "DISK". This lets a backup follow the
// cluster's data fileservice format (the .toml) so restore reads it correctly.
func LocalBackendOf(fs FileService) string {
	if lfs, ok := fs.(*LocalFS); ok && !lfs.noChecksum {
		return diskFileServiceBackend
	}
	// Everything else exposes the raw (unframed) on-disk layout — a DISK-V2
	// LocalFS, S3FS, MemoryFS, ErrorFS, … — so a local copy of their data must
	// use the DISK-V2 (raw) backend. Only the legacy CRC32-framed LocalFS above
	// maps to DISK.
	return diskV2FileServiceBackend
}

// GetForBackup creates a FileService instance for backup operations.
// if service part of path is empty, a LocalFS is created; backend selects its
// on-disk format ("DISK-V2" for raw, anything else incl. "DISK"/empty for the
// legacy CRC32-framed format). backend is ignored for argumented (s3-opts) specs.
// if service part of path is argumented, a FileService instance will be created dynamically with those arguments
// supported dynamic file service:
// s3-opts,endpoint=<endpoint>,region=<region>,bucket=<bucket>,key=<key>,secret=<secret>,prefix=<prefix>,role-arn=<role arn>,external-id=<external id>,is-minio=<is-minio>
func GetForBackup(ctx context.Context, spec string, backend string) (res FileService, err error) {
	fsPath, err := ParsePath(spec)
	if err != nil {
		return nil, err
	}

	sqlArgs, err := admitSQLFilePath(ctx, fsPath, false)
	if err != nil {
		return nil, err
	}
	if fsPath.Service == "" {
		// no service, create local fs in the format matching the cluster data
		if strings.EqualFold(backend, diskV2FileServiceBackend) {
			res, err = NewLocalFS2(ctx, "backup", spec, DisabledCacheConfig, nil)
		} else {
			res, err = NewLocalFS(ctx, "backup", spec, DisabledCacheConfig, nil)
		}
		if err != nil {
			return nil, err
		}

	} else if len(fsPath.ServiceArguments) > 0 {
		// service with arguments, create dynamically
		switch fsPath.Service {

		case "s3-opts":
			args := sqlArgs
			args.NoBucketValidation = true
			setBackupConnectionPool(&args)
			res, err = NewS3FS(
				ctx,
				args,
				DisabledCacheConfig,
				nil,
				true,
				false,
			)
			if err != nil {
				return
			}

		default:
			err = moerr.NewInvalidInputNoCtxf("no such service: %s", fsPath.Service)
		}

	} else {
		return nil, moerr.NewInvalidInputNoCtxf("unknown file service: %v", spec)
	}

	return
}

func setBackupConnectionPool(args *ObjectStorageArguments) {
	if args.MaxConnsPerHost == 0 {
		// A backup destination must not share the process-wide HTTP pool with
		// the cluster source file service. A streaming copy retains its source
		// response while opening the destination request; sharing a saturated
		// per-host pool would make those two operations wait on each other.
		args.MaxConnsPerHost = maxConnsPerHost
	}
}

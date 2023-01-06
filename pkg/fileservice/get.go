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
	"path/filepath"
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
func GetForETL(fs FileService, path string) (res ETLFileService, readPath string, err error) {
	fsPath, err := ParsePath(path)
	if err != nil {
		return nil, "", err
	}

	if fsPath.Service == "" {
		// no service, create local ETL fs
		dir, file := filepath.Split(path)
		res, err = NewLocalETLFS("etl", dir)
		if err != nil {
			return nil, "", err
		}
		readPath = file

	} else if len(fsPath.ServiceArguments) > 0 {
		// service with arguments, create dynamically
		switch fsPath.Service {

		case "s3":
			arguments := fsPath.ServiceArguments
			if len(arguments) < 6 {
				return nil, "", moerr.NewInvalidInputNoCtx("invalid S3 arguments")
			}
			endpoint := arguments[0]
			region := arguments[1]
			bucket := arguments[2]
			accessKey := arguments[3]
			accessSecret := arguments[4]
			keyPrefix := arguments[5]
			var name string
			if len(arguments) > 6 {
				name = arguments[6]
			}
			res, err = newS3FS([]string{
				"endpoint=" + endpoint,
				"region=" + region,
				"bucket=" + bucket,
				"key=" + accessKey,
				"secret=" + accessSecret,
				"prefix=" + keyPrefix,
				"name=" + name,
			})
			if err != nil {
				return
			}

		case "s3-no-key":
			arguments := fsPath.ServiceArguments
			if len(arguments) < 4 {
				return nil, "", moerr.NewInvalidInputNoCtx("invalid S3 arguments")
			}
			endpoint := arguments[0]
			region := arguments[1]
			bucket := arguments[2]
			keyPrefix := arguments[3]
			var name string
			if len(arguments) > 4 {
				name = arguments[4]
			}
			res, err = newS3FS([]string{
				"endpoint=" + endpoint,
				"region=" + region,
				"bucket=" + bucket,
				"prefix=" + keyPrefix,
				"name=" + name,
			})
			if err != nil {
				return
			}

		case "s3-opts":
			res, err = newS3FS(fsPath.ServiceArguments)
			if err != nil {
				return
			}

		case "minio":
			arguments := fsPath.ServiceArguments
			if len(arguments) < 6 {
				return nil, "", moerr.NewInvalidInputNoCtx("invalid S3 arguments")
			}
			endpoint := arguments[0]
			region := arguments[1]
			_ = region
			bucket := arguments[2]
			accessKey := arguments[3]
			accessSecret := arguments[4]
			keyPrefix := arguments[5]
			var name string
			if len(arguments) > 6 {
				name = arguments[6]
			}
			res, err = newS3FS([]string{
				"endpoint=" + endpoint,
				"region=" + region,
				"bucket=" + bucket,
				"prefix=" + keyPrefix,
				"name=" + name,
				"key=" + accessKey,
				"secret=" + accessSecret,
				"is-minio=true",
			})
			if err != nil {
				return
			}

		default:
			err = moerr.NewInvalidInputNoCtx("no such service: %s", fsPath.Service)
		}

		readPath = fsPath.File

	} else {
		// get etl fs
		res, err = Get[ETLFileService](fs, fsPath.Service)
		if err != nil {
			return nil, "", err
		}
		readPath = path
	}

	return
}

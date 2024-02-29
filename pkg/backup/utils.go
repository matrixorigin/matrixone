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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/version"
)

func buildInfo() string {
	infos := []string{
		"GoVersion: " + version.GoVersion,
		"BranchName: " + version.BranchName,
		"CommitID: " + version.CommitID,
		"BuildTime: " + version.BuildTime,
		"Version: " + version.Version,
	}
	return strings.Join(infos, "|")
}

// SaveLaunchConfigPath saves all config file paths for the standalone config
func SaveLaunchConfigPath(typ string, paths []string) {
	launchConfigPaths[typ] = paths
}

func getS3Config(ctx context.Context, option []string) (*s3Config, error) {
	conf := &s3Config{}
	for i := 0; i < len(option); i += 2 {
		switch strings.ToLower(option[i]) {
		case "endpoint":
			conf.endpoint = option[i+1]
		case "region":
			conf.region = option[i+1]
		case "access_key_id":
			conf.accessKeyId = option[i+1]
		case "secret_access_key":
			conf.secretAccessKey = option[i+1]
		case "bucket":
			conf.bucket = option[i+1]
		case "filepath":
			conf.filepath = option[i+1]
		case "compression":
			conf.compression = option[i+1]
		case "provider":
			conf.provider = option[i+1]
		case "role_arn":
			conf.roleArn = option[i+1]
		case "external_id":
			conf.externalId = option[i+1]
		case "format":
			format := strings.ToLower(option[i+1])
			if format != tree.CSV && format != tree.JSONLINE {
				return nil, moerr.NewBadConfig(ctx, "the format '%s' is not supported", format)
			}
			conf.format = format
		case "jsondata":
			jsondata := strings.ToLower(option[i+1])
			if jsondata != tree.OBJECT && jsondata != tree.ARRAY {
				return nil, moerr.NewBadConfig(ctx, "the jsondata '%s' is not supported", jsondata)
			}
			conf.jsonData = jsondata
			conf.format = tree.JSONLINE
		case "is_minio":
			isMinioData := strings.ToLower(option[i+1])
			if isMinioData != "true" && isMinioData != "false" {
				return nil, moerr.NewBadConfig(ctx, "the is_minio '%s' is not supported", isMinioData)
			}
			if isMinioData == "true" {
				conf.isMinio = true
			} else {
				conf.isMinio = false
			}
		case "parallelism":
			parallelismData := strings.ToLower(option[i+1])
			parall, err := strconv.ParseUint(parallelismData, 10, 16)
			if err != nil {
				return nil, moerr.NewBadConfig(ctx, "the parallelism '%s' is invalid", parallelismData)
			}
			conf.parallelism = uint16(parall)
		default:
			return nil, moerr.NewBadConfig(ctx, "the keyword '%s' is not support", strings.ToLower(option[i]))
		}
	}
	if conf.format == tree.JSONLINE && len(conf.jsonData) == 0 {
		return nil, moerr.NewBadConfig(ctx, "the jsondata must be specified")
	}
	if len(conf.format) == 0 {
		conf.format = tree.CSV
	}
	return conf, nil
}

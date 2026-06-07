// Copyright 2021 Matrix Origin
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

package toolfs

import (
	"context"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const defaultFSName = "SHARED"

// StorageOptions describes a fileservice backend requested by a tool command.
type StorageOptions struct {
	FSConfig string
	FSName   string
	S3       string
	Backend  string
}

// IsRemote reports whether the command should build an explicit fileservice.
func (o StorageOptions) IsRemote() bool {
	return o.FSConfig != "" || o.S3 != "" || o.Backend != ""
}

// Open creates a FileService from tool options.
func Open(ctx context.Context, opts StorageOptions) (fileservice.FileService, string, error) {
	if opts.FSName == "" {
		opts.FSName = defaultFSName
	}

	if opts.FSConfig != "" {
		return openFromConfig(ctx, opts.FSConfig, opts.FSName)
	}

	backend := strings.ToUpper(opts.Backend)
	if backend == "" {
		backend = "S3"
	}
	if backend != "S3" && backend != "MINIO" {
		return nil, "", moerr.NewInvalidInputNoCtxf("unsupported backend %q, use S3 or MINIO", opts.Backend)
	}
	if opts.S3 == "" {
		return nil, "", moerr.NewInvalidInputNoCtx("missing --s3 arguments")
	}

	args := fileservice.ObjectStorageArguments{Name: opts.FSName}
	if err := args.SetFromString(splitArgs(opts.S3)); err != nil {
		return nil, "", err
	}
	cfg := fileservice.Config{
		Name:    opts.FSName,
		Backend: backend,
		S3:      args,
		Cache:   fileservice.DisabledCacheConfig,
	}
	fs, err := fileservice.NewFileService(ctx, cfg, nil)
	if err != nil {
		return nil, "", err
	}
	if backend == "S3" || backend == "MINIO" {
		fs, _, err = newLazyCacheFS(ctx, fs)
		if err != nil {
			return nil, "", err
		}
	}
	return fs, fmt.Sprintf("%s:%s", strings.ToLower(backend), args.KeyPrefix), nil
}

type launchConfig struct {
	DataDir     string               `toml:"data-dir"`
	FileService []fileservice.Config `toml:"fileservice"`
	TN          struct {
		Txn struct {
			Storage struct {
				FileService string `toml:"fileservice"`
			} `toml:"Storage"`
		} `toml:"Txn"`
	} `toml:"tn"`
}

func openFromConfig(ctx context.Context, path string, fsName string) (fileservice.FileService, string, error) {
	var cfg launchConfig
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, "", err
	}

	if cfg.TN.Txn.Storage.FileService != "" && fsName == defaultFSName {
		fsName = cfg.TN.Txn.Storage.FileService
	}
	for _, fsCfg := range cfg.FileService {
		if strings.EqualFold(fsCfg.Name, fsName) {
			fs, err := fileservice.NewFileService(ctx, fsCfg, nil)
			if err != nil {
				return nil, "", err
			}
			backend := strings.ToUpper(fsCfg.Backend)
			if backend == "S3" || backend == "MINIO" {
				fs, _, err = newLazyCacheFS(ctx, fs)
				if err != nil {
					return nil, "", err
				}
			}
			return fs, fmt.Sprintf("%s:%s", path, fsCfg.Name), nil
		}
	}

	if len(cfg.FileService) == 0 && cfg.DataDir != "" {
		fsCfg := fileservice.Config{
			Name:    fsName,
			Backend: "DISK",
			DataDir: cfg.DataDir,
			Cache:   fileservice.DisabledCacheConfig,
		}
		fs, err := fileservice.NewFileService(ctx, fsCfg, nil)
		if err != nil {
			return nil, "", err
		}
		return fs, fmt.Sprintf("%s:%s", path, cfg.DataDir), nil
	}

	var names []string
	for _, fsCfg := range cfg.FileService {
		names = append(names, fsCfg.Name)
	}
	return nil, "", moerr.NewInvalidInputNoCtxf(
		"fileservice %q not found in %s; available: %s",
		fsName, path, strings.Join(names, ","),
	)
}

func splitArgs(s string) []string {
	parts := strings.Split(s, ",")
	ret := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			ret = append(ret, part)
		}
	}
	return ret
}

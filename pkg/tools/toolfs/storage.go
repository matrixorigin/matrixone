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

var (
	newToolFileService = func(ctx context.Context, cfg fileservice.Config) (fileservice.FileService, error) {
		return fileservice.NewFileService(ctx, cfg, nil)
	}
	newToolLazyCacheFS = newLazyCacheFS
)

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
		return nil, "", moerr.NewInvalidInputNoCtx("missing --remote-s3 arguments")
	}

	args, err := ParseS3Arguments(opts.S3, opts.FSName)
	if err != nil {
		return nil, "", err
	}
	cfg := fileservice.Config{
		Name:    opts.FSName,
		Backend: backend,
		S3:      args,
		Cache:   fileservice.DisabledCacheConfig,
	}
	fs, err := newToolFileService(ctx, cfg)
	if err != nil {
		return nil, "", err
	}
	if backend == "S3" || backend == "MINIO" {
		remote := fs
		fs, _, err = newToolLazyCacheFS(ctx, remote)
		if err != nil {
			remote.Close(ctx)
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
	fsCfg, err := ResolveConfig(path, fsName)
	if err != nil {
		return nil, "", err
	}
	fs, err := newToolFileService(ctx, fsCfg)
	if err != nil {
		return nil, "", err
	}
	backend := strings.ToUpper(fsCfg.Backend)
	if backend == "S3" || backend == "MINIO" {
		remote := fs
		fs, _, err = newToolLazyCacheFS(ctx, remote)
		if err != nil {
			remote.Close(ctx)
			return nil, "", err
		}
	}
	displayTarget := fsCfg.Name
	if fsCfg.DataDir != "" {
		displayTarget += ":" + fsCfg.DataDir
	}
	return fs, fmt.Sprintf("%s:%s", path, displayTarget), nil
}

// ResolveConfig selects and returns a fileservice configuration without
// constructing the service. Tooling uses this to keep generated restore paths
// consistent with the backend that receives the dump files.
func ResolveConfig(path string, fsName string) (fileservice.Config, error) {
	var cfg launchConfig
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return fileservice.Config{}, err
	}
	if fsName == "" {
		fsName = defaultFSName
	}

	if cfg.TN.Txn.Storage.FileService != "" && fsName == defaultFSName {
		fsName = cfg.TN.Txn.Storage.FileService
	}
	for _, fsCfg := range cfg.FileService {
		if strings.EqualFold(fsCfg.Name, fsName) {
			return fsCfg, nil
		}
	}

	if len(cfg.FileService) == 0 && cfg.DataDir != "" {
		fsCfg := fileservice.Config{
			Name:    fsName,
			Backend: "DISK",
			DataDir: cfg.DataDir,
			Cache:   fileservice.DisabledCacheConfig,
		}
		return fsCfg, nil
	}

	names := make([]string, 0, len(cfg.FileService))
	for _, fsCfg := range cfg.FileService {
		names = append(names, fsCfg.Name)
	}
	return fileservice.Config{}, moerr.NewInvalidInputNoCtxf(
		"fileservice %q not found in %s; available: %s",
		fsName, path, strings.Join(names, ","),
	)
}

func ParseS3Arguments(s string, fsName string) (fileservice.ObjectStorageArguments, error) {
	args := fileservice.ObjectStorageArguments{Name: fsName}
	if err := args.SetFromString(splitArgs(s)); err != nil {
		return fileservice.ObjectStorageArguments{}, err
	}
	return args, nil
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

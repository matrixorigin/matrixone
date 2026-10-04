// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	legacyDumpTableDir        = "dumpTable"
	legacyDumpTableTimeLayout = "2006-01-02.15.04.05.MST"
	legacyDumpTableFileTTL    = 24 * time.Hour
)

func init() {
	// Keep the old application registered for upgrade compatibility. New code
	// cannot create dumpTable data, but upgraded TNs still need to reclaim files
	// left by the removed inspect command.
	fileservice.RegisterAppConfig(&fileservice.AppConfig{
		Name: legacyDumpTableDir,
		GCFn: gcLegacyDumpTableFiles,
	})
}

func decodeLegacyDumpTableDir(dir string) (time.Time, error) {
	parts := strings.Split(dir, "_")
	if len(parts) != 3 {
		return time.Time{}, moerr.NewInternalErrorNoCtxf(
			"invalid legacy dump table directory %q", dir)
	}
	if _, err := strconv.ParseUint(parts[0], 10, 64); err != nil {
		return time.Time{}, moerr.NewInternalErrorNoCtxf(
			"invalid legacy dump table id in %q: %v", dir, err)
	}
	createTime, err := time.Parse(legacyDumpTableTimeLayout, parts[1])
	if err != nil {
		return time.Time{}, moerr.NewInternalErrorNoCtxf(
			"invalid legacy dump table creation time in %q: %v", dir, err)
	}
	ts := strings.Split(parts[2], "-")
	if len(ts) != 2 {
		return time.Time{}, moerr.NewInternalErrorNoCtxf(
			"invalid legacy dump table snapshot in %q", dir)
	}
	if _, err := strconv.ParseInt(ts[0], 10, 64); err != nil {
		return time.Time{}, moerr.NewInternalErrorNoCtxf(
			"invalid legacy dump table snapshot physical time in %q: %v", dir, err)
	}
	if _, err := strconv.ParseUint(ts[1], 10, 32); err != nil {
		return time.Time{}, moerr.NewInternalErrorNoCtxf(
			"invalid legacy dump table snapshot logical time in %q: %v", dir, err)
	}
	return createTime, nil
}

func gcLegacyDumpTableFiles(filePath string, fs fileservice.FileService) (bool, error) {
	createTime, err := decodeLegacyDumpTableDir(filePath)
	if err != nil {
		return false, err
	}
	if !createTime.Add(legacyDumpTableFileTTL).Before(time.Now()) {
		return false, nil
	}

	ctx, cancel := context.WithTimeoutCause(
		context.Background(),
		5*time.Second,
		moerr.CauseClearPersistTable,
	)
	defer cancel()

	entries := fs.List(ctx, filePath)
	for entry, err := range entries {
		if err != nil {
			return false, err
		}
		if entry == nil {
			continue
		}
		if err := fs.Delete(ctx, path.Join(filePath, entry.Name)); err != nil {
			return false, err
		}
	}
	if err := fs.Delete(ctx, filePath); err != nil {
		return false, err
	}
	return true, nil
}

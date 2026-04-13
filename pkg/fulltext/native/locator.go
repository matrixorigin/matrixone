// Copyright 2025 Matrix Origin
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

package native

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const sidecarLocatorVersion = 1

type SidecarLocator struct {
	Version uint32                `json:"version"`
	Entries []SidecarLocatorEntry `json:"entries"`
}

type SidecarLocatorEntry struct {
	IndexTable string `json:"index_table"`
	FilePath   string `json:"file_path"`
}

func SidecarLocatorPath(objectPath string) string {
	return objectPath + ".fts.locator"
}

func WriteSidecarLocator(
	ctx context.Context,
	fs fileservice.FileService,
	objectPath string,
	entries []SidecarLocatorEntry,
) error {
	entries = normalizeLocatorEntries(entries)
	if len(entries) == 0 {
		return nil
	}
	data, err := json.Marshal(SidecarLocator{
		Version: sidecarLocatorVersion,
		Entries: entries,
	})
	if err != nil {
		return err
	}
	return fs.Write(ctx, fileservice.IOVector{
		FilePath: SidecarLocatorPath(objectPath),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   data,
		}},
	})
}

func ReadSidecarLocator(
	ctx context.Context,
	fs fileservice.FileService,
	objectPath string,
) (*SidecarLocator, bool, error) {
	vec := &fileservice.IOVector{
		FilePath: SidecarLocatorPath(objectPath),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	if err := fs.Read(ctx, vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var locator SidecarLocator
	if err := json.Unmarshal(vec.Entries[0].Data, &locator); err != nil {
		return nil, false, err
	}
	if locator.Version != sidecarLocatorVersion {
		return nil, false, moerr.NewInternalErrorNoCtx("unsupported native fulltext sidecar locator version")
	}
	locator.Entries = normalizeLocatorEntries(locator.Entries)
	return &locator, true, nil
}

func ExpandDeletePathsWithLocators(
	ctx context.Context,
	fs fileservice.FileService,
	objectPaths []string,
) []string {
	out := make([]string, 0, len(objectPaths))
	seen := make(map[string]struct{}, len(objectPaths))
	appendUnique := func(path string) {
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}

	for _, objectPath := range objectPaths {
		appendUnique(objectPath)

		locatorPath := SidecarLocatorPath(objectPath)
		exists, err := statPathExists(ctx, fs, locatorPath)
		if err != nil {
			logutil.Warn(
				"[NATIVE-FTS-GC-LOCATOR-STAT-FAILED]",
				zap.String("object", objectPath),
				zap.String("locator", locatorPath),
				zap.Error(err),
			)
			continue
		}
		if !exists {
			continue
		}
		appendUnique(locatorPath)

		locator, _, err := ReadSidecarLocator(ctx, fs, objectPath)
		if err != nil {
			logutil.Warn(
				"[NATIVE-FTS-GC-LOCATOR-READ-FAILED]",
				zap.String("object", objectPath),
				zap.String("locator", locatorPath),
				zap.Error(err),
			)
			continue
		}
		for _, entry := range locator.Entries {
			exists, err := statPathExists(ctx, fs, entry.FilePath)
			if err != nil {
				logutil.Warn(
					"[NATIVE-FTS-GC-SIDECAR-STAT-FAILED]",
					zap.String("object", objectPath),
					zap.String("sidecar", entry.FilePath),
					zap.Error(err),
				)
				continue
			}
			if exists {
				appendUnique(entry.FilePath)
			}
		}
	}
	return out
}

func normalizeLocatorEntries(entries []SidecarLocatorEntry) []SidecarLocatorEntry {
	if len(entries) == 0 {
		return nil
	}
	uniq := make(map[string]SidecarLocatorEntry, len(entries))
	for _, entry := range entries {
		if entry.FilePath == "" {
			continue
		}
		uniq[entry.FilePath] = entry
	}
	if len(uniq) == 0 {
		return nil
	}
	paths := make([]string, 0, len(uniq))
	for path := range uniq {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	out := make([]SidecarLocatorEntry, 0, len(paths))
	for _, path := range paths {
		out = append(out, uniq[path])
	}
	return out
}

func statPathExists(ctx context.Context, fs fileservice.FileService, filePath string) (bool, error) {
	if _, err := fs.StatFile(ctx, filePath); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

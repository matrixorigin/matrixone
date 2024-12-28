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

package ioutil

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func ListTSRangeFiles(
	ctx context.Context,
	dir string,
	fs fileservice.FileService,
) (files []TSRangeFile, err error) {
	var (
		entries []fileservice.DirEntry
	)
	if entries, err = fileservice.SortedList(
		fs.List(ctx, dir),
	); err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir {
			if file := DecodeTSRangeFile(entry.Name); file.IsValid() {
				files = append(files, file)
			}
		}
	}
	return
}

func ListTSRangeFilesInGCDir(
	ctx context.Context,
	fs fileservice.FileService,
) (files []TSRangeFile, err error) {
	var (
		entries []fileservice.DirEntry
	)
	if entries, err = fileservice.SortedList(
		fs.List(ctx, GetGCDir()),
	); err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir {
			if file := DecodeTSRangeFile(entry.Name); file.IsValid() {
				files = append(files, file)
			}
		}
	}
	return
}

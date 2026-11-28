// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"bytes"
	"context"
	"path"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/udf"
)

// Python source and admission slots live outside the checkpoint object list.
// Include retained versions and reservations even when the current checkpoint
// no longer has a live routine using them. Publishers claim a slot before
// writing source, so listing slots after copying source also preserves charges
// for publications that completed while the source namespace was being copied.
func (c backupFileCopier) copyPythonArtifactFiles(
	ctx context.Context, src, dst fileservice.FileService, ts types.TS,
) ([]*taeFile, error) {
	accounts, err := fileservice.SortedList(src.List(ctx, udf.PythonArtifactDirectory+"/"))
	if err != nil {
		return nil, err
	}
	directories := make([]string, 0, len(accounts)+1)
	for _, account := range accounts {
		id, err := strconv.ParseUint(account.Name, 10, 64)
		if account.IsDir && err == nil && strconv.FormatUint(id, 10) == account.Name {
			directories = append(directories, path.Join(udf.PythonArtifactDirectory, account.Name))
		}
	}
	directories = append(directories, udf.PythonArtifactAdmissionDirectory)

	var files []*taeFile
	for _, dir := range directories {
		validName := udf.IsSHA256Digest
		if dir == udf.PythonArtifactAdmissionDirectory {
			validName = isPythonArtifactAdmissionSlot
		}
		entries, err := fileservice.SortedList(src.List(ctx, dir+"/"))
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir || !validName(entry.Name) {
				continue
			}
			name := path.Join(dir, entry.Name)
			checksum, err := c.copyPythonArtifactFile(ctx, src, dst, name)
			if err != nil {
				return nil, err
			}
			files = append(files, &taeFile{
				path: name, size: entry.Size, checksum: checksum, needCopy: true, ts: ts,
			})
		}
	}
	return files, nil
}

func isPythonArtifactAdmissionSlot(name string) bool {
	_, err := strconv.ParseUint(name, 10, 16)
	return len(name) == 4 && err == nil
}

func (c backupFileCopier) copyPythonArtifactFile(
	ctx context.Context, src, dst fileservice.FileService, name string,
) ([]byte, error) {
	_, err := dst.StatFile(ctx, name)
	if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return c.copyFileWithRetry(ctx, src, dst, name, "")
	}
	if err != nil {
		return nil, err
	}

	// An earlier copy to the same destination can be reused only if its
	// immutable bytes agree, including slot payloads that enforce the quota.
	expected, err := checksumFile(ctx, src, name)
	if err != nil {
		return nil, err
	}
	actual, err := checksumFile(ctx, dst, name)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(expected, actual) {
		return nil, moerr.NewInternalErrorf(ctx, "backup Python artifact or admission checksum mismatch: %s", name)
	}
	return actual, nil
}

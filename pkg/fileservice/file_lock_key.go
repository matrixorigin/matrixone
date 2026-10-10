// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"os"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// FileLockKey identifies an ETL destination for cross-CN admission locks.
// Service names and credentials do not identify storage: Stage aliases must
// share a key. Object storage endpoints are deliberately omitted, since aliases
// can reach the same bucket. Equal bucket/key paths on different stores may
// conservatively contend; different paths remain independent.
func FileLockKey(fs FileService, filePath string) (string, error) {
	switch f := fs.(type) {
	case *subPathFS:
		p, err := f.toUpstreamFilePath(filePath)
		if err != nil {
			return "", err
		}
		return FileLockKey(f.upstream, p)
	case *LocalETLFS:
		p, err := parseFilePathAtService(filePath, f.name)
		if err != nil {
			return "", err
		}
		native := f.toNativeFilePath(p.File)
		// A fresh Stage subdirectory may not exist yet. Resolve its existing
		// ancestor so aliases through symlinks still use the same lock key.
		suffix := ""
		for {
			resolved, err := filepath.EvalSymlinks(native)
			if err == nil {
				return "file\x00" + filepath.Join(resolved, suffix), nil
			}
			if !os.IsNotExist(err) {
				return "", err
			}
			parent := filepath.Dir(native)
			if parent == native {
				return "", err
			}
			suffix = filepath.Join(filepath.Base(native), suffix)
			native = parent
		}
	case *S3FS:
		p, err := parseFilePathAtService(filePath, f.name)
		if err != nil {
			return "", err
		}
		return "object\x00" + f.bucket + "\x00" + f.pathToKey(p.File), nil
	default:
		return "", moerr.NewNotSupportedNoCtxf("file lock key for %T", fs)
	}
}

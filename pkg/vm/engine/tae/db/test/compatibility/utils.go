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

package compatibility

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	PrepareDir = "prepare"
)

func GetPrepareDirName() string {
	return filepath.Join("/tmp", "tae-compatibility-test", PrepareDir)
}

func GetPrepareVersionName() string {
	return filepath.Join(GetPrepareDirName(), "version")
}

func RemovePrepareDir() (string, error) {
	dir := GetPrepareDirName()
	return dir, os.RemoveAll(dir)
}

func InitPrepareEnv() error {
	dir, _ := RemovePrepareDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(
		GetPrepareVersionName(),
		os.O_RDWR|os.O_CREATE,
		os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	ver := int64(version)
	buf := types.EncodeInt64(&ver)
	_, err = file.Write(buf)
	return err
}

func ReadPrepareVersion() (int, error) {
	versionFileName := GetPrepareVersionName()
	if _, err := os.Stat(versionFileName); err != nil {
		return 0, err
	}
	file, err := os.OpenFile(
		versionFileName,
		os.O_RDONLY,
		os.ModePerm)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	buf := make([]byte, 8)
	if _, err = file.Read(buf); err != nil {
		return 0, err
	}
	ver := int(types.DecodeInt64(buf))
	return ver, nil
}

func EnsurePrepareEnvOK() error {
	ver, err := ReadPrepareVersion()
	if err != nil {
		return err
	}

	if ver > version {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("prepare env version is %d, but current version is %d", ver, version))
	}

	return nil
}

func GetPrepareDirByName(name string) string {
	return filepath.Join("/tmp", "tae-compatibility-test", PrepareDir, name)
}

func InitPrepareDirByName(name string) (string, error) {
	dir := GetPrepareDirByName(name)
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return dir, nil
}

func GetExecureDirName() string {
	return filepath.Join("/tmp", "tae-compatibility-test", "execute")
}

func RemoveExecuteDir() (string, error) {
	dir := GetExecureDirName()
	return dir, os.RemoveAll(dir)
}

func InitExecuteEnv() error {
	dir, _ := RemoveExecuteDir()
	return os.MkdirAll(dir, 0755)
}

func InitTestCaseExecuteDir(name string) (string, error) {
	dir := filepath.Join(GetExecureDirName(), name)
	err := os.RemoveAll(dir)
	return dir, err
}

func CopyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, _ error) error {
		if info.IsDir() {
			rel, err := filepath.Rel(src, path)
			if err != nil {
				return err
			}
			return os.Mkdir(filepath.Join(dst, rel), 0755)
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		return CopyFile(path, filepath.Join(dst, rel))
	})
}

// Copy file from src to dst
func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	dstFile, err := os.OpenFile(
		dst,
		os.O_RDWR|os.O_CREATE,
		os.ModePerm)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	_, err = dstFile.ReadFrom(srcFile)
	return err
}

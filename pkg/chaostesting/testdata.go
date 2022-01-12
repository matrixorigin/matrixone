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

package fz

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/reusee/e4"
)

type TestDataDir string

func (_ Def) TestDataDir() (dir TestDataDir) {
	dir = "testdata"

	// ensure testdata dir
	_, err := os.Stat(string(dir))
	if errors.Is(err, os.ErrNotExist) {
		err = nil
		ce(os.Mkdir(string(dir), 0755))
	}
	ce(err)

	return
}

type TestDataFilePath func(
	id uuid.UUID,
	category string,
	extension string,
) string

func (_ Def) TestDataFilePath(
	dir TestDataDir,
	id uuid.UUID,
) TestDataFilePath {
	outputDir := string(dir)
	return func(
		id uuid.UUID,
		category string,
		extension string,
	) string {
		return filepath.Join(
			outputDir,
			id.String()+"."+category+"."+extension,
		)
	}
}

type WriteTestDataFile func(
	category string,
	extension string,
) (
	file *os.File,
	err error,
	done func() error,
)

func (_ Def) WriteTestDataFile(
	dir TestDataDir,
	getFilePath TestDataFilePath,
	id uuid.UUID,
) WriteTestDataFile {

	outputDir := string(dir)

	return func(
		category string,
		extension string,
	) (
		file *os.File,
		err error,
		done func() error,
	) {
		defer he(&err)

		file, err = os.CreateTemp(outputDir, "*.tmp")
		ce(err)

		done = func() error {
			if err := file.Close(); err != nil {
				return err
			}

			name := getFilePath(id, category, extension)
			if err := os.Rename(file.Name(), name); err != nil {
				return err
			}

			return nil
		}

		return
	}
}

type ClearTestDataFile func(
	category string,
	extension string,
) (
	err error,
)

func (_ Def) ClearTestDataFile(
	getFilePath TestDataFilePath,
	id uuid.UUID,
) ClearTestDataFile {
	return func(
		category string,
		extension string,
	) (
		err error,
	) {
		return we.With(
			e4.Ignore(os.ErrNotExist),
		)(os.Remove(getFilePath(id, category, extension)))
	}
}

type GlobTestDataFiles func(
	category string,
	extension string,
) (
	paths []string,
	err error,
)

func (_ Def) GlobTestDataFiles(
	testDataDir TestDataDir,
) GlobTestDataFiles {
	dir := string(testDataDir)
	return func(
		category string,
		extension string,
	) (
		paths []string,
		err error,
	) {
		return filepath.Glob(
			filepath.Join(
				dir,
				"*."+category+"."+extension,
			),
		)
	}
}

type TestDataFilePathToUUID func(
	path string,
) (
	id uuid.UUID,
	err error,
)

func (_ Def) TestDataFilePathToUUID() TestDataFilePathToUUID {
	return func(
		path string,
	) (
		id uuid.UUID,
		err error,
	) {
		base := filepath.Base(path)
		idStr, _, ok := strings.Cut(base, ".")
		if !ok {
			err = fmt.Errorf("bad path: %s", path)
			return
		}
		id, err = uuid.Parse(idStr)
		return
	}
}

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

package main

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

type TestCase struct {
	ID         uuid.UUID
	ConfigPath string
	LastRunAt  time.Time
	ModifiedAt time.Time
}

type TestCases []*TestCase

type GetTestCases func(args []string) TestCases

func (_ Def) GetTestCases(
	testDataDir fz.TestDataDir,
	testDataFilePath fz.TestDataFilePath,
	globTestDataFiles fz.GlobTestDataFiles,
	pathToID fz.TestDataFilePathToUUID,
) GetTestCases {

	return func(args []string) (cases TestCases) {

		if len(args) == 0 {
			// run all
			configPaths, err := globTestDataFiles("config", "xml")
			ce(err)
			for _, configPath := range configPaths {
				id, err := pathToID(configPath)
				ce(err)
				cases = append(cases, &TestCase{
					ID:         id,
					ConfigPath: configPath,
				})
			}

		} else {
			// run some
			for _, path := range args {
				path := path
				_, err := os.Stat(path)
				if errors.Is(err, os.ErrNotExist) {
					err = nil
					path = filepath.Join(string(testDataDir), path+"-config.xml")
				}
				ce(err)
				id, err := pathToID(path)
				ce(err)
				cases = append(cases, &TestCase{
					ID:         id,
					ConfigPath: path,
				})
			}
		}

		// get last run time and modified time
		for _, c := range cases {
			if c.LastRunAt.IsZero() {
				stat, err := os.Stat(testDataFilePath(c.ID, "cube", "log"))
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				ce(err)
				c.LastRunAt = stat.ModTime()
			}
			if c.ModifiedAt.IsZero() {
				stat, err := os.Stat(testDataFilePath(c.ID, "config", "xml"))
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				ce(err)
				c.ModifiedAt = stat.ModTime()
			}
		}

		// sort by last run time
		sort.Slice(cases, func(i, j int) bool {
			c1 := cases[i]
			c2 := cases[j]
			if !c1.LastRunAt.Equal(c2.LastRunAt) {
				return cases[i].LastRunAt.Before(cases[j].LastRunAt)
			}
			return c1.ModifiedAt.After(c2.ModifiedAt)
		})

		return
	}

}

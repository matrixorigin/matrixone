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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/e4"
)

type TestCase struct {
	ConfigPath string
	LastRunAt  time.Time
	ModifiedAt time.Time
}

type TestCases []*TestCase

type GetTestCases func(args []string) TestCases

func (_ Def) GetTestCases(
	testDataDir fz.TestDataDir,
	testDataFilePath fz.TestDataFilePath,
) GetTestCases {

	return func(args []string) (cases TestCases) {

		if len(args) == 0 {
			// run all
			ce(filepath.WalkDir(string(testDataDir), func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				if !strings.HasSuffix(path, "-config.xml") {
					return nil
				}
				info, err := entry.Info()
				ce(err)
				cases = append(cases, &TestCase{
					ConfigPath: path,
					ModifiedAt: info.ModTime(),
				})
				return nil
			}), e4.Ignore(os.ErrNotExist))

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
				cases = append(cases, &TestCase{
					ConfigPath: path,
				})
			}
		}

		// get last run time and modified time
		for _, c := range cases {
			if c.LastRunAt.IsZero() {
				stat, err := os.Stat(testDataFilePath("cube", "log"))
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				ce(err)
				c.LastRunAt = stat.ModTime()
			}
			if c.ModifiedAt.IsZero() {
				stat, err := os.Stat(testDataFilePath("config", "xml"))
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

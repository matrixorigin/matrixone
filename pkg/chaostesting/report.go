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
	"sync"

	"github.com/google/uuid"
)

type Report struct {
	Kind string
	Desc string
}

type AddReport func(...Report)

type GetReports func(kind string) []Report

type GetAllReports func() []Report

func (_ Def) Report() (
	add AddReport,
	getAll GetAllReports,
	get GetReports,
) {

	var reports []Report
	var l sync.Mutex

	add = func(rs ...Report) {
		l.Lock()
		defer l.Unlock()
		reports = append(reports, rs...)
	}

	getAll = func() []Report {
		l.Lock()
		defer l.Unlock()
		return reports
	}

	get = func(kind string) (res []Report) {
		l.Lock()
		defer l.Unlock()
		for _, report := range reports {
			if report.Kind == kind {
				res = append(res, report)
			}
		}
		return
	}

	return
}

type processReports func() error

func (_ Def) ProcessReports(
	clearFile ClearTestDataFile,
	getReports GetAllReports,
	writeFile WriteTestDataFile,
	testDataDir TestDataDir,
	id uuid.UUID,
	testDataFilePath TestDataFilePath,
	logger Logger,
) processReports {
	return func() (err error) {
		defer he(&err)
		defer func() {
			logger.Info("ProcessReports done")
		}()

		// clear old files
		ce(clearFile("report", "log"))
		byKindDir := filepath.Join(
			string(testDataDir),
			"reports-by-kind",
		)
		paths, err := filepath.Glob(filepath.Join(
			byKindDir,
			"*",
			id.String()+"*",
		))
		ce(err)
		for _, path := range paths {
			ce(os.Remove(path))
		}

		reports := getReports()

		if len(reports) > 0 {

			// write report log
			file, err, done := writeFile("report", "log")
			ce(err)
			for _, report := range reports {
				pt("%s\n", report)
				_, err := file.WriteString(fmt.Sprintf("%s\n\n", report))
				ce(err)
			}
			ce(done())

			// by kind
			for _, report := range reports {
				dir := filepath.Join(
					byKindDir,
					report.Kind,
				)
				_, err := os.Stat(dir)
				if errors.Is(err, os.ErrNotExist) {
					err = nil
					ce(os.MkdirAll(dir, 0755))
				}
				ce(err)
				reportLogPath := testDataFilePath(id, "report", "log")
				ce(os.Symlink(
					filepath.Join(
						"..",
						"..",
						filepath.Base(reportLogPath),
					),
					filepath.Join(
						dir,
						filepath.Base(reportLogPath),
					),
				))
			}

		}

		return
	}
}

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
	"fmt"
	"reflect"
	"sync"
)

type AddReport func(value any)

type GetReport func(target any) bool

type GetReports func() []any

func (_ Def) Report() (
	add AddReport,
	getAll GetReports,
	get GetReport,
) {

	var reports []any
	var l sync.Mutex

	add = func(report any) {
		l.Lock()
		defer l.Unlock()
		reports = append(reports, report)
	}

	getAll = func() []any {
		l.Lock()
		defer l.Unlock()
		return reports
	}

	get = func(target any) (found bool) {
		t := reflect.TypeOf(target).Elem()
		l.Lock()
		defer l.Unlock()
		for _, report := range reports {
			if reflect.TypeOf(report) == t {
				reflect.ValueOf(target).Elem().Set(reflect.ValueOf(report))
				found = true
			}
		}
		return
	}

	return
}

func (_ Def) ReportFiles(
	clear ClearTestDataFile,
	get GetReports,
	write WriteTestDataFile,
) Operators {
	return Operators{
		{

			BeforeDo: func() {
				ce(clear("report", "log"))
			},

			BeforeReport: func() {
				reports := get()
				if len(reports) == 0 {
					return
				}
				file, err, done := write("report", "log")
				ce(err)
				for _, report := range reports {
					_, err := file.WriteString(fmt.Sprintf("%s\n\n", report))
					ce(err)
				}
				ce(done())
			},
		},
	}
}

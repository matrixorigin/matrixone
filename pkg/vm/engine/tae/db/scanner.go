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

package db

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
)

type mergeScanner struct {
	catalog *catalog.Catalog
	op      *merge.Scheduler
}

func (scanner *mergeScanner) OnStopped() {
	logutil.Infof("DBScanner Stopped")
}

func (scanner *mergeScanner) OnExec() {
	dbutils.PrintMemStats()
	err := scanner.op.PreExecute()
	if err != nil {
		panic(err)
	}
	if err = scanner.catalog.RecurLoop(scanner); err != nil {
		logutil.Errorf("DBScanner Execute: %v", err)
	}
	err = scanner.op.PostExecute()
	if err != nil {
		panic(err)
	}
}

func newMergeScanner(cata *catalog.Catalog, scheduler *merge.Scheduler) *mergeScanner {
	return &mergeScanner{
		catalog: cata,
		op:      scheduler,
	}
}

func (scanner *mergeScanner) OnPostObject(entry *catalog.ObjectEntry) (err error) {
	return nil
}

func (scanner *mergeScanner) OnPostTable(entry *catalog.TableEntry) (err error) {
	return scanner.op.OnPostTable(entry)
}

func (scanner *mergeScanner) OnPostDatabase(entry *catalog.DBEntry) (err error) {
	return nil
}

func (scanner *mergeScanner) OnObject(entry *catalog.ObjectEntry) (err error) {
	return scanner.op.OnObject(entry)
}

func (scanner *mergeScanner) OnTombstone(entry *catalog.ObjectEntry) (err error) {
	return scanner.op.OnTombstone(entry)
}

func (scanner *mergeScanner) OnTable(entry *catalog.TableEntry) (err error) {
	return scanner.op.OnTable(entry)
}

func (scanner *mergeScanner) OnDatabase(entry *catalog.DBEntry) (err error) {
	return scanner.op.OnDatabase(entry)
}

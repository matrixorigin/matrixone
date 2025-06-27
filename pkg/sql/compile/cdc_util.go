// Copyright 2023 Matrix Origin
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

package compile

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

/* CDC APIs */
type SinkerInfo struct {
	SinkerType int8
	DBName     string
	TableName  string
	IndexName  string
}

func CreateTask(c *Compile, pitr_id int, sinkerinfo SinkerInfo) (bool, error) {
	logutil.Infof("Create Index Task %v", sinkerinfo)
	return true, nil
}

func DeleteTask(c *Compile, sinkerinfo SinkerInfo) (bool, error) {
	logutil.Infof("Delete Index Task %v", sinkerinfo)
	return true, nil
}

func getIndexPitrName(dbname string, tablename string) string {
	return fmt.Sprintf("__mo_idxpitr_%s_%s", dbname, tablename)
}

func CreateIndexPitr(c *Compile, dbname string, tablename string) (int, error) {
	pitr_name := getIndexPitrName(dbname, tablename)
	pitr_id := 0
	logutil.Infof("Create Index Pitr %s", pitr_name)
	return pitr_id, nil
}

func DeleteIndexPitr(c *Compile, dbname string, tablename string) error {
	pitr_name := getIndexPitrName(dbname, tablename)
	// remove pitr
	logutil.Infof("Delete Index Pitr %s", pitr_name)

	return nil
}

func CreateIndexCdcTask(c *Compile, tableDef *plan.TableDef, dbname string, tablename string, indexname string) error {
	var err error
	// create table pitr if not exists and return pitr_id
	pitr_id, err := CreateIndexPitr(c, dbname, tablename)
	if err != nil {
		return err
	}

	// create index cdc task
	ok, err := CreateTask(c, pitr_id, SinkerInfo{SinkerType: 0, DBName: dbname, TableName: tablename, IndexName: indexname})
	if err != nil {
		return err
	}

	if !ok {
		// cdc task already exist
		return moerr.NewInternalError(c.proc.Ctx, fmt.Sprintf("index cdc task (%s, %s, %s) already exists", dbname, tablename, indexname))
	}
	return nil
}

func DropIndexCdcTask(c *Compile, tableDef *plan.TableDef, dbname string, tablename string, indexname string) error {
	var err error

	// delete index cdc task
	_, err = DeleteTask(c, SinkerInfo{SinkerType: 0, DBName: dbname, TableName: tablename, IndexName: indexname})
	if err != nil {
		return err
	}

	// remove pitr if no index uses the pitr
	nindex := 0
	for _, idx := range tableDef.Indexes {
		if idx.TableExist &&
			(catalog.IsHnswIndexAlgo(idx.IndexAlgo) ||
				catalog.IsIvfIndexAlgo(idx.IndexAlgo) ||
				catalog.IsFullTextIndexAlgo(idx.IndexAlgo)) {

			if idx.IndexName != indexname {
				nindex++
			}
		}

	}

	if nindex == 0 {
		// remove pitr
		err = DeleteIndexPitr(c, dbname, tablename)
		if err != nil {
			return err
		}
	}

	return nil
}

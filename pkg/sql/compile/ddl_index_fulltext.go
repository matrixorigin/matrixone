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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	selectAllWithProjections = "SELECT %s FROM `%s`.`%s`;"
)

func tokenizeForFullTextIndex(c *Compile, originalTableDef *plan.TableDef, indexDef *plan.IndexDef, qryDatabase string) (err error) {

	var columns []string

	// select pkcol, indexDef.Parts
	pkeyName := originalTableDef.Pkey.PkeyColName
	if pkeyName == catalog.CPrimaryKeyColName {
		// TODO: not supported yet and see genInsertTableSqlForMasterIndex to serial multiple keys together
		return moerr.NewNotSupportedNoCtx("composite primary key not supported")
	} else {
		columns = append(columns, pkeyName)
	}

	if len(indexDef.Parts) > 1 {
		concat := fmt.Sprintf("CONCAT(%s)", strings.Join(indexDef.Parts, ", ' ', "))
		columns = append(columns, concat)
	} else {
		columns = append(columns, indexDef.Parts[0])
	}

	projects := strings.Join(columns, ",")
	sql := fmt.Sprintf(selectAllWithProjections, projects, qryDatabase, originalTableDef.Name)

	res, err := c.runSqlWithResult(sql, NoAccountId)
	if err != nil {
		return err
	}

	defer res.Close()

	if res.Batches != nil {
		for _, bat := range res.Batches {

			for i := 0; i < int(bat.RowCount()); i++ {
				typ := bat.Vecs[0].GetType()
				switch typ.Oid {
				case types.T_varchar:
					idstr := bat.Vecs[0].UnsafeGetStringAt(i)
					logutil.Infof(idstr)
				default:
				}
				id := bat.Vecs[0].GetRawBytesAt(i)
				null := bat.Vecs[1].IsNull(uint64(i))
				if null {
					continue
				}
				str := bat.Vecs[1].GetStringAt(i)

				logutil.Infof("%v %s", id, str)
			}

		}
	}

	logutil.Infof("RESULT %v", res)
	return nil

}

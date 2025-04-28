// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var hnswsync_runsql = sqlexec.RunSql

func hnswsync(proc *process.Process, db string, tbl string, cdc *vectorindex.HnswCdc[float32]) error {

	b, err := json.Marshal(cdc)
	if err != nil {
		return err
	}
	os.Stderr.WriteString(string(b))

	sql := fmt.Sprintf("select index_table_name, algo_table_type, algo_params, column_name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s') and algo='hnsw';",
		tbl, db)

	res, err := hnswsync_runsql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	bat := res.Batches[0]

	idxtblvec := bat.Vecs[0]
	algotypevec := bat.Vecs[1]
	paramvec := bat.Vecs[2]
	colvec := bat.Vecs[3]

	for i := 0; i < bat.RowCount(); i++ {

		idxtbl := idxtblvec.UnsafeGetStringAt(i)
		algotyp := algotypevec.UnsafeGetStringAt(i)
		param := paramvec.UnsafeGetStringAt(i)
		cname := colvec.UnsafeGetStringAt(i)
		os.Stderr.WriteString(fmt.Sprintf("idxtbl %s, type %s, param %s, cname %s\n", idxtbl, algotyp, param, cname))
	}

	return nil
}

func hnswCdcUpdate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {

	if len(ivecs) != 3 {
		return moerr.NewInvalidInput(proc.Ctx, "number of arguments != 3")
	}

	dbVec := vector.GenerateFunctionStrParameter(ivecs[0])
	tblVec := vector.GenerateFunctionStrParameter(ivecs[1])
	cdcVec := vector.GenerateFunctionStrParameter(ivecs[2])

	for i := uint64(0); i < uint64(length); i++ {
		dbname, isnull := dbVec.GetStrValue(i)
		if isnull {
			return moerr.NewInvalidInput(proc.Ctx, "dbname is null")
		}

		tblname, isnull := tblVec.GetStrValue(i)
		if isnull {
			return moerr.NewInvalidInput(proc.Ctx, "table name is null")

		}
		cdcstr, isnull := cdcVec.GetStrValue(i)
		if isnull {
			return moerr.NewInvalidInput(proc.Ctx, "cdc is null")
		}

		var cdc vectorindex.HnswCdc[float32]
		err := json.Unmarshal([]byte(cdcstr), &cdc)
		if err != nil {
			return moerr.NewInvalidInput(proc.Ctx, "cdc is not json object")
		}
		// hnsw sync
		os.Stderr.WriteString(fmt.Sprintf("db=%s, table=%s, json=%s\n", dbname, tblname, cdcstr))
		hnswsync(proc, string(dbname), string(tblname), &cdc)
	}

	return nil
}

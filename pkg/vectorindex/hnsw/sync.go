// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"fmt"
	"os"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	catalogsql = "select index_table_name, algo_table_type, algo_params, column_name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s') and algo='hnsw';"
)

func CdcSync(proc *process.Process, db string, tbl string, cdc *vectorindex.VectorIndexCdc[float32]) error {

	sql := fmt.Sprintf(catalogsql, tbl, db)
	res, err := runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	os.Stderr.WriteString(sql)
	os.Stderr.WriteString(fmt.Sprintf("\nnumber of batch = %d\n", len(res.Batches)))

	if len(res.Batches) == 0 {
		return nil
	}

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

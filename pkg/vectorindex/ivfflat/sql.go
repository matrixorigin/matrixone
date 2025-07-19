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

package ivfflat

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func GetVersion(proc *process.Process, tblcfg vectorindex.IndexTableConfig) (int64, error) {
	sql := fmt.Sprintf("SELECT CAST(`%s` AS BIGINT) FROM `%s`.`%s` WHERE `%s` = 'version'",
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		tblcfg.DbName,
		tblcfg.MetadataTable,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key)

	res, err := runSql(proc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if len(res.Batches) == 0 {
		return 0, moerr.NewInternalError(proc.Ctx, "version not found")
	}

	version := int64(0)
	bat := res.Batches[0]
	if bat.RowCount() == 1 {
		version = vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0)
		//logutil.Infof("NROW = %d", nrow)
	}

	return version, nil
}

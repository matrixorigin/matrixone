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

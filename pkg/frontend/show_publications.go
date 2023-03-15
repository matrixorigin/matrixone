package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	getPublicationsInfoFormat = "select pub_name as Name,database_name as `Database` from mo_catalog.mo_pubs;"
)

var (
	showPublicationOutputColumns = [2]Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Database",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
	}
)

func doShowPublications(ctx context.Context, ses *Session, sa *tree.ShowPublications) error {
	var err error
	var rs = &MysqlResultSet{}
	var erArray []ExecResult
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}
	err = bh.Exec(ctx, getPublicationsInfoFormat)
	if err != nil {
		goto handleFailed
	}
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		goto handleFailed
	}
	if execResultArrayHasData(erArray) {
		rs = erArray[0].(*MysqlResultSet)
	} else {
		rs.AddColumn(showPublicationOutputColumns[0])
		rs.AddColumn(showPublicationOutputColumns[1])
	}
	ses.SetMysqlResultSet(rs)
	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}
	return nil
handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	getAllAccountInfoFormat = "select " +
		"account_id as `account_id`, " +
		"account_name as `account_name`, " +
		"created_time as `created`, " +
		"status as `status`, " +
		"suspended_time as `suspended_time`, " +
		"comments as `comment` " +
		"from mo_catalog.mo_account;"
)

func getSqlForAllAccountInfo() string {
	return getAllAccountInfoFormat
}

type AccountInfo struct {
	accountId     uint64
	accountName   string
	adminName     string
	createdTime   string
	status        string
	suspendedTime string
	databaseCount uint64
	tableCount    uint64
	rowCount      uint64
	size          string
	comment       string
}

type column struct {
	name   string
	typ    defines.MysqlType
	signed bool
}

var (
	columns = []column{
		{name: "account_name", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "admin_name", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "created", typ: defines.MYSQL_TYPE_TIMESTAMP},
		{name: "status", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "suspended_time", typ: defines.MYSQL_TYPE_TIMESTAMP},
		{name: "db_count", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "table_count", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "row_count", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "size", typ: defines.MYSQL_TYPE_DECIMAL},
		{name: "comment", typ: defines.MYSQL_TYPE_VARCHAR},
	}
)

func newShowAccountsResultSet(allAccountInfo []*AccountInfo) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	for _, col := range columns {
		col1 := &MysqlColumn{}
		col1.SetName(col.name)
		col1.SetColumnType(col.typ)
		col1.SetSigned(col.signed)
		mrs.AddColumn(col1)
	}

	for _, info := range allAccountInfo {
		row := []interface{}{
			info.accountName,
			info.adminName,
			info.createdTime,
			info.status,
			info.suspendedTime,
			info.databaseCount,
			info.tableCount,
			info.rowCount,
			info.size,
			info.comment,
		}
		mrs.AddRow(row)
	}

	return mrs
}

func doShowAccounts(ctx context.Context, ses *Session, sa *tree.ShowAccounts) error {
	var err error
	var sql string
	var erArray []ExecResult
	var allAccountInfo []*AccountInfo
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	account := ses.GetTenantInfo()

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//step1: current account is sys or non-sys ?
	//the result of the statement show accounts is different
	//under the sys or non-sys.

	//step2:
	if account.IsSysTenant() {
		//under sys account
		//step2.1: get all account info from mo_account;
		//step2.2: for all accounts, switch into an account,
		//get the admin_name, table size and table rows.
		sql = getSqlForAllAccountInfo()
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(ctx, bh)
		if err != nil {
			goto handleFailed
		}
		if execResultArrayHasData(erArray) {
			for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
				accInfo := new(AccountInfo)
				//account_id
				accInfo.accountId, err = erArray[0].GetUint64(ctx, i, 0)
				if err != nil {
					goto handleFailed
				}

				//account_name
				accInfo.accountName, err = erArray[0].GetString(ctx, i, 1)
				if err != nil {
					goto handleFailed
				}

				//created_time
				accInfo.createdTime, err = erArray[0].GetString(ctx, i, 2)
				if err != nil {
					goto handleFailed
				}

				//status
				accInfo.status, err = erArray[0].GetString(ctx, i, 3)
				if err != nil {
					goto handleFailed
				}

				//suspended_time
				accInfo.suspendedTime, err = erArray[0].GetString(ctx, i, 4)
				if err != nil {
					goto handleFailed
				}

				//comment
				accInfo.comment, err = erArray[0].GetString(ctx, i, 5)
				if err != nil {
					goto handleFailed
				}

				allAccountInfo = append(allAccountInfo, accInfo)
			}
		}

		ses.SetMysqlResultSet(newShowAccountsResultSet(allAccountInfo))
	} else {
		//under non-sys account
		//step2.1: switch into the sys account, get the account info
		//step2.2: get the admin_name, table size and table rows.
		ses.SetMysqlResultSet(newShowAccountsResultSet(allAccountInfo))
	}

	//step3: make a response packet.
	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

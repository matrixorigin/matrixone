package frontend

import (
    "context"
    "fmt"
    "github.com/matrixorigin/matrixone/pkg/backup"
    "github.com/matrixorigin/matrixone/pkg/common/moerr"
    "github.com/matrixorigin/matrixone/pkg/defines"
    "github.com/matrixorigin/matrixone/pkg/fileservice"
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (mce *MysqlCmdExecutor) handleStartBackup(ctx context.Context, sb *tree.BackupStart) error {
    return doBackup(ctx, mce.GetSession(), sb)
}

func doBackup(ctx context.Context, ses *Session, bs *tree.BackupStart) error {
    var (
        err error
    )
    fmt.Println("+++>doBackup start", tree.String(bs, dialect.MYSQL))
    defer func() {
        fmt.Println("+++>doBackup end", tree.String(bs, dialect.MYSQL))
    }()
    defer func() {
        if err2 := recover(); err2 != nil {
            err = moerr.NewInternalError(ctx, "panic happens during backup. %v", err2)
        }
    }()
    //ts, err := strconv.ParseInt(sb.Timestamp, 10, 64)
    //if err != nil {
    //    return err
    //}
    //
    //dnTs := types.BuildTS(ts, 0)
    conf := &backup.Config{
        //Timestamp: dnTs,
        HAkeeper: ses.GetParameterUnit().HAKeeperClient,
    }
    conf.SharedFs, err = fileservice.Get[fileservice.FileService](ses.GetParameterUnit().FileService, defines.SharedFileServiceName)
    if err != nil {
        return err
    }
    return backup.Backup(ctx, bs, conf)
}

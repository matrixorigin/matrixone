package frontend

import (
    "context"
    "github.com/matrixorigin/matrixone/pkg/backup"
    "github.com/matrixorigin/matrixone/pkg/common/moerr"
    "github.com/matrixorigin/matrixone/pkg/defines"
    "github.com/matrixorigin/matrixone/pkg/fileservice"
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (mce *MysqlCmdExecutor) handleStartBackup(ctx context.Context, sb *tree.BackupStart) error {
    return doBackup(ctx, mce.GetSession(), sb)
}

func doBackup(ctx context.Context, ses *Session, bs *tree.BackupStart) error {
    var (
        err error
    )
    defer func() {
        if err2 := recover(); err2 != nil {
            err = moerr.NewInternalError(ctx, "panic happens during backup. %v", err2)
        }
    }()
    conf := &backup.Config{
        HAkeeper: ses.GetParameterUnit().HAKeeperClient,
    }
    conf.SharedFs, err = fileservice.Get[fileservice.FileService](ses.GetParameterUnit().FileService, defines.SharedFileServiceName)
    if err != nil {
        return err
    }
    return backup.Backup(ctx, bs, conf)
}

package frontend

import (
    "context"
    "fmt"
    "github.com/matrixorigin/matrixone/pkg/backup"
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
    "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (mce *MysqlCmdExecutor) handleStartBackup(ctx context.Context, sb *tree.BackupStart) error {
    return doBackup(ctx, mce.GetSession(), sb)
}

func doBackup(ctx context.Context, ses *Session, bs *tree.BackupStart) error {
    fmt.Println("+++>doBackup start", tree.String(bs, dialect.MYSQL))
    defer func() {
        fmt.Println("+++>doBackup end", tree.String(bs, dialect.MYSQL))
    }()
    //ts, err := strconv.ParseInt(sb.Timestamp, 10, 64)
    //if err != nil {
    //    return err
    //}
    //
    //dnTs := types.BuildTS(ts, 0)
    conf := &backup.Config{
        //Timestamp: dnTs,
    }
    return backup.Backup(ctx, bs, conf)
}

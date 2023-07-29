package tree

type BackupStart struct {
    statementImpl
    Timestamp string
    IsS3      bool
    Dir       string
    //s3 option
    Option []string
}

func (node *BackupStart) Format(ctx *FmtCtx) {
    ctx.WriteString("backup ")
    ctx.WriteString(node.Timestamp)
    ctx.WriteString(" ")
    if node.IsS3 {
        ctx.WriteString("s3option ")
        formatS3option(ctx, node.Option)
    } else {
        ctx.WriteString("filesystem ")
        ctx.WriteString(node.Dir)
    }
}

func (node *BackupStart) GetStatementType() string { return "Backup Start" }
func (node *BackupStart) GetQueryType() string     { return QueryTypeOth }
